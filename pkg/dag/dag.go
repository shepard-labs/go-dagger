// Package dag defines immutable DAG topology, validation, and YAML support.
//
// Task implementations fetch large external dependencies inside Execute and
// append durable results to the run state. DAG edges are control dependencies only,
// declared explicitly through Task.DependsOn.
package dag

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shepard-labs/go-dagger/internal/apperrors"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type DAG[S any] struct {
	Name             string
	Version          string
	ConcurrencyLimit int
	Timeout          time.Duration
	Tasks            map[string]*task.Task[S]
	TaskOrder        []string
	Adjacency        map[string][]string
	InDegree         map[string]int
}

func (d *DAG[S]) Validate() error {
	if d == nil {
		return fmt.Errorf("%w: dag is nil", apperrors.ErrValidation)
	}
	if strings.TrimSpace(d.Name) == "" {
		return fmt.Errorf("%w: dag name is required", apperrors.ErrValidation)
	}
	if d.Timeout < 0 {
		return fmt.Errorf("%w: dag timeout must be non-negative", apperrors.ErrValidation)
	}
	if d.ConcurrencyLimit < 0 {
		return fmt.Errorf("%w: concurrency_limit must be >= 1 when set", apperrors.ErrValidation)
	}
	if len(d.Tasks) == 0 {
		return fmt.Errorf("%w: dag must contain at least one task", apperrors.ErrValidation)
	}
	var zero S
	if _, err := json.Marshal(zero); err != nil {
		return fmt.Errorf("%w: run state is not JSON serializable: %v", apperrors.ErrValidation, err)
	}

	order, err := d.normalizedOrder()
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(order))
	for _, name := range order {
		if _, exists := seen[name]; exists {
			return fmt.Errorf("%w: duplicate task name %q", apperrors.ErrValidation, name)
		}
		seen[name] = struct{}{}
		t := d.Tasks[name]
		if t == nil {
			return fmt.Errorf("%w: task %q is nil", apperrors.ErrValidation, name)
		}
		t.Normalize()
		if strings.TrimSpace(t.Name) == "" {
			return fmt.Errorf("%w: task name is required", apperrors.ErrValidation)
		}
		if t.Name != name {
			return fmt.Errorf("%w: task map key %q does not match task name %q", apperrors.ErrValidation, name, t.Name)
		}
		if strings.Contains(t.Name, ".") {
			return fmt.Errorf("%w: task %q name must not contain '.'", apperrors.ErrValidation, t.Name)
		}
		if t.Mode == "" {
			t.Mode = task.ExecutionModeParallel
		}
		if t.Mode != task.ExecutionModeParallel && t.Mode != task.ExecutionModeSequential {
			return fmt.Errorf("%w: task %q has invalid execution_mode %q", apperrors.ErrValidation, t.Name, t.Mode)
		}
		if err := t.Retry.Validate(); err != nil {
			return fmt.Errorf("task %q: %w", t.Name, err)
		}
		t.Retry.Normalize()
		if t.Timeout < 0 {
			return fmt.Errorf("%w: task %q timeout must be non-negative", apperrors.ErrValidation, t.Name)
		}
		if t.Execute == nil {
			return fmt.Errorf("%w: task %q missing Execute", apperrors.ErrValidation, t.Name)
		}
	}

	adjacency, indegree, err := d.buildGraph(order)
	if err != nil {
		return err
	}
	d.Adjacency = adjacency
	d.InDegree = indegree
	d.TaskOrder = order
	if _, err := d.TopologicalSort(); err != nil {
		return err
	}
	return nil
}

func (d *DAG[S]) TopologicalSort() ([]string, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: dag is nil", apperrors.ErrValidation)
	}
	order, err := d.normalizedOrder()
	if err != nil {
		return nil, err
	}
	adjacency := d.Adjacency
	indegree := d.InDegree
	if adjacency == nil || indegree == nil {
		adjacency, indegree, err = d.buildGraph(order)
		if err != nil {
			return nil, err
		}
	}
	runtimeInDegree := make(map[string]int, len(indegree))
	for _, name := range order {
		runtimeInDegree[name] = indegree[name]
	}
	ready := make([]string, 0, len(order))
	for _, name := range sortedForScheduling(order, d.Tasks) {
		if runtimeInDegree[name] == 0 {
			ready = append(ready, name)
		}
	}
	result := make([]string, 0, len(order))
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		result = append(result, current)
		dependents := append([]string(nil), adjacency[current]...)
		sortByScheduling(dependents, orderIndex(order), d.Tasks)
		for _, dependent := range dependents {
			runtimeInDegree[dependent]--
			if runtimeInDegree[dependent] == 0 {
				ready = append(ready, dependent)
				sortByScheduling(ready, orderIndex(order), d.Tasks)
			}
		}
	}
	if len(result) != len(order) {
		cycle := make([]string, 0)
		for _, name := range order {
			if runtimeInDegree[name] > 0 {
				cycle = append(cycle, name)
			}
		}
		return nil, fmt.Errorf("%w: cycle detected involving tasks %s", apperrors.ErrValidation, strings.Join(cycle, ", "))
	}
	return result, nil
}

func (d *DAG[S]) normalizedOrder() ([]string, error) {
	if len(d.TaskOrder) > 0 {
		order := append([]string(nil), d.TaskOrder...)
		for _, name := range order {
			if _, ok := d.Tasks[name]; !ok {
				return nil, fmt.Errorf("%w: task_order references unknown task %q", apperrors.ErrValidation, name)
			}
		}
		if len(order) != len(d.Tasks) {
			missing := make([]string, 0)
			inOrder := make(map[string]struct{}, len(order))
			for _, name := range order {
				inOrder[name] = struct{}{}
			}
			for name := range d.Tasks {
				if _, ok := inOrder[name]; !ok {
					missing = append(missing, name)
				}
			}
			sort.Strings(missing)
			order = append(order, missing...)
		}
		return order, nil
	}
	order := make([]string, 0, len(d.Tasks))
	for name := range d.Tasks {
		order = append(order, name)
	}
	sort.Strings(order)
	return order, nil
}

func (d *DAG[S]) buildGraph(order []string) (map[string][]string, map[string]int, error) {
	adjacency := make(map[string][]string, len(order))
	indegree := make(map[string]int, len(order))
	for _, name := range order {
		adjacency[name] = []string{}
		indegree[name] = 0
	}
	for _, name := range order {
		t := d.Tasks[name]
		depSeen := map[string]struct{}{}
		for _, dep := range t.DependsOn {
			if dep == t.Name {
				return nil, nil, fmt.Errorf("%w: task %q depends on itself", apperrors.ErrValidation, t.Name)
			}
			if _, ok := d.Tasks[dep]; !ok {
				return nil, nil, fmt.Errorf("%w: task %q depends on unknown task %q", apperrors.ErrValidation, t.Name, dep)
			}
			if _, ok := depSeen[dep]; ok {
				return nil, nil, fmt.Errorf("%w: task %q has duplicate dependency %q", apperrors.ErrValidation, t.Name, dep)
			}
			depSeen[dep] = struct{}{}
			adjacency[dep] = append(adjacency[dep], t.Name)
			indegree[t.Name]++
		}
	}
	idx := orderIndex(order)
	for name := range adjacency {
		sortByScheduling(adjacency[name], idx, d.Tasks)
	}
	return adjacency, indegree, nil
}

func orderIndex(order []string) map[string]int {
	idx := make(map[string]int, len(order))
	for i, name := range order {
		idx[name] = i
	}
	return idx
}

func sortedForScheduling[S any](order []string, tasks map[string]*task.Task[S]) []string {
	names := append([]string(nil), order...)
	sortByScheduling(names, orderIndex(order), tasks)
	return names
}

func sortByScheduling[S any](names []string, idx map[string]int, tasks map[string]*task.Task[S]) {
	sort.SliceStable(names, func(i, j int) bool {
		left, right := names[i], names[j]
		lp, rp := 0, 0
		if tasks[left] != nil {
			lp = tasks[left].Priority
		}
		if tasks[right] != nil {
			rp = tasks[right].Priority
		}
		if lp != rp {
			return lp < rp
		}
		li, lok := idx[left]
		ri, rok := idx[right]
		if lok && rok && li != ri {
			return li < ri
		}
		if lok != rok {
			return lok
		}
		return left < right
	})
}
