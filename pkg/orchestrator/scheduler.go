package orchestrator

import (
	"sort"

	"github.com/shepard-labs/go-dagger/pkg/task"
)

type readyQueue[S any] struct {
	names []string
	idx   map[string]int
	tasks map[string]*task.Task[S]
}

func newReadyQueue[S any](order []string, tasks map[string]*task.Task[S]) *readyQueue[S] {
	idx := make(map[string]int, len(order))
	for i, name := range order {
		idx[name] = i
	}
	return &readyQueue[S]{idx: idx, tasks: tasks}
}

func (q *readyQueue[S]) push(name string) {
	q.names = append(q.names, name)
	q.sort()
}

func (q *readyQueue[S]) popRunnable(sequentialRunning bool) (string, bool) {
	for i, name := range q.names {
		t := q.tasks[name]
		if t != nil && t.Mode == task.ExecutionModeSequential && sequentialRunning {
			continue
		}
		q.names = append(q.names[:i], q.names[i+1:]...)
		return name, true
	}
	return "", false
}

func (q *readyQueue[S]) sort() {
	sort.SliceStable(q.names, func(i, j int) bool {
		left, right := q.names[i], q.names[j]
		lp, rp := 0, 0
		if q.tasks[left] != nil {
			lp = q.tasks[left].Priority
		}
		if q.tasks[right] != nil {
			rp = q.tasks[right].Priority
		}
		if lp != rp {
			return lp < rp
		}
		li, lok := q.idx[left]
		ri, rok := q.idx[right]
		if lok && rok && li != ri {
			return li < ri
		}
		if lok != rok {
			return lok
		}
		return left < right
	})
}

func effectiveConcurrency(dagLimit, configLimit int) int {
	if dagLimit > 0 {
		return dagLimit
	}
	if configLimit > 0 {
		return configLimit
	}
	return runtimeNumCPU()
}
