// Package task defines the pure domain model for executable DAG tasks.
//
// Agents fetch large external inputs inside Task.Execute. Durable data read by
// an agent must be appended to the returned state so later orchestration
// phases can snapshot it after successful task completion.
package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ExecuteFunc is the task body invoked by the orchestrator.
type ExecuteFunc[S any] func(context.Context, *S) (*S, error)

// FunctionRegistry maps YAML function names to executable task functions.
type FunctionRegistry[S any] map[string]ExecuteFunc[S]

// BeforeHook runs before a task attempt executes.
type BeforeHook[S any] func(context.Context, *S) error

// AfterHook runs after a task attempt finishes and receives the attempt error.
type AfterHook[S any] func(context.Context, *S, error) error

// HookRegistry maps YAML hook names to before and after hook implementations.
type HookRegistry[S any] map[string]Hook[S]

// Hook groups optional before and after task lifecycle callbacks.
type Hook[S any] struct {
	Before BeforeHook[S]
	After  AfterHook[S]
}

// Tool is a JSON-message tool that task or LLM code can dispatch by name.
type Tool struct {
	Name        string
	Description string
	Handler     func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)
}

// ToolRegistry maps tool names to their executable handlers.
type ToolRegistry map[string]Tool

// Dispatch invokes a registered tool by name.
func (r ToolRegistry) Dispatch(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
	tool, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("tool %q not found in registry", name)
	}
	if tool.Handler == nil {
		return nil, fmt.Errorf("tool %q has nil handler", name)
	}
	return tool.Handler(ctx, input)
}

// ExecutionMode controls whether a task can run alongside other ready tasks.
type ExecutionMode string

const (
	// ExecutionModeParallel allows the task to run within the global concurrency limit.
	ExecutionModeParallel ExecutionMode = "parallel"
	// ExecutionModeSequential reserves the sequential lane while the task is running.
	ExecutionModeSequential ExecutionMode = "sequential"
)

// Task is the executable unit in a DAG.
type Task[S any] struct {
	Name            string
	Description     string
	Tags            map[string]string
	Priority        int
	DependsOn       []string
	Mode            ExecutionMode
	Retry           RetryConfig
	Timeout         time.Duration
	FunctionName    string
	BeforeHookNames []string
	AfterHookNames  []string
	ToolNames       []string
	Tools           ToolRegistry
	BeforeHooks     []BeforeHook[S]
	AfterHooks      []AfterHook[S]
	Execute         ExecuteFunc[S]
}

// Normalize fills nil slices and maps with empty values for deterministic behavior.
func (t *Task[S]) Normalize() {
	if t.Tags == nil {
		t.Tags = map[string]string{}
	}
	if t.DependsOn == nil {
		t.DependsOn = []string{}
	}
	if t.BeforeHookNames == nil {
		t.BeforeHookNames = []string{}
	}
	if t.AfterHookNames == nil {
		t.AfterHookNames = []string{}
	}
	if t.ToolNames == nil {
		t.ToolNames = []string{}
	}
	if t.Tools == nil {
		t.Tools = ToolRegistry{}
	}
	if t.BeforeHooks == nil {
		t.BeforeHooks = []BeforeHook[S]{}
	}
	if t.AfterHooks == nil {
		t.AfterHooks = []AfterHook[S]{}
	}
}

// NormalizeTasks normalizes every non-nil task in the slice.
func NormalizeTasks[S any](tasks []*Task[S]) {
	for _, task := range tasks {
		if task != nil {
			task.Normalize()
		}
	}
}
