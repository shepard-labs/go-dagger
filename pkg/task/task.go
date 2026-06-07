// Package task defines the pure domain model for executable DAG tasks.
//
// Agents fetch large external inputs inside Task.Execute. Durable data read by
// an agent must be appended to the returned state so later orchestration
// phases can snapshot it after successful task completion.
package task

import (
	"context"
	"encoding/json"
	"time"
)

type ExecuteFunc[S any] func(context.Context, *S) (*S, error)
type FunctionRegistry[S any] map[string]ExecuteFunc[S]

type BeforeHook[S any] func(context.Context, *S) error
type AfterHook[S any] func(context.Context, *S, error) error
type HookRegistry[S any] map[string]Hook[S]

type Hook[S any] struct {
	Before BeforeHook[S]
	After  AfterHook[S]
}

type Tool struct {
	Name        string
	Description string
	Handler     func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)
}

type ToolRegistry map[string]Tool

type ExecutionMode string

const (
	ExecutionModeParallel   ExecutionMode = "parallel"
	ExecutionModeSequential ExecutionMode = "sequential"
)

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

func NormalizeTasks[S any](tasks []*Task[S]) {
	for _, task := range tasks {
		if task != nil {
			task.Normalize()
		}
	}
}
