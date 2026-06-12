package main

import (
	"context"
	"fmt"
	"log"

	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct {
	Trace []string `json:"trace,omitempty"`
}

func main() {
	d := &dag.DAG[RunState]{
		Name:      "cycle-example",
		TaskOrder: []string{"A", "B", "C", "D"},
		Tasks:     map[string]*task.Task[RunState]{},
	}

	d.Tasks["A"] = taskWithDeps("A", "B")
	d.Tasks["B"] = taskWithDeps("B", "C")
	d.Tasks["C"] = taskWithDeps("C", "D")
	d.Tasks["D"] = taskWithDeps("D", "A") // closes the cycle: A → B → C → D → A

	fmt.Println("validating DAG with cycle: A → B → C → D → A")
	if err := d.Validate(); err != nil {
		log.Fatalf("validation failed (expected): %v", err)
	}
	fmt.Println("validation passed (unexpected)")
}

func taskWithDeps(name string, deps ...string) *task.Task[RunState] {
	return &task.Task[RunState]{
		Name:         name,
		DependsOn:    deps,
		FunctionName: "examples.cycle." + name,
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.Trace = append(state.Trace, name)
			return state, nil
		},
	}
}
