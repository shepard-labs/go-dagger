package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct {
	TargetURL string `json:"target_url,omitempty"`
}

func main() {
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../../.env")
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		log.Fatal("POSTGRES_DSN is required")
	}
	d := &dag.DAG[RunState]{
		Name:      "minimal",
		Tasks:     map[string]*task.Task[RunState]{},
		TaskOrder: []string{"seed", "publish"},
	}
	d.Tasks["seed"] = &task.Task[RunState]{
		Name:         "seed",
		FunctionName: "examples.seed",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			return state, nil
		},
	}
	d.Tasks["publish"] = &task.Task[RunState]{
		Name:         "publish",
		DependsOn:    []string{"seed"},
		FunctionName: "examples.publish",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			fmt.Println("publishing", state.TargetURL)
			return state, nil
		},
	}
	if err := d.Validate(); err != nil {
		log.Fatal(err)
	}
	orch, err := orchestrator.NewOrchestrator[RunState](context.Background(), orchestrator.Config{PostgresDSN: dsn})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = orch.Close() }()
	run, err := orch.Run(context.Background(), d, orchestrator.GlobalInputs[RunState]{
		Value: RunState{TargetURL: "https://example.com"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("run", run.ID, "finished")
}
