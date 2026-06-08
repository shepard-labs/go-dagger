package main

import (
	"context"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct {
	FeedbackSummary string `json:"feedback_summary,omitempty"`
}

func main() {
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../../.env")
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		log.Fatal("POSTGRES_DSN is required")
	}
	runIDText := os.Getenv("DAG_RUN_ID")
	if runIDText == "" {
		log.Fatal("DAG_RUN_ID is required")
	}
	runID, err := uuid.Parse(runIDText)
	if err != nil {
		log.Fatal(err)
	}
	d := &dag.DAG[RunState]{Name: "resume-example", Tasks: map[string]*task.Task[RunState]{}, TaskOrder: []string{"step"}}
	d.Tasks["step"] = &task.Task[RunState]{
		Name:         "step",
		FunctionName: "examples.step",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.FeedbackSummary = "resumed"
			return state, nil
		},
	}
	orch, err := orchestrator.NewOrchestrator[RunState](context.Background(), orchestrator.Config{PostgresDSN: dsn})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = orch.Close() }()
	if _, err := orch.Resume(context.Background(), d, runID); err != nil {
		log.Fatal(err)
	}
}
