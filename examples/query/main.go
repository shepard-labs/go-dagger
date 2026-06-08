package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
)

type RunState struct{}

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
	orch, err := orchestrator.NewOrchestrator[RunState](context.Background(), orchestrator.Config{PostgresDSN: dsn})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = orch.Close() }()
	run, err := orch.GetDAGRun(context.Background(), runID)
	if err != nil {
		log.Fatal(err)
	}
	tasks, err := orch.ListTaskRuns(context.Background(), runID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(run.ID, run.Status, len(tasks))
}
