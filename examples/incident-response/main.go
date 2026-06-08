package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct {
	IncidentID      string   `json:"incident_id,omitempty"`
	AlertName       string   `json:"alert_name,omitempty"`
	Service         string   `json:"service,omitempty"`
	Severity        string   `json:"severity,omitempty"`
	Symptoms        []string `json:"symptoms,omitempty"`
	SuspectedCause  string   `json:"suspected_cause,omitempty"`
	ImpactedRegions []string `json:"impacted_regions,omitempty"`
	Runbook         string   `json:"runbook,omitempty"`
	MitigationSteps []string `json:"mitigation_steps,omitempty"`
	RollbackVersion string   `json:"rollback_version,omitempty"`
	StatusPageCopy  string   `json:"status_page_copy,omitempty"`
	ResolutionNote  string   `json:"resolution_note,omitempty"`
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	d := &dag.DAG[RunState]{
		Name:             "incident-response-example",
		ConcurrencyLimit: 3,
		TaskOrder:        []string{"ingest-alert", "triage", "select-runbook", "mitigate", "notify-status-page", "resolve"},
		Tasks:            map[string]*task.Task[RunState]{},
	}

	d.Tasks["ingest-alert"] = &task.Task[RunState]{
		Name:         "ingest-alert",
		FunctionName: "examples.incident.ingest_alert",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			fmt.Println("ingested alert", state.AlertName)
			return state, nil
		},
	}

	d.Tasks["triage"] = &task.Task[RunState]{
		Name:         "triage",
		DependsOn:    []string{"ingest-alert"},
		FunctionName: "examples.incident.triage",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.Severity = "SEV-2"
			state.SuspectedCause = "new payment gateway client release"
			state.ImpactedRegions = []string{"us-west-2"}
			fmt.Println("triaged", state.IncidentID, state.Severity, "cause:", state.SuspectedCause)
			return state, nil
		},
	}

	d.Tasks["select-runbook"] = &task.Task[RunState]{
		Name:         "select-runbook",
		DependsOn:    []string{"triage"},
		FunctionName: "examples.incident.select_runbook",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.Runbook = "rollback-payment-client"
			state.MitigationSteps = []string{
				"disable payment client v2026.06.06 feature flag",
				"rollback checkout-api to v2026.06.05",
				"drain unhealthy us-west pods",
			}
			fmt.Println("selected runbook", state.Runbook)
			return state, nil
		},
	}

	d.Tasks["mitigate"] = &task.Task[RunState]{
		Name:         "mitigate",
		DependsOn:    []string{"select-runbook"},
		FunctionName: "examples.incident.mitigate",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			if len(state.MitigationSteps) == 0 {
				return state, fmt.Errorf("no mitigation steps selected for %s", state.IncidentID)
			}
			state.RollbackVersion = "checkout-api:v2026.06.05"
			fmt.Println("applied mitigation", state.RollbackVersion, "steps:", strings.Join(state.MitigationSteps, " | "))
			return state, nil
		},
	}

	d.Tasks["notify-status-page"] = &task.Task[RunState]{
		Name:         "notify-status-page",
		DependsOn:    []string{"triage"},
		FunctionName: "examples.incident.notify_status_page",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.StatusPageCopy = fmt.Sprintf("We are investigating elevated %s errors in %s. Impact is currently limited to %s.", state.Service, state.Severity, strings.Join(state.ImpactedRegions, ", "))
			fmt.Println("posted status update:", state.StatusPageCopy)
			return state, nil
		},
	}

	d.Tasks["resolve"] = &task.Task[RunState]{
		Name:         "resolve",
		DependsOn:    []string{"mitigate", "notify-status-page"},
		FunctionName: "examples.incident.resolve",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.ResolutionNote = fmt.Sprintf("%s resolved after %s rollback; customer comms: %q", state.IncidentID, state.RollbackVersion, state.StatusPageCopy)
			fmt.Println("resolved incident", state.ResolutionNote)
			return state, nil
		},
	}

	return runDAG(ctx, d)
}

func runDAG(ctx context.Context, d *dag.DAG[RunState]) error {
	if err := d.Validate(); err != nil {
		return err
	}
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../../.env")
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}
	orch, err := orchestrator.NewOrchestrator[RunState](ctx, orchestrator.Config{PostgresDSN: dsn, GlobalTimeout: 2 * time.Minute})
	if err != nil {
		return err
	}
	defer func() { _ = orch.Close() }()
	run, err := orch.Run(ctx, d, orchestrator.GlobalInputs[RunState]{
		Value: RunState{
			IncidentID: "inc_2026_0606_001",
			AlertName:  "checkout-api error budget burn",
			Service:    "checkout-api",
			Symptoms:   []string{"5xx rate above 8%", "payment authorization latency above 2s", "us-west checkout failures"},
		},
	})
	if err != nil {
		return err
	}
	fmt.Println("run", run.ID, "finished for", d.Name)
	return nil
}
