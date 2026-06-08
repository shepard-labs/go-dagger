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
	BatchID          string         `json:"batch_id,omitempty"`
	SourceSystem     string         `json:"source_system,omitempty"`
	RawFiles         []string       `json:"raw_files,omitempty"`
	ExtractedRows    int            `json:"extracted_rows,omitempty"`
	RejectedRows     int            `json:"rejected_rows,omitempty"`
	ValidationErrors []string       `json:"validation_errors,omitempty"`
	NormalizedTables []string       `json:"normalized_tables,omitempty"`
	Metrics          map[string]int `json:"metrics,omitempty"`
	WarehouseTable   string         `json:"warehouse_table,omitempty"`
	DashboardURL     string         `json:"dashboard_url,omitempty"`
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	d := &dag.DAG[RunState]{
		Name:             "etl-example",
		ConcurrencyLimit: 2,
		TaskOrder:        []string{"discover-files", "extract", "validate", "transform", "load-warehouse", "refresh-dashboard"},
		Tasks:            map[string]*task.Task[RunState]{},
	}

	d.Tasks["discover-files"] = &task.Task[RunState]{
		Name:         "discover-files",
		FunctionName: "examples.etl.discover_files",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			fmt.Println("discovered", len(state.RawFiles), "files for", state.BatchID)
			return state, nil
		},
	}

	d.Tasks["extract"] = &task.Task[RunState]{
		Name:         "extract",
		DependsOn:    []string{"discover-files"},
		FunctionName: "examples.etl.extract",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.ExtractedRows = len(state.RawFiles) * 12500
			fmt.Println("extracted", state.ExtractedRows, "rows from", state.SourceSystem)
			return state, nil
		},
	}

	d.Tasks["validate"] = &task.Task[RunState]{
		Name:         "validate",
		DependsOn:    []string{"extract"},
		FunctionName: "examples.etl.validate",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.RejectedRows = 37
			state.ValidationErrors = []string{"37 rows missing billing country"}
			fmt.Println("validated batch", state.BatchID, "rejected", state.RejectedRows, "rows")
			return state, nil
		},
	}

	d.Tasks["transform"] = &task.Task[RunState]{
		Name:         "transform",
		DependsOn:    []string{"validate"},
		FunctionName: "examples.etl.transform",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			accepted := state.ExtractedRows - state.RejectedRows
			state.NormalizedTables = []string{"stg_orders", "stg_order_items", "stg_customers"}
			state.Metrics = map[string]int{"accepted_rows": accepted, "rejected_rows": state.RejectedRows, "source_files": len(state.RawFiles)}
			fmt.Println("transformed", accepted, "rows into", strings.Join(state.NormalizedTables, ", "))
			return state, nil
		},
	}

	d.Tasks["load-warehouse"] = &task.Task[RunState]{
		Name:         "load-warehouse",
		DependsOn:    []string{"transform"},
		FunctionName: "examples.etl.load_warehouse",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			if state.Metrics["accepted_rows"] == 0 {
				return state, fmt.Errorf("no accepted rows to load for %s", state.BatchID)
			}
			state.WarehouseTable = "analytics.fact_orders_daily"
			fmt.Println("loaded", state.Metrics["accepted_rows"], "rows into", state.WarehouseTable)
			return state, nil
		},
	}

	d.Tasks["refresh-dashboard"] = &task.Task[RunState]{
		Name:         "refresh-dashboard",
		DependsOn:    []string{"load-warehouse"},
		FunctionName: "examples.etl.refresh_dashboard",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			state.DashboardURL = fmt.Sprintf("https://bi.example.com/dashboards/orders?batch=%s", state.BatchID)
			fmt.Println("refreshed dashboard", state.DashboardURL, "from", state.WarehouseTable)
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
			BatchID:      "batch_2026_06_06_orders",
			SourceSystem: "shopify",
			RawFiles:     []string{"s3://raw/orders/2026-06-06/orders-0001.json", "s3://raw/orders/2026-06-06/orders-0002.json"},
		},
	})
	if err != nil {
		return err
	}
	fmt.Println("run", run.ID, "finished for", d.Name)
	return nil
}
