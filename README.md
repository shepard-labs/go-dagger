# go-dagger

[![CI](https://github.com/shepard-labs/go-dagger/actions/workflows/ci.yml/badge.svg)](https://github.com/shepard-labs/go-dagger/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/shepard-labs/go-dagger.svg)](https://pkg.go.dev/github.com/shepard-labs/go-dagger)
[![Go Report Card](https://goreportcard.com/badge/github.com/shepard-labs/go-dagger)](https://goreportcard.com/report/github.com/shepard-labs/go-dagger)
[![Release](https://img.shields.io/github/v/release/shepard-labs/go-dagger?sort=semver)](https://github.com/shepard-labs/go-dagger/releases)
[![License](https://img.shields.io/github/license/shepard-labs/go-dagger)](LICENSE)

A durable, Postgres-backed DAG orchestrator for Go. Define typed task graphs in
code or YAML, run them with persisted state, retries, cancellation, and resumability,
and query the run history from the same orchestrator instance.

`go-dagger` executes one DAG run per `Run` / `Resume` call. It is intentionally
single-run focused: scheduling comes from in-memory DAG topology and task state,
not Postgres polling. Multi-run concurrency, job claiming, and worker pools belong
to the calling application (see [Deployment](#deployment)).

## Features

| Feature | Go API | YAML | `Config` | Built-in |
| --- | :---: | :---: | :---: | :---: |
| **Typed DAGs** — `DAG[S]` threads a user state `S` through every task; snapshots persisted after each successful task. | `*dag.DAG[S]` | — | — | ✓ |
| **Programmatic or YAML definitions** — build in Go, or parse a strict YAML doc (unknown fields + duplicate keys rejected) and wire functions/hooks/tools by name. | `dag.DAG` / `dag.ParseYAML` / `dag.SerializeYAML` | `dag.yaml` | — | ✓ |
| **Control dependencies** — `Task.DependsOn` declares edges; scheduler emits tasks as deps complete. | `Task.DependsOn` | `depends_on` | — | ✓ |
| **Execution modes** — `parallel` (runs within the concurrency limit) or `sequential` (reserves a single sequential lane). | `Task.Mode` | `execution_mode` | — | ✓ |
| **Priority scheduling** — ready queue sorts by priority, then DAG order, then name. | `Task.Priority` | `priority` | — | ✓ |
| **Concurrency limit** — caps ready tasks; DAG limit wins, else config, else `runtime.NumCPU()`. | `DAG.ConcurrencyLimit` | `concurrency_limit` | `ConcurrencyLimit` | ✓ |
| **Retries with backoff** — per-task `none` / `linear` / `exponential` backoff, jitter, max-delay cap. | `Task.Retry` (`RetryConfig`) | `max_attempts`, `backoff`, `backoff_base`, `backoff_max`, `backoff_jitter` | — | ✓ |
| **Per-task timeout** — each attempt runs under its own context; a grace period lets in-flight work settle. | `Task.Timeout` | `timeout` | `GracePeriod` | ✓ |
| **DAG / global timeout** — deadline for the whole run. | `DAG.Timeout` | `timeout` | `GlobalTimeout` | ✓ |
| **Before/After hooks** — lifecycle callbacks run before and after each task attempt. | `Task.BeforeHooks` / `AfterHooks` (+ `HookRegistry` for YAML) | `before_hooks`, `after_hooks` | — | ✓ |
| **Tools** — `ToolRegistry` dispatches JSON-message tools by name (used by LLM agent examples for structured output). | `Task.Tools` / `ToolRegistry` (+ `ToolNames` for YAML) | `tools` | — | ✓ |
| **Persisted run lifecycle** — every run, task, event, and log line written to Postgres; status survives restarts. | — | — | `PostgresDSN`, `PostgresSchema`, `PostgresPoolSize`, `PersistenceTimeout`, `PersistenceRetries` | ✓ |
| **Resume** — continue a `running` run from its latest successful task snapshots; advisory lock prevents concurrent resumes. | `orch.Resume(ctx, dag, runID)` | — | — | ✓ |
| **Cancel** — request cancellation of an active run; downstream tasks are skipped or cancelled. | `orch.Cancel(runID)` | — | — | ✓ |
| **Query API** — read runs, task runs, events, and logs without starting a run. | `GetDAGRun`, `ListDAGRuns`, `GetTaskRun`, `ListTaskRuns`, `GetTaskEvents`, `GetTaskLogs`, `GetDAGRunLogs` | — | — | ✓ |
| **Structured logging + redaction** — `zap` fans out to stdout/stderr and `task_logs`; Postgres DSNs and `password=`/`passwd=`/`pwd=`/`secret=` key-value pairs are redacted before any sink. | — | — | `Logger` | ✓ |
| **Dry-run validation** — validate a DAG without creating a persisted run. | `orch.DryRun(dag)` | — | — | ✓ |

## Requirements

- Go `1.25.0` or newer.
- Postgres (uses `pgx/v5`, JSONB, and advisory locks). CI runs against Postgres 18.

## Installation

```bash
go get github.com/shepard-labs/go-dagger
```

## Quick start

The smallest end-to-end run is [`examples/minimal`](./examples/minimal): two tasks,
one dependency, seeded state. Set up a Postgres DSN and run it:

```bash
cp .env.example .env   # then edit .env to point at your database
go run ./examples/minimal
```

A minimal program looks like:

```go
package main

import (
	"context"
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
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		log.Fatal("POSTGRES_DSN is required")
	}

	d := &dag.DAG[RunState]{
		Name:  "minimal",
		Tasks: map[string]*task.Task[RunState]{},
	}
	d.Tasks["seed"] = &task.Task[RunState]{
		Name: "seed",
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			return state, nil
		},
	}
	d.Tasks["publish"] = &task.Task[RunState]{
		Name:      "publish",
		DependsOn: []string{"seed"},
		Execute: func(ctx context.Context, state *RunState) (*RunState, error) {
			// ... publish state.TargetURL ...
			return state, nil
		},
	}
	if err := d.Validate(); err != nil {
		log.Fatal(err)
	}

	orch, err := orchestrator.NewOrchestrator[RunState](
		context.Background(),
		orchestrator.Config{PostgresDSN: dsn},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer orch.Close()

	run, err := orch.Run(context.Background(), d, orchestrator.GlobalInputs[RunState]{
		Value: RunState{TargetURL: "https://example.com"},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("run", run.ID, "finished")
}
```

### YAML definitions

Define the graph in a `dag.yaml` and wire Go functions by name:

```yaml
name: video-publishing-yaml-example
version: "1.0.0"
concurrency_limit: 3
timeout: 2m
tasks:
  - name: ingest-upload
    description: Accept the uploaded source video.
    depends_on: []
    execute:
      type: go
      function: examples.video.ingest_upload

  - name: probe-media
    description: Read container, duration, and source metadata before fan-out.
    depends_on: [ingest-upload]
    execute:
      type: go
      function: examples.video.probe_media

  - name: transcode-1080p
    depends_on: [probe-media]
    timeout: 30s
    execute:
      type: go
      function: examples.video.transcode_1080p

  - name: publish-video
    depends_on: [transcode-1080p]
    execute:
      type: go
      function: examples.video.publish_video
```

Parse with:

```go
dag, err := dag.ParseYAML[RunState](yamlBytes,
	task.FunctionRegistry[RunState]{
		"examples.video.ingest_upload":    ingestUpload,
		"examples.video.probe_media":      probeMedia,
		"examples.video.transcode_1080p":  transcode1080p,
		"examples.video.publish_video":    publishVideo,
	},
	nil, // hooks
	nil, // tools
)
```

See [`examples/video-publishing-yaml`](./examples/video-publishing-yaml) and
[`examples/humanoid-drone-fusion-yaml`](./examples/humanoid-drone-fusion-yaml)
for complete runnable YAML DAGs.

## Architecture

```
cmd/server            Deployment patterns (Cloud Run Jobs, VPS, startup recovery)
examples/             Runnable examples (programmatic + YAML DAGs, query, resume)
internal/apperrors/   Shared sentinel errors
pkg/dag/              Immutable DAG topology, validation, YAML parse/serialize
pkg/orchestrator/     Run/Resume/Cancel/Close, scheduler, task executor, query API
pkg/persistence/      Postgres stores (dag/task/event/log), embedded migrations
pkg/task/             Task domain model, retry config, tool registry, hooks
```

### Packages

#### `pkg/dag`

`DAG[S]` is the immutable, validated task graph. `Validate` materializes the
adjacency list, in-degree map, and deterministic `TaskOrder`, and runs a
topological sort that detects cycles. `ParseYAML` converts a strict YAML
document (unknown fields rejected, duplicate keys rejected) into a `DAG[S]`,
binding function/hook/tool names to Go registries. `SerializeYAML` round-trips
a `DAG` back to YAML.

Edges are **control dependencies only**, declared via `Task.DependsOn`. Tasks
fetch large external inputs inside `Execute` and append durable results to the
state `S`; the orchestrator snapshots `S` after each successful task.

#### `pkg/task`

The pure domain model: `Task[S]`, `ExecuteFunc[S]`, `RetryConfig` (with
`BackoffType` none/linear/exponential, jitter, max-delay), `ExecutionMode`
(parallel/sequential), `ToolRegistry` + `Tool`, and `HookRegistry` + `Hook`
(before/after callbacks). No orchestrator or persistence imports.

#### `pkg/orchestrator`

`Orchestrator[S]` validates, runs, resumes, cancels, and queries DAG executions.
`NewOrchestrator[S]` opens a `pgxpool` against the configured DSN (optional
`PostgresSchema` sets `search_path`). Each `Run` call:

1. Validates a cloned DAG.
2. Builds initial state from `GlobalInputs[S]` (or zero-value `S`).
3. Creates a persisted `dag_runs` row and `task_runs` rows.
4. Executes tasks via the in-memory scheduler — a priority ready queue bounded by
   `effectiveConcurrency(dag.ConcurrencyLimit, config.ConcurrencyLimit)` and
   gated by a single `sequential` lane for `ExecutionModeSequential` tasks.
5. Marks the run `success`, `failed`, or `cancelled`.

`Resume` acquires a Postgres advisory lock keyed on the run ID, reconciles the
persisted task rows against the DAG, hydrates `S` from the latest successful task
snapshot (or `global_inputs`), and re-executes only the non-successful tasks.
`Cancel` cancels the active run's context. `Close` cancels all active runs, waits
up to `GracePeriod`, then closes the pool.

Logging fans out via a custom `zapcore.Core` to stdout/stderr **and** the
`task_logs` table, with `dag_run_id`/`task_run_id` fields extracted for
persistence. `RedactLogValue` strips Postgres DSNs and `password=`/`passwd=`/`pwd=`/`secret=`
key-value pairs from messages and fields before they reach any sink.

#### `pkg/persistence`

Postgres stores for `dag_runs`, `task_runs`, `task_events`, and `task_logs`.
`NewPostgres` opens a `pgxpool` with ping retry; `NewPostgresFromPool` wraps an
existing pool (for application-level pooling). `ApplyMigrations` runs the embedded
SQL in [`migrations/001_schema.sql`](./pkg/persistence/migrations/001_schema.sql)
in filename order. Writes use `WithWriteRetry` (timeout + exponential retry) and
all errors redact the DSN before wrapping with `apperrors.ErrPersistence`.

### Schema

Four tables, created by the embedded migration:

| Table | Purpose |
| --- | --- |
| `dag_runs` | One row per DAG execution (`running` / `success` / `failed` / `cancelled`), with `global_inputs` JSONB. |
| `task_runs` | One row per task per run, with `status`, `attempt`, `order_index`, `tags`, `priority`, and `run_state_snapshot` JSONB. |
| `task_events` | Append-only lifecycle events (`started`, `succeeded`, `failed`, `retried`, `cancelled`, `skipped`, `retry_exhausted`, `after_hook_failed`). |
| `task_logs` | Structured log lines scoped to a DAG run and (optionally) a task run. |

Migrations are **not** applied automatically by `NewOrchestrator`. Call
`postgres.ApplyMigrations(ctx)` once against a fresh database (or after a schema
bump) before starting runs. The migrations use `CREATE TABLE IF NOT EXISTS`, so
running them repeatedly is safe.

## Configuration

`orchestrator.Config`:

| Field | Description |
| --- | --- |
| `PostgresDSN` | **Required.** Postgres connection string. |
| `PostgresSchema` | Sets the connection's `search_path` for unqualified table names. Empty = server default (`public`). |
| `PostgresPoolSize` | `pgxpool.MaxConns`. 0 = pgx default. |
| `PersistenceTimeout` | Per-write timeout. Default `10s`. |
| `PersistenceRetries` | Write retry count. Default `3`. |
| `ConcurrencyLimit` | Global task concurrency when `DAG.ConcurrencyLimit` is unset. Default = `runtime.NumCPU()`. |
| `GlobalTimeout` | Deadline for the whole run. 0 = no deadline. |
| `GracePeriod` | Time given to in-flight tasks to settle after their attempt context is cancelled (on `Close` / timeout), and to `Close` for active runs to finish. Default `30s`. |
| `Logger` | Optional base `*zap.Logger`. Defaults to a no-op logger, which `NewOrchestrator` wraps into a JSON fan-out logger (stdout + `task_logs`) when Postgres is configured. |

Environment variables (read by examples, not the library):

- `POSTGRES_DSN` — required by every Postgres-backed example.
- `ANTHROPIC_API_KEY` — required by `llm-agent-dag` and `llm-agent-dag-yaml`.
- `RESEARCH_AGENT_MODEL`, `PLANNING_AGENT_MODEL`, `CRITIQUE_AGENT_MODEL`,
  `SYNTHESIS_AGENT_MODEL`, `RISK_AGENT_MODEL` — optional per-agent model overrides.

## Examples

See [`examples/README.md`](./examples/README.md) for the full index. Highlights:

| Example | Command | Notes |
| --- | --- | --- |
| `minimal` | `go run ./examples/minimal` | Smallest persisted DAG run. |
| `order-fulfillment` | `go run ./examples/order-fulfillment` | Programmatic fan-out / fan-in. |
| `etl` | `go run ./examples/etl` | Extract / transform / load workflow. |
| `incident-response` | `go run ./examples/incident-response` | SRE workflow with parallel comms. |
| `video-publishing-yaml` | `go run ./examples/video-publishing-yaml` | YAML DAG with per-task timeouts. |
| `humanoid-drone-fusion-yaml` | `go run ./examples/humanoid-drone-fusion-yaml` | YAML DAG, wide parallel fan-out. |
| `public-api-chain-yaml` | `go run ./examples/public-api-chain-yaml` | YAML DAG calling public APIs. |
| `llm-agent-dag` | `go run ./examples/llm-agent-dag` | Programmatic LLM agent DAG (Anthropic). |
| `llm-agent-dag-yaml` | `go run ./examples/llm-agent-dag-yaml` | YAML LLM agent DAG with research/risk fan-out. |
| `llm-agent-failure` | `go run ./examples/llm-agent-failure` | Mock LLM clients, retry exhaustion demo. |
| `query` | `DAG_RUN_ID=<id> go run ./examples/query` | Read-only inspection of a persisted run. |
| `resume` | `DAG_RUN_ID=<id> go run ./examples/resume` | Resume an existing run. |
| `yaml` | `go run ./examples/yaml` | YAML parse / serialize round-trip (no Postgres). |
| `cycle` | `go run ./examples/cycle` | Intentional cycle-validation error. |

## Deployment

`pkg/orchestrator` executes one DAG run per `Run`/`Resume` call. It does not own a
worker pool, broker, or distributed scheduler — multi-run concurrency belongs to
the application process. See [`cmd/server/README.md`](./cmd/server/README.md) for
the recommended patterns:

- **Cloud Run Jobs** — Cloud Scheduler invokes the job process on a cadence; each
  process claims one application job with `SELECT ... FOR UPDATE SKIP LOCKED`,
  constructs an orchestrator, calls `Run`/`Resume`, marks the job terminal, and exits.
- **VPS** — a long-running process spawns `N` goroutines, each running the
  claim → run → mark → sleep loop. `N` goroutines = `N` concurrent DAG runs.
- **Startup recovery** — on startup, list `dag_runs` with status `running` and call
  `Resume(ctx, dag, runID)` for runs the process owns. The advisory lock prevents
  two callers resuming the same run.

The job-claiming query belongs to `cmd/server` or the calling application, not
`pkg/orchestrator`. Use `github.com/jackc/pgx/v5/pgxpool` for application-level
Postgres work.

## Development

```bash
go test ./...           # unit + integration tests (needs POSTGRES_DSN for integration)
go test -race ./...     # race detector
go vet ./...            # go vet
gofmt -l .              # formatting check (CI fails on non-empty output)
staticcheck ./...       # static analysis (CI runs dominikh/staticcheck-action)
govulncheck ./...       # vulnerability scan (CI runs this, non-blocking)
```

CI (`.github/workflows/ci.yml`) runs on push/PR to `main` against a Postgres 18
service container, executes `go test`, `go test -race`, `go vet`, `gofmt`,
`staticcheck`, and `govulncheck`, and auto-creates a semver release tag on `main`
pushes.

### Test environment

Integration tests in `pkg/orchestrator` and `pkg/persistence` need a reachable
Postgres. CI uses `postgres://postgres:postgres@localhost:5432/go_dagger_test?sslmode=disable`.
Locally, point `POSTGRES_DSN` at a throwaway database.

## License

This project is part of the `shepard-labs` organization. See repository history for
licensing details.