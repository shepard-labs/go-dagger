# AGENTS.md

Guidance for agents working in this repo. `README.md` has the architecture and
feature reference; this file is only the operational gotchas you'd otherwise miss.

## Commands

No Makefile/Taskfile — use `go` directly.

```bash
go build ./...                          # compiles (no codegen step)
go vet ./...                             # CI runs this; run before test
gofmt -l .                              # CI fails on non-empty output; fix with gofmt -w .
go test ./...                            # runs clean WITHOUT Postgres (integration tests skip on missing POSTGRES_DSN)
go test -race ./...                      # CI runs the race detector separately
go test ./pkg/dag/... ./pkg/task/...     # pure unit tests, no DB needed
go test -run TestName ./pkg/orchestrator/ # single test; integration tests need POSTGRES_DSN
```

CI order (`.github/workflows/ci.yml`): `gofmt -l .` → `go test ./...` → `go test -race ./...` → `go vet ./...` → `staticcheck` (action) → `govulncheck` (non-blocking). Reproduce the gate with `gofmt -l . && go vet ./... && go test -race ./...`.

## Test prerequisites

- `go test ./...` passes **without** Postgres. Integration tests in `pkg/orchestrator` (`orchestrator_integration_test.go:594`) and `pkg/persistence` (`persistence_test.go:293`) `t.Skip` when `POSTGRES_DSN` is unset.
- To run the full suite: start Postgres, set `POSTGRES_DSN=postgres://user:pass@localhost:5432/dbname?sslmode=disable`, then `go test -race ./...`. Tests apply migrations themselves via `ApplyMigrations` (idempotent `CREATE TABLE IF NOT EXISTS`).

## Examples need a schema

Examples call `NewOrchestrator` but **never** `ApplyMigrations` — they assume the schema already exists. Before running any Postgres-backed example against a fresh database, apply migrations once:

```go
// add this before orch.Run, or run via a one-off program:
postgres, _ := persistence.NewPostgres(ctx, persistence.Config{DSN: dsn})
postgres.ApplyMigrations(ctx)
```

The embedded migration is `pkg/persistence/migrations/001_schema.sql`.

## Environment

- Examples load `.env` then `../../.env` (repo root) via `godotenv`. A root-level `.env` with `POSTGRES_DSN` is enough; copy from `.env.example`.
- `ANTHROPIC_API_KEY` required for `examples/llm-agent-dag` and `examples/llm-agent-dag-yaml`.
- `GOPRIVATE=github.com/shepard-labs/*` is set in CI for the private `go-ai-sdk` dependency. If `go build` fails on `github.com/shepard-labs/go-ai-sdk` with a sum/db error locally, run `go env -w GOPRIVATE=github.com/shepard-labs/*` (or `GONOSUMCHECK`).

## Architecture boundaries

- `pkg/orchestrator` executes **one DAG run per `Run`/`Resume` call**. It owns no worker pool, broker, or distributed scheduler — multi-run concurrency and job claiming (`SELECT ... FOR UPDATE SKIP LOCKED`) belong to the caller (`cmd/server`). Don't add scheduling loops to the orchestrator package.
- `pkg/task` is pure domain model — no orchestrator or persistence imports. Keep it dependency-free.
- `pkg/dag` edges are **control dependencies only** (`Task.DependsOn`); tasks fetch large external inputs inside `Execute` and append durable results to state `S`, which is snapshotted after each successful task.
- `NewOrchestrator` does **not** auto-apply migrations. Callers run `ApplyMigrations` themselves.

## Style

- No comments unless asked (matches repo convention — public APIs are documented via godoc, code is comment-light).
- Generic type parameter on everything user-facing: `DAG[S]`, `Task[S]`, `Orchestrator[S]`, `ExecuteFunc[S]`. Don't erase the generic when editing.
- Errors wrap sentinels from `internal/apperrors` (`ErrValidation`, `ErrPersistence`). Persisted errors redact the DSN via `persistence.RedactDSN` / `orchestrator.RedactLogValue` — never log a raw DSN.

## Release

CI auto-creates a semver tag (`vX.Y.Z`, patch-incremented from the latest tag) on every push to `main` after `test` + `staticcheck` pass. Don't tag releases manually.