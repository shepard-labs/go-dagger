# Deployment Patterns

`pkg/orchestrator` executes one DAG run per `Run` or `Resume` call. It does not own a worker pool, broker, or distributed scheduler. Multi-run concurrency belongs to the application process.

## Cloud Run Jobs

Cloud Scheduler invokes the job process on a cadence. Each process connects to Postgres, claims one application job with `SELECT ... FOR UPDATE SKIP LOCKED`, constructs an orchestrator with a `pgxpool`-backed DSN, calls `Run` or `Resume`, marks the application job terminal, and exits.

The job claiming query belongs to `cmd/server` or the calling application, not `pkg/orchestrator`. During healthy DAG execution, scheduling decisions come from in-memory DAG topology and task state, not Postgres polling.

```go
pool, err := pgxpool.New(ctx, dsn)
if err != nil {
    return err
}
defer pool.Close()

tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

row := tx.QueryRow(ctx, `
    SELECT id, dag_run_id
    FROM jobs
    WHERE status = 'pending'
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1`)

// If a job is found, the application constructs an orchestrator and calls
// Run or Resume. This claim loop remains outside pkg/orchestrator.
```

## VPS

A long-running `cmd/server` can start `N` goroutines. Each goroutine runs a simple loop: claim one application job with `SELECT ... FOR UPDATE SKIP LOCKED`, call `Run` or `Resume`, mark the job terminal, then sleep when no job is available.

`N` goroutines means `N` concurrent DAG runs. The orchestrator package remains single-run focused.

## Startup Recovery

On startup, the application may list `dag_runs` with status `running` and explicitly call `Resume(ctx, dag, runID)` for runs it owns. Resume uses the orchestrator's advisory lock to prevent two callers from resuming the same run at once.

Use `github.com/jackc/pgx/v5` and `github.com/jackc/pgx/v5/pgxpool` for application-level Postgres work.
