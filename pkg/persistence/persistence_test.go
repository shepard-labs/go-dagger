package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shepard-labs/go-dagger/internal/apperrors"
	dagpkg "github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct{}

func TestREQPERSIST001PersistenceErrorsRedactDSN(t *testing.T) {
	dsn := "postgres://user:secret@example.com:5432/dbname?sslmode=disable"
	err := persistenceError("connect", errors.New("failed for "+dsn+" password secret user user"), dsn)
	if !errors.Is(err, apperrors.ErrPersistence) {
		t.Fatalf("expected ErrPersistence, got %v", err)
	}
	message := err.Error()
	for _, leaked := range []string{dsn, "secret", "postgres://user"} {
		if strings.Contains(message, leaked) {
			t.Fatalf("persistence error leaked %q: %s", leaked, message)
		}
	}
}

func TestREQPERSIST003AllIDsGeneratedByGo(t *testing.T) {
	ids := []uuid.UUID{NewDAGRunID(), NewTaskRunID(), NewTaskEventID(), NewTaskLogID()}
	for _, id := range ids {
		if id == uuid.Nil {
			t.Fatalf("expected non-zero uuid")
		}
		if id.Version() != 4 {
			t.Fatalf("expected uuid v4, got %s", id)
		}
	}
}

func TestREQPERSIST001WriteTimeoutFailsRun(t *testing.T) {
	pg := NewPostgresFromPool(nil, Config{PersistenceTimeout: time.Nanosecond, WriteRetries: 0})
	err := pg.WithWriteRetry(context.Background(), "slow write", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, apperrors.ErrPersistence) {
		t.Fatalf("expected ErrPersistence, got %v", err)
	}
}

func TestREQPERSIST001WriteRetrySuccessAllowsRunToContinue(t *testing.T) {
	pg := NewPostgresFromPool(nil, Config{PersistenceTimeout: time.Second, WriteRetries: 2, RetryBaseDelay: time.Nanosecond})
	attempts := 0
	err := pg.WithWriteRetry(context.Background(), "retry write", func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected retry success, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestREQPERSIST001WriteRetryExhaustionFailsRun(t *testing.T) {
	pg := NewPostgresFromPool(nil, Config{PersistenceTimeout: time.Second, WriteRetries: 2, RetryBaseDelay: time.Nanosecond})
	attempts := 0
	err := pg.WithWriteRetry(context.Background(), "retry write", func(context.Context) error {
		attempts++
		return errors.New("persistent")
	})
	if !errors.Is(err, apperrors.ErrPersistence) {
		t.Fatalf("expected ErrPersistence, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestREQPERSIST002TaskTerminalSnapshotEventAtomicSuccess(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	run, taskRun := seedRunAndTask(t, pool)
	snapshot := json.RawMessage(`{"target_url":"https://example.com","input_keywords":["go"]}`)
	store := NewTaskStore[RunState](pool, "")
	if err := store.MarkTaskSucceededWithSnapshotAndEvent(ctx, taskRun.ID, snapshot, 1); err != nil {
		t.Fatalf("MarkTaskSucceededWithSnapshotAndEvent failed: %v", err)
	}
	updated, err := store.Get(ctx, taskRun.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if updated.Status != TaskRunStatusSuccess || len(updated.RunStateSnapshot) == 0 {
		t.Fatalf("success snapshot was not persisted atomically: %#v", updated)
	}
	events, err := NewEventStore(pool, "").ListByTaskRun(ctx, taskRun.ID)
	if err != nil {
		t.Fatalf("ListByTaskRun failed: %v", err)
	}
	if len(events) != 1 || events[0].EventType != TaskEventSucceeded || events[0].Attempt != 1 {
		t.Fatalf("unexpected events for run %s: %#v", run.ID, events)
	}
}

func TestREQPERSIST002InjectedFailureLeavesNoPartialTerminalState(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	_, taskRun := seedRunAndTask(t, pool)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE task_runs SET status='success', run_state_snapshot=$2, finished_at=NOW(), updated_at=NOW() WHERE id=$1`, taskRun.ID, json.RawMessage(`{"target_url":"x","input_keywords":[]}`)); err != nil {
		t.Fatalf("update in tx failed: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	updated, err := NewTaskStore[RunState](pool, "").Get(ctx, taskRun.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if updated.Status != TaskRunStatusPending || len(updated.RunStateSnapshot) != 0 {
		t.Fatalf("rolled back terminal state leaked: %#v", updated)
	}
}

func TestREQDAG002ListTaskRunsOrdersByOrderIndex(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	dagStore := NewDAGStore(pool, "")
	run, err := dagStore.CreateRunning(ctx, "pipeline", nil, nil)
	if err != nil {
		t.Fatalf("CreateRunning failed: %v", err)
	}
	d := &dagpkg.DAG[RunState]{Name: "pipeline", Tasks: map[string]*task.Task[RunState]{}, TaskOrder: []string{"b", "a", "c"}}
	for _, name := range d.TaskOrder {
		d.Tasks[name] = &task.Task[RunState]{Name: name, Tags: map[string]string{}, Execute: func(context.Context, *RunState) (*RunState, error) { return &RunState{}, nil }}
	}
	if _, err := NewTaskStore[RunState](pool, "").CreateForDAG(ctx, run.ID, d); err != nil {
		t.Fatalf("CreateForDAG failed: %v", err)
	}
	rows, err := NewTaskStore[RunState](pool, "").ListByRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("ListByRun failed: %v", err)
	}
	got := []string{}
	for _, row := range rows {
		got = append(got, row.TaskName)
	}
	want := []string{"b", "a", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got order %v, want %v", got, want)
		}
	}
}

func TestREQPERSIST003MigrationsDoNotDefineUUIDDefaults(t *testing.T) {
	assertNoUUIDDefaultsInMigration(t)
}

func TestSchemaNoUUIDDefaultGeneration(t *testing.T) {
	assertNoUUIDDefaultsInMigration(t)
}

func TestSchemaAppliesCleanlyToFreshPostgres(t *testing.T) {
	testPool(t)
}

func TestSchemaDagRunsHasGlobalInputsJSONBDefault(t *testing.T) {
	pool := testPool(t)
	assertColumn(t, pool, "dag_runs", "global_inputs", "jsonb", "NO")
	var defaultValue *string
	if err := pool.QueryRow(context.Background(), `SELECT column_default FROM information_schema.columns WHERE table_name='dag_runs' AND column_name='global_inputs'`).Scan(&defaultValue); err != nil {
		t.Fatalf("query default failed: %v", err)
	}
	if defaultValue == nil || !strings.Contains(*defaultValue, "'{}'::jsonb") {
		t.Fatalf("unexpected global_inputs default: %v", defaultValue)
	}
}

func TestSchemaTaskRunsHasRunStateSnapshotJSONB(t *testing.T) {
	pool := testPool(t)
	assertColumn(t, pool, "task_runs", "run_state_snapshot", "jsonb", "YES")
}

func TestSchemaTaskLogsExistsWithNullableTaskRunID(t *testing.T) {
	pool := testPool(t)
	assertColumn(t, pool, "task_logs", "task_run_id", "uuid", "YES")
}

func TestSchemaStatusConstraintsRejectInvalidValues(t *testing.T) {
	pool := testPool(t)
	_, err := pool.Exec(context.Background(), `INSERT INTO dag_runs (id, dag_name, status, started_at) VALUES ($1,'pipeline','bad',NOW())`, uuid.New())
	if err == nil {
		t.Fatalf("expected dag_runs status constraint failure")
	}
}

func TestSchemaEventTypeConstraintsRejectInvalidValues(t *testing.T) {
	pool := testPool(t)
	_, taskRun := seedRunAndTask(t, pool)
	_, err := pool.Exec(context.Background(), `INSERT INTO task_events (id, task_run_id, event_type) VALUES ($1,$2,'bad')`, uuid.New(), taskRun.ID)
	if err == nil {
		t.Fatalf("expected task_events event_type constraint failure")
	}
}

func TestSchemaTaskLogsLevelConstraintRejectsInvalidValues(t *testing.T) {
	pool := testPool(t)
	run, _ := seedRunAndTask(t, pool)
	_, err := pool.Exec(context.Background(), `INSERT INTO task_logs (id, dag_run_id, level, message) VALUES ($1,$2,'bad','message')`, uuid.New(), run.ID)
	if err == nil {
		t.Fatalf("expected task_logs level constraint failure")
	}
}

func TestSchemaIndexesExistForRunStatusTaskOrderEventsAndLogs(t *testing.T) {
	pool := testPool(t)
	want := []string{"idx_dag_runs_status", "idx_dag_runs_running", "idx_task_runs_dag_run_order", "idx_task_events_task_run_event", "idx_task_logs_created_at"}
	for _, indexName := range want {
		var exists bool
		if err := pool.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname=$1)`, indexName).Scan(&exists); err != nil {
			t.Fatalf("index query failed: %v", err)
		}
		if !exists {
			t.Fatalf("missing index %s", indexName)
		}
	}
}

func TestSchemaCascadeDeleteRemovesTaskRunsEventsAndLogs(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	run, taskRun := seedRunAndTask(t, pool)
	if _, err := NewEventStore(pool, "").Insert(ctx, taskRun.ID, TaskEventStarted, 1, nil); err != nil {
		t.Fatalf("insert event failed: %v", err)
	}
	if _, err := NewLogStore(pool, "").Insert(ctx, run.ID, &taskRun.ID, LogLevelInfo, "message", nil); err != nil {
		t.Fatalf("insert log failed: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM dag_runs WHERE id=$1`, run.ID); err != nil {
		t.Fatalf("delete dag_run failed: %v", err)
	}
	for table, column := range map[string]string{"task_runs": "dag_run_id", "task_events": "task_run_id", "task_logs": "dag_run_id"} {
		var count int
		query := `SELECT COUNT(*) FROM ` + table
		if table == "task_events" {
			query += ` WHERE ` + column + `=$1`
			if err := pool.QueryRow(ctx, query, taskRun.ID).Scan(&count); err != nil {
				t.Fatalf("count %s failed: %v", table, err)
			}
		} else {
			query += ` WHERE ` + column + `=$1`
			if err := pool.QueryRow(ctx, query, run.ID).Scan(&count); err != nil {
				t.Fatalf("count %s failed: %v", table, err)
			}
		}
		if count != 0 {
			t.Fatalf("expected cascade delete from %s, count=%d", table, count)
		}
	}
}

func assertNoUUIDDefaultsInMigration(t *testing.T) {
	t.Helper()
	data, err := migrationFS.ReadFile("migrations/001_schema.sql")
	if err != nil {
		t.Fatalf("read migration failed: %v", err)
	}
	lower := strings.ToLower(string(data))
	for _, forbidden := range []string{"gen_random_uuid", "uuid_generate_v4", "default uuid"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("migration defines uuid default with %q", forbidden)
		}
	}
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is not set; skipping Postgres integration/schema test")
	}
	ctx := context.Background()
	baseConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse POSTGRES_DSN failed: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, baseConfig)
	if err != nil {
		t.Fatalf("create pool failed: %v", err)
	}
	t.Cleanup(pool.Close)
	schema := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	if _, err := pool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema failed: %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`) })
	if _, err := pool.Exec(ctx, `SET search_path TO `+schema); err != nil {
		t.Fatalf("set search_path failed: %v", err)
	}
	pg := NewPostgresFromPool(pool, Config{})
	if err := pg.ApplyMigrations(ctx); err != nil {
		t.Fatalf("ApplyMigrations failed: %v", err)
	}
	return pool
}

func seedRunAndTask(t *testing.T, pool *pgxpool.Pool) (*DAGRun, *TaskRun) {
	t.Helper()
	ctx := context.Background()
	run, err := NewDAGStore(pool, "").CreateRunning(ctx, "pipeline", nil, nil)
	if err != nil {
		t.Fatalf("CreateRunning failed: %v", err)
	}
	d := &dagpkg.DAG[RunState]{Name: "pipeline", Tasks: map[string]*task.Task[RunState]{"task": {Name: "task", Tags: map[string]string{}, Execute: func(context.Context, *RunState) (*RunState, error) { return &RunState{}, nil }}}, TaskOrder: []string{"task"}}
	taskRuns, err := NewTaskStore[RunState](pool, "").CreateForDAG(ctx, run.ID, d)
	if err != nil {
		t.Fatalf("CreateForDAG failed: %v", err)
	}
	return run, &taskRuns[0]
}

func assertColumn(t *testing.T, pool *pgxpool.Pool, table, column, dataType, nullable string) {
	t.Helper()
	var gotType, gotNullable string
	err := pool.QueryRow(context.Background(), `SELECT data_type, is_nullable FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2`, table, column).Scan(&gotType, &gotNullable)
	if err != nil {
		t.Fatalf("query column %s.%s failed: %v", table, column, err)
	}
	if gotType != dataType || gotNullable != nullable {
		t.Fatalf("%s.%s got type=%s nullable=%s, want type=%s nullable=%s", table, column, gotType, gotNullable, dataType, nullable)
	}
}
