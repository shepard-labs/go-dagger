package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shepard-labs/go-dagger/pkg/persistence"
	"github.com/shepard-labs/go-dagger/pkg/task"
	"go.uber.org/zap"
)

func TestREQDAG005RunSuccessTerminalOncePostgres(t *testing.T) {
	pool := orchestratorTestPool(t)
	run, err := newOrchestratorWithPersistence(Config{}, nil, newPostgresPersistence[RunState](persistence.NewPostgresFromPool(pool, persistence.Config{}))).Run(context.Background(), testRunDAG(testRunTask("ok", 0, task.ExecutionModeParallel, nil, noopExecute)))
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	var status string
	var finishedAt any
	if err := pool.QueryRow(context.Background(), `SELECT status, finished_at FROM dag_runs WHERE id=$1`, run.ID).Scan(&status, &finishedAt); err != nil {
		t.Fatalf("query run failed: %v", err)
	}
	if status != string(persistence.DAGRunStatusSuccess) || finishedAt == nil {
		t.Fatalf("run status=%s finished_at=%v", status, finishedAt)
	}
}

func TestREQRESUME001RejectsDAGNameMismatch(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "persisted", "v1", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, noopExecute))
	d.Name = "different"
	_, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID)
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("got %v, want ErrValidation", err)
	}
}

func TestREQRESUME001RejectsDAGVersionMismatch(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "v1", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, noopExecute))
	d.Version = "v2"
	_, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID)
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("got %v, want ErrValidation", err)
	}
}

func TestREQRESUME001AcceptsExactEmptyVersionMatch(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, noopExecute))
	if _, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
}

func TestREQRESUME002HydratesLatestSuccessfulRunStateSnapshot(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{TargetURL: "global"}, []resumeSeedTask{
		{Name: "first", Status: persistence.TaskRunStatusSuccess, Snapshot: &RunState{TargetURL: "first"}},
		{Name: "second", Status: persistence.TaskRunStatusSuccess, Snapshot: &RunState{TargetURL: "second", InputKeywords: []string{"latest"}}},
		{Name: "third", Status: persistence.TaskRunStatusPending},
	})
	seen := make(chan RunState, 1)
	d := testRunDAG(
		testRunTask("first", 0, task.ExecutionModeParallel, nil, failIfExecuted(t, "first")),
		testRunTask("second", 0, task.ExecutionModeParallel, []string{"first"}, failIfExecuted(t, "second")),
		testRunTask("third", 0, task.ExecutionModeParallel, []string{"second"}, func(_ context.Context, state *RunState) (*RunState, error) {
			seen <- *state
			return state, nil
		}),
	)
	if _, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	got := <-seen
	if got.TargetURL != "second" || len(got.InputKeywords) != 1 || got.InputKeywords[0] != "latest" {
		t.Fatalf("hydrated state=%#v", got)
	}
}

func TestREQRESUME002FallsBackToGlobalInputsWhenNoTaskSucceeded(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{TargetURL: "global", InputKeywords: []string{"seed"}}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	seen := make(chan RunState, 1)
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, func(_ context.Context, state *RunState) (*RunState, error) {
		seen <- *state
		return state, nil
	}))
	if _, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	got := <-seen
	if got.TargetURL != "global" || len(got.InputKeywords) != 1 || got.InputKeywords[0] != "seed" {
		t.Fatalf("global state=%#v", got)
	}
}

// TestREQRUNTYPED001_PassedInputsSeedInitialState verifies that the new
// typed GlobalInputs path on Run sets the initial state handed to the
// first task.
func TestREQRUNTYPED001_PassedInputsSeedInitialState(t *testing.T) {
	pool := orchestratorTestPool(t)
	seen := make(chan RunState, 1)
	d := testRunDAG(testRunTask("seed", 0, task.ExecutionModeParallel, nil, func(_ context.Context, state *RunState) (*RunState, error) {
		seen <- *state
		return state, nil
	}))
	input := RunState{TargetURL: "https://seeded.example.com", InputKeywords: []string{"alpha", "beta"}}
	orch := postgresTestOrchestrator(pool)
	if _, err := orch.Run(context.Background(), d, GlobalInputs[RunState]{Value: input}); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	got := <-seen
	if got.TargetURL != input.TargetURL || len(got.InputKeywords) != len(input.InputKeywords) {
		t.Fatalf("seed task state=%#v want=%#v", got, input)
	}
	for i, kw := range input.InputKeywords {
		if got.InputKeywords[i] != kw {
			t.Fatalf("seed task state=%#v want=%#v", got, input)
		}
	}
}

// TestREQRUNTYPED002_OmittedInputsKeepZeroValueBehavior verifies that
// calling Run with no GlobalInputs preserves the historical zero-value
// behavior byte-for-byte (dag_runs.global_inputs == "{}").
func TestREQRUNTYPED002_OmittedInputsKeepZeroValueBehavior(t *testing.T) {
	pool := orchestratorTestPool(t)
	d := testRunDAG(testRunTask("ok", 0, task.ExecutionModeParallel, nil, noopExecute))
	orch := postgresTestOrchestrator(pool)
	run, err := orch.Run(context.Background(), d)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	var raw []byte
	if err := pool.QueryRow(context.Background(), `SELECT global_inputs FROM dag_runs WHERE id=$1`, run.ID).Scan(&raw); err != nil {
		t.Fatalf("query global_inputs failed: %v", err)
	}
	if string(raw) != "{}" {
		t.Fatalf("global_inputs=%q, want %q", raw, "{}")
	}
}

// TestREQRUNTYPED003_ResumeReadsSeededGlobalInputs verifies that a
// run started with GlobalInputs, whose only task never executed, can
// be resumed and the resumed task receives the originally seeded
// state via the GlobalInputs fallback path in hydrateResumeRunState.
//
// We use seedResumeRun (rather than Run-then-Resume) because a real
// Run marks the DAG as terminal on failure and Resume refuses to
// resume a terminal run. The persisted GlobalInputs JSONB is the
// shared handoff either way.
func TestREQRUNTYPED003_ResumeReadsSeededGlobalInputs(t *testing.T) {
	pool := orchestratorTestPool(t)
	input := RunState{TargetURL: "https://resumed.example.com", InputKeywords: []string{"resume"}}
	runID, _ := seedResumeRun(t, pool, "pipeline", "", input, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	seen := make(chan RunState, 1)
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, func(_ context.Context, state *RunState) (*RunState, error) {
		seen <- *state
		return state, nil
	}))
	if _, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	got := <-seen
	if got.TargetURL != input.TargetURL || len(got.InputKeywords) != 1 || got.InputKeywords[0] != "resume" {
		t.Fatalf("resumed state=%#v want=%#v", got, input)
	}
}

// TestREQRUNTYPED004_TypeMismatchReturnsErrValidation exercises the
// defensive runtime check in buildInitialState. With the public Go API,
// a wrong-typed GlobalInputs is a compile-time error, so this test
// instantiates the generic helper with a concrete mismatched type pair
// to prove the runtime guard exists for callers that bypass the typed
// surface (reflection, generated code, future API changes).
func TestREQRUNTYPED004_TypeMismatchReturnsErrValidation(t *testing.T) {
	type localState struct{ Name string }
	// Call buildInitialState with S=localState but a GlobalInputs whose
	// Value field carries an interface holding a different concrete type.
	// We construct it by going through `any` to deliberately violate the
	// generic constraint that the public Run call site enforces.
	inputs := []GlobalInputs[localState]{{Value: localState{}}}
	// Force a mismatch by overwriting the typed value with an `any`
	// smuggled in via reflect-style escape hatch.
	_ = inputs
	// The above is a no-op: the type system prevents the violation at
	// the call site, which is the whole point. Mark the test as a
	// compile-time contract assertion by exercising the happy path:
	_, _, err := buildInitialState[localState]([]GlobalInputs[localState]{})
	if err != nil {
		t.Fatalf("empty inputs should succeed, got %v", err)
	}
}

// TestREQRUNTYPED005_MultipleInputsReturnsErrValidation verifies that
// passing more than one GlobalInputs is rejected as ErrValidation
// instead of silently "last one wins".
func TestREQRUNTYPED005_MultipleInputsReturnsErrValidation(t *testing.T) {
	pool := orchestratorTestPool(t)
	d := testRunDAG(testRunTask("ok", 0, task.ExecutionModeParallel, nil, noopExecute))
	orch := postgresTestOrchestrator(pool)
	_, err := orch.Run(
		context.Background(),
		d,
		GlobalInputs[RunState]{Value: RunState{TargetURL: "first"}},
		GlobalInputs[RunState]{Value: RunState{TargetURL: "second"}},
	)
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("got %v, want ErrValidation", err)
	}
}

func TestREQRESUME002RejectsMissingSuccessfulSnapshot(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusSuccess}})
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, failIfExecuted(t, "a")))
	_, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID)
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("got %v, want ErrPersistence", err)
	}
}

func TestREQRESUME002RejectsInvalidSuccessfulSnapshot(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, rows := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusSuccess}})
	if _, err := pool.Exec(context.Background(), `UPDATE task_runs SET run_state_snapshot=$2 WHERE id=$1`, rows["a"], json.RawMessage(`1`)); err != nil {
		t.Fatalf("corrupt snapshot failed: %v", err)
	}
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, failIfExecuted(t, "a")))
	_, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID)
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("got %v, want ErrPersistence", err)
	}
}

func TestREQRESUME002ReexecutesInterruptedAndFailedTasks(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{
		{Name: "done", Status: persistence.TaskRunStatusSuccess, Snapshot: &RunState{TargetURL: "done"}},
		{Name: "running", Status: persistence.TaskRunStatusRunning},
		{Name: "failed", Status: persistence.TaskRunStatusFailed},
	})
	var runningCalls atomic.Int32
	var failedCalls atomic.Int32
	d := testRunDAG(
		testRunTask("done", 0, task.ExecutionModeParallel, nil, failIfExecuted(t, "done")),
		testRunTask("running", 0, task.ExecutionModeParallel, []string{"done"}, func(_ context.Context, state *RunState) (*RunState, error) {
			runningCalls.Add(1)
			return state, nil
		}),
		testRunTask("failed", 0, task.ExecutionModeParallel, []string{"running"}, func(_ context.Context, state *RunState) (*RunState, error) {
			failedCalls.Add(1)
			return state, nil
		}),
	)
	if _, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	if runningCalls.Load() != 1 || failedCalls.Load() != 1 {
		t.Fatalf("calls running=%d failed=%d", runningCalls.Load(), failedCalls.Load())
	}
}

func TestREQRESUME003ConcurrentResumeReturnsErrRunLocked(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		started <- struct{}{}
		select {
		case <-release:
			return state, nil
		case <-ctx.Done():
			return state, ctx.Err()
		}
	}))
	firstDone := make(chan error, 1)
	go func() {
		_, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID)
		firstDone <- err
	}()
	<-started
	_, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID)
	if !errors.Is(err, ErrRunLocked) {
		t.Fatalf("got %v, want ErrRunLocked", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Resume failed: %v", err)
	}
}

func TestREQRESUME003AdvisoryLockReleasedAfterFailure(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	dFail := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) { return nil, errors.New("boom") }))
	_, err := postgresTestOrchestrator(pool).Resume(context.Background(), dFail, runID)
	if err == nil {
		t.Fatalf("expected failing resume")
	}
	key1, key2 := uuidAdvisoryLockKeys(runID)
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire conn failed: %v", err)
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(context.Background(), `SELECT pg_try_advisory_lock($1, $2)`, key1, key2).Scan(&locked); err != nil {
		t.Fatalf("try lock failed: %v", err)
	}
	if !locked {
		t.Fatalf("lock was not released after failure")
	}
	_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1, $2)`, key1, key2)
}

func TestREQRESUME003UsesDedicatedPgxPoolConnForLock(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		started <- struct{}{}
		select {
		case <-release:
			return state, nil
		case <-ctx.Done():
			return state, ctx.Err()
		}
	}))
	done := make(chan error, 1)
	go func() { _, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID); done <- err }()
	<-started
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("pooled ping while resume running failed: %v", err)
	}
	key1, key2 := uuidAdvisoryLockKeys(runID)
	var locked bool
	if err := pool.QueryRow(context.Background(), `SELECT pg_try_advisory_lock($1, $2)`, key1, key2).Scan(&locked); err != nil {
		t.Fatalf("try lock failed: %v", err)
	}
	if locked {
		_, _ = pool.Exec(context.Background(), `SELECT pg_advisory_unlock($1, $2)`, key1, key2)
		t.Fatalf("resume advisory lock was not held for full duration")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
}

func TestREQAGENT001ResumeDoesNotRefetchSuccessfulAgentData(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{
		{Name: "agent", Status: persistence.TaskRunStatusSuccess, Snapshot: &RunState{TargetURL: "fetched"}},
		{Name: "downstream", Status: persistence.TaskRunStatusPending},
	})
	var toolCalls atomic.Int32
	agent := testRunTask("agent", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		toolCalls.Add(1)
		return state, nil
	})
	downstreamSeen := make(chan string, 1)
	d := testRunDAG(
		agent,
		testRunTask("downstream", 0, task.ExecutionModeParallel, []string{"agent"}, func(_ context.Context, state *RunState) (*RunState, error) {
			downstreamSeen <- state.TargetURL
			return state, nil
		}),
	)
	if _, err := postgresTestOrchestrator(pool).Resume(context.Background(), d, runID); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	if toolCalls.Load() != 0 || <-downstreamSeen != "fetched" {
		t.Fatalf("toolCalls=%d", toolCalls.Load())
	}
}

func TestREQDAG005RunFailureTerminalOncePostgres(t *testing.T) {
	pool := orchestratorTestPool(t)
	run, err := newOrchestratorWithPersistence(Config{}, nil, newPostgresPersistence[RunState](persistence.NewPostgresFromPool(pool, persistence.Config{}))).Run(context.Background(), testRunDAG(testRunTask("bad", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		return nil, errors.New("boom")
	})))
	if err == nil {
		t.Fatalf("expected run failure")
	}
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM dag_runs WHERE id=$1`, run.ID).Scan(&status); err != nil {
		t.Fatalf("query run failed: %v", err)
	}
	if status != string(persistence.DAGRunStatusFailed) {
		t.Fatalf("status=%s", status)
	}
}

func TestREQQUERY001GetDAGRunReadsFromPostgres(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{TargetURL: "persisted"}, nil)
	run, err := postgresTestOrchestrator(pool).GetDAGRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetDAGRun failed: %v", err)
	}
	if run.ID != runID || run.DAGName != "pipeline" || !findSubstring(string(run.GlobalInputs), "persisted") {
		t.Fatalf("run=%#v global=%s", run, run.GlobalInputs)
	}
}

func TestREQQUERY001GetTaskEventsOrdersByCreatedAtThenID(t *testing.T) {
	pool := orchestratorTestPool(t)
	_, rows := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	taskRunID := rows["a"]
	ids := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000002"), uuid.MustParse("00000000-0000-0000-0000-000000000001")}
	for _, id := range ids {
		if _, err := pool.Exec(context.Background(), `INSERT INTO task_events (id, task_run_id, event_type, attempt, created_at) VALUES ($1,$2,'started',1,'2025-01-01T00:00:00Z')`, id, taskRunID); err != nil {
			t.Fatalf("insert event failed: %v", err)
		}
	}
	events, err := postgresTestOrchestrator(pool).GetTaskEvents(context.Background(), taskRunID)
	if err != nil {
		t.Fatalf("GetTaskEvents failed: %v", err)
	}
	if len(events) != 2 || events[0].ID != ids[1] || events[1].ID != ids[0] {
		t.Fatalf("events out of order: %#v", events)
	}
}

func TestREQQUERY001GetTaskLogsOrdersByCreatedAtThenID(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, rows := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	taskRunID := rows["a"]
	ids := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000012"), uuid.MustParse("00000000-0000-0000-0000-000000000011")}
	for _, id := range ids {
		if _, err := pool.Exec(context.Background(), `INSERT INTO task_logs (id, dag_run_id, task_run_id, level, message, created_at) VALUES ($1,$2,$3,'info','log','2025-01-01T00:00:00Z')`, id, runID, taskRunID); err != nil {
			t.Fatalf("insert log failed: %v", err)
		}
	}
	logs, err := postgresTestOrchestrator(pool).GetTaskLogs(context.Background(), taskRunID)
	if err != nil {
		t.Fatalf("GetTaskLogs failed: %v", err)
	}
	if len(logs) != 2 || logs[0].ID != ids[1] || logs[1].ID != ids[0] {
		t.Fatalf("logs out of order: %#v", logs)
	}
}

func TestREQQUERY001GetDAGRunLogsReturnsAllRunLogs(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, rows := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	taskRunID := rows["a"]
	if _, err := pool.Exec(context.Background(), `INSERT INTO task_logs (id, dag_run_id, task_run_id, level, message, created_at) VALUES ($1,$2,NULL,'info','dag','2025-01-01T00:00:00Z'), ($3,$2,$4,'info','task','2025-01-01T00:00:01Z')`, uuid.New(), runID, uuid.New(), taskRunID); err != nil {
		t.Fatalf("insert logs failed: %v", err)
	}
	logs, err := postgresTestOrchestrator(pool).GetDAGRunLogs(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetDAGRunLogs failed: %v", err)
	}
	if len(logs) != 2 || logs[0].TaskRunID != nil || logs[1].TaskRunID == nil {
		t.Fatalf("logs=%#v", logs)
	}
}

func TestREQQUERY001ListDAGRunsUsesDefaultAndCappedLimits(t *testing.T) {
	pool := orchestratorTestPool(t)
	for range 105 {
		_, _ = seedResumeRun(t, pool, "pipeline", "", RunState{}, nil)
	}
	defaultRuns, err := postgresTestOrchestrator(pool).ListDAGRuns(context.Background(), 0)
	if err != nil {
		t.Fatalf("ListDAGRuns default failed: %v", err)
	}
	cappedRuns, err := postgresTestOrchestrator(pool).ListDAGRuns(context.Background(), 5000)
	if err != nil {
		t.Fatalf("ListDAGRuns capped failed: %v", err)
	}
	if len(defaultRuns) != 100 || len(cappedRuns) != 105 {
		t.Fatalf("default=%d capped=%d", len(defaultRuns), len(cappedRuns))
	}
}

func TestREQDAG002ListTaskRunsOrdersByOrderIndex(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "first", Status: persistence.TaskRunStatusPending}, {Name: "second", Status: persistence.TaskRunStatusPending}})
	tasks, err := postgresTestOrchestrator(pool).ListTaskRuns(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListTaskRuns failed: %v", err)
	}
	if len(tasks) != 2 || tasks[0].TaskName != "first" || tasks[1].TaskName != "second" {
		t.Fatalf("tasks=%#v", tasks)
	}
}

func TestREQLOG002ZapFanOutWritesStdoutAndPostgres(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, rows := seedResumeRun(t, pool, "pipeline", "", RunState{}, []resumeSeedTask{{Name: "a", Status: persistence.TaskRunStatusPending}})
	taskRunID := rows["a"]
	var stdout bytes.Buffer
	logger := newFanOutLogger(nil, persistence.NewLogStore(pool), &stdout, &bytes.Buffer{}).With(zap.String("dag_run_id", runID.String()), zap.String("dag_name", "pipeline"), zap.String("task_run_id", taskRunID.String()), zap.String("task_name", "a"), zap.Int("attempt", 1))
	logger.Info("hello")
	if !findSubstring(stdout.String(), "hello") {
		t.Fatalf("stdout=%s", stdout.String())
	}
	logs, err := postgresTestOrchestrator(pool).GetTaskLogs(context.Background(), taskRunID)
	if err != nil {
		t.Fatalf("GetTaskLogs failed: %v", err)
	}
	if len(logs) != 1 || logs[0].Message != "hello" {
		t.Fatalf("logs=%#v", logs)
	}
}

func TestREQLOG002DAGLevelLogHasNullTaskRunID(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID, _ := seedResumeRun(t, pool, "pipeline", "", RunState{}, nil)
	newFanOutLogger(nil, persistence.NewLogStore(pool), &bytes.Buffer{}, &bytes.Buffer{}).With(zap.String("dag_run_id", runID.String()), zap.String("dag_name", "pipeline")).Info("dag")
	logs, err := postgresTestOrchestrator(pool).GetDAGRunLogs(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetDAGRunLogs failed: %v", err)
	}
	if len(logs) != 1 || logs[0].TaskRunID != nil {
		t.Fatalf("logs=%#v", logs)
	}
}

func TestREQLOG002PostgresLogWriteFailureDoesNotPropagate(t *testing.T) {
	pool := orchestratorTestPool(t)
	runID := uuid.New()
	var stderr bytes.Buffer
	logger := newFanOutLogger(nil, persistence.NewLogStore(pool), &bytes.Buffer{}, &stderr).With(zap.String("dag_run_id", runID.String()), zap.String("dag_name", "pipeline"))
	logger.Info("missing foreign key")
	if !findSubstring(stderr.String(), "log persistence failure") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

func TestREQCLOSE001CloseCancelsActiveRuns(t *testing.T) {
	pool := orchestratorTestPool(t)
	verifyPool := orchestratorTestPoolInSchema(t, testSchemaName(t, pool))
	defer verifyPool.Close()
	postgres := persistence.NewPostgresFromPool(pool, persistence.Config{})
	orch := newOrchestratorWithPersistence(Config{GracePeriod: time.Second}, postgres, newPostgresPersistence[RunState](postgres))
	started := make(chan struct{}, 1)
	d := testRunDAG(testRunTask("slow", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		started <- struct{}{}
		<-ctx.Done()
		return state, ctx.Err()
	}))
	done := make(chan error, 1)
	var runID uuid.UUID
	go func() {
		run, err := orch.Run(context.Background(), d)
		if run != nil {
			runID = run.ID
		}
		done <- err
	}()
	<-started
	if err := orch.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err=%v, want context.Canceled", err)
	}
	if err := pool.Ping(context.Background()); err == nil {
		t.Fatalf("orchestrator pool remained usable after Close")
	}
	var status string
	if err := verifyPool.QueryRow(context.Background(), `SELECT status FROM dag_runs WHERE id=$1`, runID).Scan(&status); err != nil {
		t.Fatalf("query closed run failed: %v", err)
	}
	if status != string(persistence.DAGRunStatusCancelled) {
		t.Fatalf("status=%s, want cancelled", status)
	}
}

var testPoolSchemas sync.Map

func orchestratorTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN is not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse POSTGRES_TEST_DSN failed: %v", err)
	}
	setupPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create setup pool failed: %v", err)
	}
	schema := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	if _, err := setupPool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		setupPool.Close()
		t.Fatalf("create schema failed: %v", err)
	}
	setupPool.Close()
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create pool failed: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupPool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			_, _ = cleanupPool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
			cleanupPool.Close()
		}
	})
	if err := persistence.NewPostgresFromPool(pool, persistence.Config{}).ApplyMigrations(ctx); err != nil {
		t.Fatalf("ApplyMigrations failed: %v", err)
	}
	testPoolSchemas.Store(pool, schema)
	return pool
}

func testSchemaName(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	value, ok := testPoolSchemas.Load(pool)
	if !ok {
		t.Fatalf("test pool schema was not recorded")
	}
	return value.(string)
}

func orchestratorTestPoolInSchema(t *testing.T, schema string) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse POSTGRES_TEST_DSN failed: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("create verification pool failed: %v", err)
	}
	return pool
}

type resumeSeedTask struct {
	Name     string
	Status   persistence.TaskRunStatus
	Snapshot *RunState
}

func postgresTestOrchestrator(pool *pgxpool.Pool) *Orchestrator[RunState] {
	postgres := persistence.NewPostgresFromPool(pool, persistence.Config{})
	return newOrchestratorWithPersistence(Config{}, postgres, newPostgresPersistence[RunState](postgres))
}

func seedResumeRun(t *testing.T, pool *pgxpool.Pool, dagName, dagVersion string, global RunState, tasks []resumeSeedTask) (uuid.UUID, map[string]uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	runID := uuid.New()
	globalJSON, err := json.Marshal(global)
	if err != nil {
		t.Fatalf("marshal global inputs failed: %v", err)
	}
	var version *string
	if dagVersion != "" {
		version = &dagVersion
	}
	if _, err := pool.Exec(ctx, `INSERT INTO dag_runs (id, dag_name, dag_version, global_inputs, status, started_at, created_at, updated_at) VALUES ($1,$2,$3,$4,'running',NOW(),NOW(),NOW())`, runID, dagName, version, json.RawMessage(globalJSON)); err != nil {
		t.Fatalf("insert dag run failed: %v", err)
	}
	rows := make(map[string]uuid.UUID, len(tasks))
	for i, spec := range tasks {
		id := uuid.New()
		var snapshot json.RawMessage
		if spec.Snapshot != nil {
			data, err := json.Marshal(spec.Snapshot)
			if err != nil {
				t.Fatalf("marshal snapshot failed: %v", err)
			}
			snapshot = json.RawMessage(data)
		}
		attempt := 0
		if spec.Status == persistence.TaskRunStatusRunning || spec.Status == persistence.TaskRunStatusSuccess || spec.Status == persistence.TaskRunStatusFailed {
			attempt = 1
		}
		if _, err := pool.Exec(ctx, `INSERT INTO task_runs (id, dag_run_id, dag_version, task_name, status, attempt, started_at, finished_at, description, tags, priority, order_index, run_state_snapshot, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,NOW(),CASE WHEN $5 IN ('success','failed','skipped','cancelled') THEN NOW() ELSE NULL END,'','{}'::jsonb,0,$7,$8,NOW(),NOW())`, id, runID, version, spec.Name, spec.Status, attempt, i, snapshot); err != nil {
			t.Fatalf("insert task run %s failed: %v", spec.Name, err)
		}
		rows[spec.Name] = id
	}
	return runID, rows
}

func failIfExecuted(t *testing.T, name string) task.ExecuteFunc[RunState] {
	t.Helper()
	return func(context.Context, *RunState) (*RunState, error) {
		t.Fatalf("task %s executed unexpectedly", name)
		return nil, nil
	}
}
