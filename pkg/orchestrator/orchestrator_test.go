package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dagpkg "github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/persistence"
	"github.com/shepard-labs/go-dagger/pkg/task"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type RunState struct {
	TargetURL     string   `json:"target_url,omitempty"`
	InputKeywords []string `json:"input_keywords,omitempty"`
}

func TestREQDAG002SchedulesByPriorityTaskOrderThenName(t *testing.T) {
	store := newMemoryPersistence()
	order := make([]string, 0, 3)
	d := testRunDAG(
		testRunTask("order-b", 2, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
			order = append(order, "order-b")
			return &RunState{}, nil
		}),
		testRunTask("order-a", 2, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
			order = append(order, "order-a")
			return &RunState{}, nil
		}),
		testRunTask("priority", 1, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
			order = append(order, "priority")
			return &RunState{}, nil
		}),
	)
	d.ConcurrencyLimit = 1
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	want := []string{"priority", "order-b", "order-a"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("got order %v, want %v", order, want)
	}
}

func TestREQDAG003UsesDAGConcurrencyLimitWhenSet(t *testing.T) {
	if got := effectiveConcurrency(2, 5); got != 2 {
		t.Fatalf("got %d, want DAG limit 2", got)
	}
}

func TestREQDAG003UsesConfigConcurrencyLimitWhenDAGUnset(t *testing.T) {
	if got := effectiveConcurrency(0, 3); got != 3 {
		t.Fatalf("got %d, want config limit 3", got)
	}
}

func TestREQDAG003DefaultsConcurrencyToRuntimeNumCPU(t *testing.T) {
	old := runtimeNumCPU
	runtimeNumCPU = func() int { return 9 }
	defer func() { runtimeNumCPU = old }()
	if got := effectiveConcurrency(0, 0); got != 9 {
		t.Fatalf("got %d, want runtime NumCPU fallback", got)
	}
}

func TestREQDAG004StartsOnlyAfterDependenciesSucceed(t *testing.T) {
	store := newMemoryPersistence()
	var rootCommitted atomic.Bool
	d := testRunDAG(
		testRunTask("root", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
			return &RunState{TargetURL: "root"}, nil
		}),
		testRunTask("child", 0, task.ExecutionModeParallel, []string{"root"}, func(context.Context, *RunState) (*RunState, error) {
			if !rootCommitted.Load() {
				t.Fatalf("child started before root snapshot commit")
			}
			return &RunState{TargetURL: "child"}, nil
		}),
	)
	store.afterTaskSuccess = func(taskName string) {
		if taskName == "root" {
			rootCommitted.Store(true)
		}
	}
	if _, err := newTestOrchestrator(store, Config{ConcurrencyLimit: 2}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}

func TestREQDAG004HandlesControlOnlyDependencies(t *testing.T) {
	store := newMemoryPersistence()
	order := []string{}
	d := testRunDAG(
		testRunTask("gate", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
			order = append(order, "gate")
			return &RunState{}, nil
		}),
		testRunTask("after", 0, task.ExecutionModeParallel, []string{"gate"}, func(context.Context, *RunState) (*RunState, error) {
			order = append(order, "after")
			return &RunState{}, nil
		}),
	)
	d.ConcurrencyLimit = 1
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"gate", "after"}) {
		t.Fatalf("got order %v", order)
	}
}

func TestREQTASK002PassesRunStateToExecute(t *testing.T) {
	store := newMemoryPersistence()
	seen := false
	d := testRunDAG(testRunTask("task", 0, task.ExecutionModeParallel, nil, func(_ context.Context, state *RunState) (*RunState, error) {
		seen = state != nil
		state.InputKeywords = append(state.InputKeywords, "seen")
		return state, nil
	}))
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !seen {
		t.Fatalf("Execute did not receive current RunState")
	}
}

func TestREQSCHED001DoesNotReadPostgresForHealthyScheduling(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, noopExecute))
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if got := store.reads.Load(); got != 0 {
		t.Fatalf("healthy scheduler performed %d persistence reads", got)
	}
}

func TestREQSCHED001ReadyQueueDeterministic(t *testing.T) {
	tasks := map[string]*task.Task[RunState]{
		"b": {Name: "b", Priority: 2},
		"a": {Name: "a", Priority: 2},
		"p": {Name: "p", Priority: 1},
		"z": {Name: "z", Priority: 2},
	}
	q := newReadyQueue([]string{"z", "b", "a"}, tasks)
	for _, name := range []string{"b", "p", "a", "z"} {
		q.push(name)
	}
	got := []string{}
	for {
		name, ok := q.popRunnable(false)
		if !ok {
			break
		}
		got = append(got, name)
	}
	want := []string{"p", "z", "b", "a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestREQSCHED001BlocksWithoutSpinningAtConcurrencyLimit(t *testing.T) {
	store := newMemoryPersistence()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	d := testRunDAG(
		testRunTask("slow", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
			started <- struct{}{}
			<-release
			return &RunState{}, nil
		}),
		testRunTask("next", 0, task.ExecutionModeParallel, nil, noopExecute),
	)
	d.ConcurrencyLimit = 1
	done := make(chan error, 1)
	go func() { _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); done <- err }()
	<-started
	time.Sleep(20 * time.Millisecond)
	if store.runningAttempts.Load() != 1 {
		t.Fatalf("scheduler started another task while at concurrency limit")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}

func TestREQSCHED002SequentialTasksCountTowardGlobalLimit(t *testing.T) {
	store := newMemoryPersistence()
	started := make(chan string, 2)
	release := make(chan struct{})
	d := testRunDAG(
		testRunTask("seq", 0, task.ExecutionModeSequential, nil, func(context.Context, *RunState) (*RunState, error) {
			started <- "seq"
			<-release
			return &RunState{}, nil
		}),
		testRunTask("parallel", 1, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
			started <- "parallel"
			return &RunState{}, nil
		}),
	)
	d.ConcurrencyLimit = 1
	done := make(chan error, 1)
	go func() { _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); done <- err }()
	if got := <-started; got != "seq" {
		t.Fatalf("first task got %s", got)
	}
	select {
	case got := <-started:
		t.Fatalf("%s started while sequential task consumed the only slot", got)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}

func TestREQSCHED002CrossLevelSequentialGateIsGlobal(t *testing.T) {
	store := newMemoryPersistence()
	var running int32
	var maxRunning atomic.Int32
	seq := func(context.Context, *RunState) (*RunState, error) {
		current := atomic.AddInt32(&running, 1)
		updateMaxInt32(&maxRunning, current)
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&running, -1)
		return &RunState{}, nil
	}
	d := testRunDAG(
		testRunTask("root", 0, task.ExecutionModeSequential, nil, seq),
		testRunTask("middle", 0, task.ExecutionModeParallel, []string{"root"}, noopExecute),
		testRunTask("other", 0, task.ExecutionModeSequential, nil, seq),
		testRunTask("deep", 0, task.ExecutionModeSequential, []string{"middle"}, seq),
	)
	d.ConcurrencyLimit = 4
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if maxRunning.Load() > 1 {
		t.Fatalf("sequential gate allowed %d sequential tasks", maxRunning.Load())
	}
}

func TestREQDAG003NeverExceedsEffectiveConcurrency(t *testing.T) {
	store := newMemoryPersistence()
	var running int32
	var maxRunning atomic.Int32
	exec := func(context.Context, *RunState) (*RunState, error) {
		current := atomic.AddInt32(&running, 1)
		updateMaxInt32(&maxRunning, current)
		time.Sleep(time.Millisecond)
		atomic.AddInt32(&running, -1)
		return &RunState{}, nil
	}
	tasks := make([]*task.Task[RunState], 0, 20)
	for i := range 20 {
		tasks = append(tasks, testRunTask(fmt.Sprintf("t%02d", i), 0, task.ExecutionModeParallel, nil, exec))
	}
	d := testRunDAG(tasks...)
	d.ConcurrencyLimit = 3
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if maxRunning.Load() > 3 {
		t.Fatalf("max concurrency %d exceeded limit 3", maxRunning.Load())
	}
}

func TestREQTASK002CopiesRunStateBeforeDownstreamUse(t *testing.T) {
	store := newMemoryPersistence()
	returned := &RunState{InputKeywords: []string{"safe"}}
	seen := make(chan []string, 1)
	d := testRunDAG(
		testRunTask("root", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) { return returned, nil }),
		testRunTask("child", 0, task.ExecutionModeParallel, []string{"root"}, func(_ context.Context, state *RunState) (*RunState, error) {
			seen <- append([]string(nil), state.InputKeywords...)
			return state, nil
		}),
	)
	store.afterTaskSuccess = func(taskName string) {
		if taskName == "root" {
			returned.InputKeywords[0] = "mutated"
		}
	}
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if got := <-seen; !reflect.DeepEqual(got, []string{"safe"}) {
		t.Fatalf("downstream observed mutated state %v", got)
	}
}

func TestREQSCHED001CopiesRuntimeInDegreePerRun(t *testing.T) {
	d := testRunDAG(
		testRunTask("root", 0, task.ExecutionModeParallel, nil, noopExecute),
		testRunTask("child", 0, task.ExecutionModeParallel, []string{"root"}, noopExecute),
	)
	if err := d.Validate(); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
	original := copyInDegree(d.InDegree, d.TaskOrder)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if _, err := newTestOrchestrator(newMemoryPersistence(), Config{}).Run(context.Background(), d); err != nil {
				t.Errorf("Run failed: %v", err)
			}
		})
	}
	wg.Wait()
	if !reflect.DeepEqual(d.InDegree, original) {
		t.Fatalf("shared DAG in-degree mutated: got %#v want %#v", d.InDegree, original)
	}
}

func TestREQSCHED002AllowsOnlyOneSequentialTaskAtATime(t *testing.T) {
	TestREQSCHED002CrossLevelSequentialGateIsGlobal(t)
}

func TestREQDAG005RunSuccessTerminalOnce(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("ok", 0, task.ExecutionModeParallel, nil, noopExecute))
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if store.runTerminalCount.Load() != 1 || store.runStatus != persistence.DAGRunStatusSuccess {
		t.Fatalf("run terminal count/status = %d/%s", store.runTerminalCount.Load(), store.runStatus)
	}
}

func TestREQDAG005RunFailureTerminalOnce(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("bad", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) { return nil, errors.New("boom") }))
	_, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d)
	if err == nil {
		t.Fatalf("expected failure")
	}
	if store.runTerminalCount.Load() != 1 || store.runStatus != persistence.DAGRunStatusFailed {
		t.Fatalf("run terminal count/status = %d/%s", store.runTerminalCount.Load(), store.runStatus)
	}
}

func TestREQTASK002PersistsSnapshotOnlyOnSuccess(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("ok", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		return &RunState{TargetURL: "https://example.com"}, nil
	}))
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if len(store.tasks["ok"].RunStateSnapshot) == 0 {
		t.Fatalf("success snapshot not persisted")
	}
}

func TestREQTASK002DoesNotPersistSnapshotOnFailure(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("bad", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		return &RunState{TargetURL: "bad"}, errors.New("boom")
	}))
	_, _ = newTestOrchestrator(store, Config{}).Run(context.Background(), d)
	if len(store.tasks["bad"].RunStateSnapshot) != 0 {
		t.Fatalf("failure snapshot persisted: %s", store.tasks["bad"].RunStateSnapshot)
	}
}

func TestREQTASK004TaskSuccessTerminalOnce(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("ok", 0, task.ExecutionModeParallel, nil, noopExecute))
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if store.taskTerminalCounts["ok"].Load() != 1 || store.tasks["ok"].Status != persistence.TaskRunStatusSuccess {
		t.Fatalf("task terminal count/status = %d/%s", store.taskTerminalCounts["ok"].Load(), store.tasks["ok"].Status)
	}
}

func TestREQTASK004TaskFailureTerminalOnce(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("bad", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) { return nil, errors.New("boom") }))
	_, _ = newTestOrchestrator(store, Config{}).Run(context.Background(), d)
	if store.taskTerminalCounts["bad"].Load() != 1 || store.tasks["bad"].Status != persistence.TaskRunStatusFailed {
		t.Fatalf("task terminal count/status = %d/%s", store.taskTerminalCounts["bad"].Load(), store.tasks["bad"].Status)
	}
}

func TestREQTASK004TaskSkippedForFailedDependency(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(
		testRunTask("bad", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) { return nil, errors.New("boom") }),
		testRunTask("child", 0, task.ExecutionModeParallel, []string{"bad"}, noopExecute),
	)
	_, _ = newTestOrchestrator(store, Config{}).Run(context.Background(), d)
	if store.tasks["child"].Status != persistence.TaskRunStatusSkipped || store.tasks["child"].Attempt != 0 {
		t.Fatalf("child = %#v", store.tasks["child"])
	}
}

func TestREQPERSIST002DownstreamStartsOnlyAfterSnapshotTransactionCommits(t *testing.T) {
	TestREQDAG004StartsOnlyAfterDependenciesSucceed(t)
}

func TestREQHOOK001BeforeHooksRunBeforeExecute(t *testing.T) {
	store := newMemoryPersistence()
	order := []string{}
	taskA := testRunTask("hooked", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		order = append(order, "execute")
		return &RunState{}, nil
	})
	taskA.BeforeHooks = []task.BeforeHook[RunState]{func(context.Context, *RunState) error {
		order = append(order, "before")
		return nil
	}}
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), testRunDAG(taskA)); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"before", "execute"}) {
		t.Fatalf("order=%v", order)
	}
}

func TestREQHOOK001BeforeHookFailureSkipsExecute(t *testing.T) {
	store := newMemoryPersistence()
	executed := false
	taskA := testRunTask("hooked", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		executed = true
		return &RunState{}, nil
	})
	taskA.BeforeHooks = []task.BeforeHook[RunState]{func(context.Context, *RunState) error { return errors.New("before failed") }}
	_, err := newTestOrchestrator(store, Config{}).Run(context.Background(), testRunDAG(taskA))
	if err == nil {
		t.Fatalf("expected failure")
	}
	if executed {
		t.Fatalf("execute ran after before hook failure")
	}
}

func TestREQCANCEL001CancelUnknownRunReturnsErrRunNotFound(t *testing.T) {
	err := newTestOrchestrator(newMemoryPersistence(), Config{}).Cancel(uuid.New())
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("got %v, want ErrRunNotFound", err)
	}
}

func TestREQTIMEOUT001ZeroTimeoutMeansNoDeadline(t *testing.T) {
	runCtx, runCancel := runContext(context.Background(), 0)
	defer runCancel()
	if deadline, ok := runCtx.Deadline(); ok {
		t.Fatalf("zero global timeout set deadline %v", deadline)
	}
	attemptCtx, attemptCancel := attemptContext(context.Background(), 0)
	defer attemptCancel()
	if deadline, ok := attemptCtx.Deadline(); ok {
		t.Fatalf("zero task timeout set deadline %v", deadline)
	}
}

func TestREQHOOK001HooksReceiveCancelledContext(t *testing.T) {
	store := newMemoryPersistence()
	seenCancelled := make(chan bool, 1)
	taskA := testRunTask("hooked", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		<-ctx.Done()
		return state, ctx.Err()
	})
	taskA.Timeout = time.Millisecond
	taskA.AfterHooks = []task.AfterHook[RunState]{func(ctx context.Context, _ *RunState, _ error) error {
		seenCancelled <- ctx.Err() != nil
		return nil
	}}
	_, _ = newTestOrchestrator(store, Config{GracePeriod: 50 * time.Millisecond}).Run(context.Background(), testRunDAG(taskA))
	if !<-seenCancelled {
		t.Fatalf("after hook did not receive cancelled context")
	}
}

func TestREQDAG005CancellationTerminalOnce(t *testing.T) {
	store := newMemoryPersistence()
	started := make(chan struct{}, 1)
	d := testRunDAG(testRunTask("slow", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		started <- struct{}{}
		<-ctx.Done()
		return state, ctx.Err()
	}))
	orch := newTestOrchestrator(store, Config{GracePeriod: 50 * time.Millisecond})
	done := make(chan error, 1)
	go func() { _, err := orch.Run(context.Background(), d); done <- err }()
	<-started
	var runID uuid.UUID
	orch.mu.Lock()
	for id := range orch.activeRuns {
		runID = id
	}
	orch.mu.Unlock()
	if err := orch.Cancel(runID); err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if store.runTerminalCount.Load() != 1 || store.runStatus != persistence.DAGRunStatusCancelled {
		t.Fatalf("run terminal count/status = %d/%s", store.runTerminalCount.Load(), store.runStatus)
	}
}

func TestREQDAG005GlobalTimeoutTerminalFailedOnce(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("slow", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		<-ctx.Done()
		return state, ctx.Err()
	}))
	_, err := newTestOrchestrator(store, Config{GlobalTimeout: time.Millisecond, GracePeriod: 50 * time.Millisecond}).Run(context.Background(), d)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
	if store.runTerminalCount.Load() != 1 || store.runStatus != persistence.DAGRunStatusFailed {
		t.Fatalf("run terminal count/status = %d/%s", store.runTerminalCount.Load(), store.runStatus)
	}
}

func TestREQDAG005LateTerminalWriteIgnored(t *testing.T) {
	TestREQTASK004LateGoroutineCannotOverwriteTaskState(t)
}

func TestREQTASK002DoesNotPersistSnapshotOnCancellation(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("slow", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		<-ctx.Done()
		state.TargetURL = "late"
		return state, ctx.Err()
	}))
	_, _ = newTestOrchestrator(store, Config{GlobalTimeout: time.Millisecond, GracePeriod: 50 * time.Millisecond}).Run(context.Background(), d)
	if len(store.tasks["slow"].RunStateSnapshot) != 0 {
		t.Fatalf("cancelled task snapshot persisted: %s", store.tasks["slow"].RunStateSnapshot)
	}
}

func TestREQTASK004TaskCancelledTerminalOnce(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(testRunTask("slow", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		<-ctx.Done()
		return state, ctx.Err()
	}))
	_, _ = newTestOrchestrator(store, Config{GlobalTimeout: time.Millisecond, GracePeriod: 50 * time.Millisecond}).Run(context.Background(), d)
	if store.taskTerminalCounts["slow"].Load() != 1 {
		t.Fatalf("task terminal count=%d", store.taskTerminalCounts["slow"].Load())
	}
}

func TestREQTASK005AttemptZeroForNeverStartedTasks(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(
		testRunTask("slow", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
			<-ctx.Done()
			return state, ctx.Err()
		}),
		testRunTask("pending", 0, task.ExecutionModeParallel, nil, noopExecute),
	)
	d.ConcurrencyLimit = 1
	_, _ = newTestOrchestrator(store, Config{GlobalTimeout: time.Millisecond, GracePeriod: 50 * time.Millisecond}).Run(context.Background(), d)
	if store.tasks["pending"].Attempt != 0 || store.tasks["pending"].Status != persistence.TaskRunStatusCancelled {
		t.Fatalf("pending=%#v", store.tasks["pending"])
	}
}

func TestREQTASK005AttemptOneForFirstStartedAttempt(t *testing.T) {
	store := newMemoryPersistence()
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), testRunDAG(testRunTask("ok", 0, task.ExecutionModeParallel, nil, noopExecute))); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if store.tasks["ok"].Attempt != 1 {
		t.Fatalf("attempt=%d", store.tasks["ok"].Attempt)
	}
}

func TestREQTASK005RetriesIncrementAttempt(t *testing.T) {
	store := newMemoryPersistence()
	var calls int
	taskA := testRunTask("retry", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("try again")
		}
		return &RunState{}, nil
	})
	taskA.Retry.MaxAttempts = 3
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), testRunDAG(taskA)); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if store.tasks["retry"].Attempt != 3 {
		t.Fatalf("attempt=%d", store.tasks["retry"].Attempt)
	}
}

func TestREQTASK005EventsCarryAttemptNumbers(t *testing.T) {
	store := newMemoryPersistence()
	var calls int
	taskA := testRunTask("retry", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("try again")
		}
		return &RunState{}, nil
	})
	taskA.Retry.MaxAttempts = 2
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), testRunDAG(taskA)); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	want := []int{1, 2, 2, 2}
	got := make([]int, 0, len(store.events["retry"]))
	for _, event := range store.events["retry"] {
		got = append(got, event.Attempt)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("attempts=%v want %v events=%v", got, want, store.events["retry"])
	}
}

func TestREQRETRY001RetryEventsAppendOnly(t *testing.T) {
	store := newMemoryPersistence()
	var calls int
	taskA := testRunTask("retry", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("try again")
		}
		return &RunState{}, nil
	})
	taskA.Retry.MaxAttempts = 3
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), testRunDAG(taskA)); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	retried := 0
	for _, event := range store.events["retry"] {
		if event.EventType == persistence.TaskEventRetried {
			retried++
		}
	}
	if retried != 2 {
		t.Fatalf("retried events=%d events=%v", retried, store.events["retry"])
	}
}

func TestREQHOOK001BeforeHookFailureCanRetry(t *testing.T) {
	store := newMemoryPersistence()
	var hookCalls, execCalls int
	taskA := testRunTask("hooked", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		execCalls++
		return &RunState{}, nil
	})
	taskA.Retry.MaxAttempts = 2
	taskA.BeforeHooks = []task.BeforeHook[RunState]{func(context.Context, *RunState) error {
		hookCalls++
		if hookCalls == 1 {
			return errors.New("before failed")
		}
		return nil
	}}
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), testRunDAG(taskA)); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if hookCalls != 2 || execCalls != 1 {
		t.Fatalf("hookCalls=%d execCalls=%d", hookCalls, execCalls)
	}
}

func TestREQHOOK001AfterHookFailureEmitsEvent(t *testing.T) {
	store := newMemoryPersistence()
	taskA := testRunTask("hooked", 0, task.ExecutionModeParallel, nil, noopExecute)
	taskA.AfterHooks = []task.AfterHook[RunState]{func(context.Context, *RunState, error) error { return errors.New("after failed") }}
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), testRunDAG(taskA)); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if store.tasks["hooked"].Status != persistence.TaskRunStatusSuccess {
		t.Fatalf("status=%s", store.tasks["hooked"].Status)
	}
	found := false
	for _, event := range store.events["hooked"] {
		if event.EventType == persistence.TaskEventAfterHookFailed {
			found = true
		}
	}
	if !found {
		t.Fatalf("after_hook_failed not emitted: %v", store.events["hooked"])
	}
}

func TestREQCANCEL001CallerContextCancellationCancelsRun(t *testing.T) {
	store := newMemoryPersistence()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	d := testRunDAG(testRunTask("slow", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		started <- struct{}{}
		<-ctx.Done()
		return state, ctx.Err()
	}))
	done := make(chan error, 1)
	go func() {
		_, err := newTestOrchestrator(store, Config{GracePeriod: 50 * time.Millisecond}).Run(ctx, d)
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if store.runStatus != persistence.DAGRunStatusCancelled {
		t.Fatalf("status=%s", store.runStatus)
	}
}

func TestREQCANCEL001CancelOnlyAffectsMatchingRun(t *testing.T) {
	storeA := newMemoryPersistence()
	storeB := newMemoryPersistence()
	startedA := make(chan struct{}, 1)
	startedB := make(chan struct{}, 1)
	releaseB := make(chan struct{})
	dagA := testRunDAG(testRunTask("slow-a", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		startedA <- struct{}{}
		<-ctx.Done()
		return state, ctx.Err()
	}))
	dagB := testRunDAG(testRunTask("slow-b", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		startedB <- struct{}{}
		select {
		case <-ctx.Done():
			return state, ctx.Err()
		case <-releaseB:
			return state, nil
		}
	}))
	orchA := newTestOrchestrator(storeA, Config{GracePeriod: 50 * time.Millisecond})
	orchB := newTestOrchestrator(storeB, Config{GracePeriod: 50 * time.Millisecond})
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { _, err := orchA.Run(context.Background(), dagA); doneA <- err }()
	go func() { _, err := orchB.Run(context.Background(), dagB); doneB <- err }()
	<-startedA
	<-startedB
	var runIDA uuid.UUID
	orchA.mu.Lock()
	for id := range orchA.activeRuns {
		runIDA = id
	}
	orchA.mu.Unlock()
	if err := orchA.Cancel(runIDA); err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}
	if err := <-doneA; !errors.Is(err, context.Canceled) {
		t.Fatalf("run A err=%v", err)
	}
	select {
	case err := <-doneB:
		t.Fatalf("run B was affected before release: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(releaseB)
	if err := <-doneB; err != nil {
		t.Fatalf("run B failed: %v", err)
	}
	if storeA.runStatus != persistence.DAGRunStatusCancelled || storeB.runStatus != persistence.DAGRunStatusSuccess {
		t.Fatalf("statuses A/B = %s/%s", storeA.runStatus, storeB.runStatus)
	}
}

func TestREQCANCEL001PendingTasksMarkedCancelled(t *testing.T) {
	TestREQTASK005AttemptZeroForNeverStartedTasks(t)
}

func TestREQCANCEL001SkippedTasksRemainSkipped(t *testing.T) {
	store := newMemoryPersistence()
	d := testRunDAG(
		testRunTask("bad", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) { return nil, errors.New("boom") }),
		testRunTask("child", 0, task.ExecutionModeParallel, []string{"bad"}, noopExecute),
	)
	_, _ = newTestOrchestrator(store, Config{}).Run(context.Background(), d)
	if store.tasks["child"].Status != persistence.TaskRunStatusSkipped {
		t.Fatalf("child=%#v", store.tasks["child"])
	}
}

func TestREQCANCEL001CancelTerminalRunReturnsErrRunTerminal(t *testing.T) {
	orch := newTestOrchestrator(newMemoryPersistence(), Config{})
	runID := uuid.New()
	if err := orch.registerRun(runID, func() {}); err != nil {
		t.Fatalf("register run failed: %v", err)
	}
	if err := orch.markRunTerminal(context.Background(), runID, persistence.DAGRunStatusSuccess, nil); err != nil {
		t.Fatalf("mark terminal failed: %v", err)
	}
	if err := orch.Cancel(runID); !errors.Is(err, ErrRunTerminal) {
		t.Fatalf("got %v, want ErrRunTerminal", err)
	}
}

func TestREQCLOSE001CloseIsIdempotent(t *testing.T) {
	orch := newTestOrchestrator(newMemoryPersistence(), Config{GracePeriod: time.Millisecond})
	first := orch.Close()
	if first != nil {
		t.Fatalf("first Close err=%v, want nil", first)
	}
	second := orch.Close()
	if first != second {
		t.Fatalf("Close did not memoize error: first=%v second=%v", first, second)
	}
}

func TestREQCLOSE001RunResumeCancelAfterCloseReturnErrOrchestratorClosed(t *testing.T) {
	orch := newTestOrchestrator(newMemoryPersistence(), Config{})
	if err := orch.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	d := testRunDAG(testRunTask("a", 0, task.ExecutionModeParallel, nil, noopExecute))
	if _, err := orch.Run(context.Background(), d); !errors.Is(err, ErrOrchestratorClosed) {
		t.Fatalf("Run err=%v, want ErrOrchestratorClosed", err)
	}
	if _, err := orch.Resume(context.Background(), d, uuid.New()); !errors.Is(err, ErrOrchestratorClosed) {
		t.Fatalf("Resume err=%v, want ErrOrchestratorClosed", err)
	}
	if err := orch.Cancel(uuid.New()); !errors.Is(err, ErrOrchestratorClosed) {
		t.Fatalf("Cancel err=%v, want ErrOrchestratorClosed", err)
	}
}

func TestREQCLOSE001QueryAfterCloseReturnsErrOrchestratorClosed(t *testing.T) {
	orch := newTestOrchestrator(newMemoryPersistence(), Config{})
	if err := orch.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	ctx := context.Background()
	runID := uuid.New()
	taskRunID := uuid.New()
	checks := []struct {
		name string
		err  error
	}{
		{name: "GetDAGRun", err: errOnly(orch.GetDAGRun(ctx, runID))},
		{name: "GetTaskRun", err: errOnly(orch.GetTaskRun(ctx, taskRunID))},
		{name: "GetTaskEvents", err: errOnly(orch.GetTaskEvents(ctx, taskRunID))},
		{name: "GetTaskLogs", err: errOnly(orch.GetTaskLogs(ctx, taskRunID))},
		{name: "GetDAGRunLogs", err: errOnly(orch.GetDAGRunLogs(ctx, runID))},
		{name: "ListDAGRuns", err: errOnly(orch.ListDAGRuns(ctx, 10))},
		{name: "ListTaskRuns", err: errOnly(orch.ListTaskRuns(ctx, runID))},
	}
	for _, check := range checks {
		if !errors.Is(check.err, ErrOrchestratorClosed) {
			t.Fatalf("%s err=%v, want ErrOrchestratorClosed", check.name, check.err)
		}
	}
}

func TestREQTIMEOUT001TaskTimeoutRetriesAttempt(t *testing.T) {
	store := newMemoryPersistence()
	var calls int
	taskA := testRunTask("timeout", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		calls++
		if calls == 1 {
			<-ctx.Done()
			return state, ctx.Err()
		}
		return state, nil
	})
	taskA.Timeout = time.Millisecond
	taskA.Retry.MaxAttempts = 2
	if _, err := newTestOrchestrator(store, Config{GracePeriod: 50 * time.Millisecond}).Run(context.Background(), testRunDAG(taskA)); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if calls != 2 || store.tasks["timeout"].Attempt != 2 {
		t.Fatalf("calls=%d attempt=%d", calls, store.tasks["timeout"].Attempt)
	}
}

func TestREQTIMEOUT001TaskTimeoutExhaustionFailsTask(t *testing.T) {
	store := newMemoryPersistence()
	taskA := testRunTask("timeout", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		<-ctx.Done()
		return state, ctx.Err()
	})
	taskA.Timeout = time.Millisecond
	taskA.Retry.MaxAttempts = 1
	_, err := newTestOrchestrator(store, Config{GracePeriod: 50 * time.Millisecond}).Run(context.Background(), testRunDAG(taskA))
	if !errors.Is(err, context.DeadlineExceeded) || store.tasks["timeout"].Status != persistence.TaskRunStatusFailed {
		t.Fatalf("err=%v task=%#v", err, store.tasks["timeout"])
	}
}

func TestREQTIMEOUT001GlobalTimeoutFailsDAG(t *testing.T) {
	TestREQDAG005GlobalTimeoutTerminalFailedOnce(t)
}

func TestREQTASK004LateGoroutineCannotOverwriteTaskState(t *testing.T) {
	store := newMemoryPersistence()
	release := make(chan struct{})
	taskA := testRunTask("late", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		<-release
		return &RunState{TargetURL: "late-success"}, nil
	})
	taskA.Timeout = time.Millisecond
	_, _ = newTestOrchestrator(store, Config{GracePeriod: time.Millisecond}).Run(context.Background(), testRunDAG(taskA))
	close(release)
	time.Sleep(5 * time.Millisecond)
	if store.tasks["late"].Status != persistence.TaskRunStatusFailed || len(store.tasks["late"].RunStateSnapshot) != 0 {
		t.Fatalf("late task overwritten: %#v", store.tasks["late"])
	}
}

func TestREQGOR001GracePeriodStopsWaiting(t *testing.T) {
	store := newMemoryPersistence()
	taskA := testRunTask("late", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		select {}
	})
	taskA.Timeout = time.Millisecond
	start := time.Now()
	_, _ = newTestOrchestrator(store, Config{GracePeriod: 5 * time.Millisecond}).Run(context.Background(), testRunDAG(taskA))
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("orchestrator did not stop waiting after grace period")
	}
}

func TestREQGOR001LateSuccessSnapshotSuppressed(t *testing.T) {
	TestREQTASK004LateGoroutineCannotOverwriteTaskState(t)
}

func TestREQGOR001LateErrorSuppressed(t *testing.T) {
	store := newMemoryPersistence()
	release := make(chan struct{})
	taskA := testRunTask("late", 0, task.ExecutionModeParallel, nil, func(context.Context, *RunState) (*RunState, error) {
		<-release
		return nil, errors.New("late error")
	})
	taskA.Timeout = time.Millisecond
	_, _ = newTestOrchestrator(store, Config{GracePeriod: time.Millisecond}).Run(context.Background(), testRunDAG(taskA))
	close(release)
	time.Sleep(5 * time.Millisecond)
	if store.taskTerminalCounts["late"].Load() != 1 || store.tasks["late"].Status != persistence.TaskRunStatusFailed {
		t.Fatalf("late error overwrote task: count=%d task=%#v", store.taskTerminalCounts["late"].Load(), store.tasks["late"])
	}
}

func TestREQAGENT001ExecuteInvokesToolAndStoresResultInRunState(t *testing.T) {
	store := newMemoryPersistence()
	called := false
	fetch := task.Tool{Name: "fetch", Handler: func(context.Context, json.RawMessage) (json.RawMessage, error) {
		called = true
		return json.RawMessage(`"https://tool.example"`), nil
	}}
	taskA := testRunTask("agent", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		out, err := fetch.Handler(ctx, nil)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(out, &state.TargetURL); err != nil {
			return nil, err
		}
		return state, nil
	})
	taskA.Tools = task.ToolRegistry{"fetch": fetch}
	d := testRunDAG(taskA)
	if _, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !called || !json.Valid(store.tasks["agent"].RunStateSnapshot) || !containsJSON(string(store.tasks["agent"].RunStateSnapshot), "tool.example") {
		t.Fatalf("tool result not snapshotted: called=%v snapshot=%s", called, store.tasks["agent"].RunStateSnapshot)
	}
}

func TestREQAGENT001ToolFailureFailsAttempt(t *testing.T) {
	store := newMemoryPersistence()
	fetch := task.Tool{Name: "fetch", Handler: func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, errors.New("tool failed") }}
	taskA := testRunTask("agent", 0, task.ExecutionModeParallel, nil, func(ctx context.Context, state *RunState) (*RunState, error) {
		_, err := fetch.Handler(ctx, nil)
		return state, err
	})
	taskA.Tools = task.ToolRegistry{"fetch": fetch}
	d := testRunDAG(taskA)
	_, err := newTestOrchestrator(store, Config{}).Run(context.Background(), d)
	if err == nil || store.tasks["agent"].Status != persistence.TaskRunStatusFailed {
		t.Fatalf("expected failed tool attempt, err=%v task=%#v", err, store.tasks["agent"])
	}
}

func TestREQLOG001TaskLogsIncludeRequiredFields(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	store := &memoryLogStore{}
	runID := uuid.New()
	taskRunID := uuid.New()
	logger := newFanOutLogger(nil, store, &stdout, &stderr)
	logger.With(
		zap.String("dag_run_id", runID.String()),
		zap.String("dag_name", "pipeline"),
		zap.String("task_run_id", taskRunID.String()),
		zap.String("task_name", "agent"),
		zap.Int("attempt", 2),
	).Info("task log")
	if len(store.logs) != 1 {
		t.Fatalf("logs=%d, want 1 stderr=%s", len(store.logs), stderr.String())
	}
	var fields map[string]any
	if err := json.Unmarshal(store.logs[0].Fields, &fields); err != nil {
		t.Fatalf("unmarshal fields failed: %v", err)
	}
	if fields["dag_run_id"] != runID.String() || fields["dag_name"] != "pipeline" || fields["task_name"] != "agent" || fields["attempt"].(float64) != 2 {
		t.Fatalf("missing required fields: %v", fields)
	}
	if !findSubstring(stdout.String(), "dag_run_id") || !findSubstring(stdout.String(), "task_name") {
		t.Fatalf("stdout missing fields: %s", stdout.String())
	}
}

func TestREQLOG001DAGLogsIncludeRequiredFields(t *testing.T) {
	store := &memoryLogStore{}
	runID := uuid.New()
	newFanOutLogger(nil, store, &bytes.Buffer{}, &bytes.Buffer{}).With(zap.String("dag_run_id", runID.String()), zap.String("dag_name", "pipeline")).Info("dag log")
	if len(store.logs) != 1 || store.logs[0].TaskRunID != nil {
		t.Fatalf("logs=%d task_run_id=%v", len(store.logs), store.logs[0].TaskRunID)
	}
	var fields map[string]any
	if err := json.Unmarshal(store.logs[0].Fields, &fields); err != nil {
		t.Fatalf("unmarshal fields failed: %v", err)
	}
	if fields["dag_run_id"] != runID.String() || fields["dag_name"] != "pipeline" {
		t.Fatalf("missing required fields: %v", fields)
	}
}

func TestREQLOG001LogsRedactPostgresSecrets(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	store := &memoryLogStore{err: errors.New("insert failed postgres://user:pass@localhost/db password=secret")}
	logger := newFanOutLogger(nil, store, &stdout, &stderr).With(zap.String("dag_run_id", uuid.NewString()), zap.String("dag_name", "pipeline"))
	logger.Error("failed postgres://user:pass@localhost/db", zap.String("dsn", "postgres://user:pass@localhost/db"), zap.Error(errors.New("password=secret")))
	combined := stdout.String() + stderr.String()
	for _, forbidden := range []string{"postgres://user:pass@localhost/db", "user:pass", "password=secret"} {
		if findSubstring(combined, forbidden) {
			t.Fatalf("log leaked %q in %s", forbidden, combined)
		}
	}
}

func newTestOrchestrator(store runPersistence[RunState], config Config) *Orchestrator[RunState] {
	return newOrchestratorWithPersistence(config, nil, store)
}

func testRunDAG(tasks ...*task.Task[RunState]) *dagpkg.DAG[RunState] {
	d := &dagpkg.DAG[RunState]{Name: "pipeline", Tasks: map[string]*task.Task[RunState]{}, TaskOrder: []string{}}
	for _, taskDef := range tasks {
		d.Tasks[taskDef.Name] = taskDef
		d.TaskOrder = append(d.TaskOrder, taskDef.Name)
	}
	return d
}

func testRunTask(name string, priority int, mode task.ExecutionMode, dependsOn []string, execute task.ExecuteFunc[RunState]) *task.Task[RunState] {
	return &task.Task[RunState]{Name: name, Priority: priority, Mode: mode, DependsOn: dependsOn, Execute: execute, FunctionName: "test." + name}
}

func noopExecute(context.Context, *RunState) (*RunState, error) {
	return &RunState{}, nil
}

func updateMaxInt32(max *atomic.Int32, value int32) {
	for {
		current := max.Load()
		if value <= current || max.CompareAndSwap(current, value) {
			return
		}
	}
}

func containsJSON(input, want string) bool {
	return json.Valid([]byte(input)) && findSubstring(input, want)
}

func findSubstring(input, want string) bool {
	for i := 0; i+len(want) <= len(input); i++ {
		if input[i:i+len(want)] == want {
			return true
		}
	}
	return false
}

func errOnly[T any](_ T, err error) error {
	return err
}

type memoryPersistence struct {
	mu                 sync.Mutex
	tasks              map[string]persistence.TaskRun
	taskTerminalCounts map[string]*atomic.Int32
	runStatus          persistence.DAGRunStatus
	runTerminalCount   atomic.Int32
	runningAttempts    atomic.Int32
	reads              atomic.Int32
	afterTaskSuccess   func(string)
	events             map[string][]persistence.TaskEvent
}

func newMemoryPersistence() *memoryPersistence {
	return &memoryPersistence{tasks: map[string]persistence.TaskRun{}, taskTerminalCounts: map[string]*atomic.Int32{}, events: map[string][]persistence.TaskEvent{}}
}

func (m *memoryPersistence) CreateRun(context.Context, *dagpkg.DAG[RunState], json.RawMessage) (*persistence.DAGRun, error) {
	return &persistence.DAGRun{ID: uuid.New(), Status: persistence.DAGRunStatusRunning}, nil
}

func (m *memoryPersistence) CreateTaskRuns(_ context.Context, runID uuid.UUID, d *dagpkg.DAG[RunState]) (map[string]persistence.TaskRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := make(map[string]persistence.TaskRun, len(d.TaskOrder))
	for i, name := range d.TaskOrder {
		row := persistence.TaskRun{ID: uuid.New(), DAGRunID: runID, TaskName: name, Status: persistence.TaskRunStatusPending, OrderIndex: i}
		rows[name] = row
		m.tasks[name] = row
		m.taskTerminalCounts[name] = &atomic.Int32{}
	}
	return rows, nil
}

func (m *memoryPersistence) GetRun(context.Context, uuid.UUID) (*persistence.DAGRun, error) {
	m.reads.Add(1)
	return &persistence.DAGRun{ID: uuid.New(), DAGName: "pipeline", Status: persistence.DAGRunStatusRunning, GlobalInputs: json.RawMessage(`{}`)}, nil
}

func (m *memoryPersistence) LoadTaskRunsForResume(context.Context, uuid.UUID) (map[string]persistence.TaskRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads.Add(1)
	rows := make(map[string]persistence.TaskRun, len(m.tasks))
	maps.Copy(rows, m.tasks)
	return rows, nil
}

func (m *memoryPersistence) MarkTaskRunning(_ context.Context, id uuid.UUID, attempt int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runningAttempts.Add(1)
	name, row, ok := m.taskByID(id)
	if !ok {
		return fmt.Errorf("missing task")
	}
	row.Status = persistence.TaskRunStatusRunning
	row.Attempt = attempt
	m.tasks[name] = row
	return nil
}

func (m *memoryPersistence) MarkTaskSuccess(_ context.Context, id uuid.UUID, snapshot json.RawMessage, attempt int) error {
	m.mu.Lock()
	name, row, ok := m.taskByID(id)
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("missing task")
	}
	row.Status = persistence.TaskRunStatusSuccess
	row.Attempt = attempt
	row.RunStateSnapshot = append(json.RawMessage(nil), snapshot...)
	m.tasks[name] = row
	m.taskTerminalCounts[name].Add(1)
	m.events[name] = append(m.events[name], persistence.TaskEvent{ID: uuid.New(), TaskRunID: id, EventType: persistence.TaskEventSucceeded, Attempt: attempt})
	after := m.afterTaskSuccess
	m.mu.Unlock()
	if after != nil {
		after(name)
	}
	return nil
}

func (m *memoryPersistence) RecordTaskEvent(_ context.Context, id uuid.UUID, eventType persistence.TaskEventType, attempt int, message *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name, _, ok := m.taskByID(id)
	if !ok {
		return fmt.Errorf("missing task")
	}
	m.events[name] = append(m.events[name], persistence.TaskEvent{ID: uuid.New(), TaskRunID: id, EventType: eventType, Attempt: attempt, ErrorMessage: message})
	return nil
}

func (m *memoryPersistence) MarkTaskFailed(_ context.Context, id uuid.UUID, attempt int, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name, row, ok := m.taskByID(id)
	if !ok {
		return fmt.Errorf("missing task")
	}
	row.Status = persistence.TaskRunStatusFailed
	row.Attempt = attempt
	row.ErrorMessage = &message
	row.RunStateSnapshot = nil
	m.tasks[name] = row
	m.taskTerminalCounts[name].Add(1)
	m.events[name] = append(m.events[name], persistence.TaskEvent{ID: uuid.New(), TaskRunID: id, EventType: persistence.TaskEventFailed, Attempt: attempt, ErrorMessage: &message})
	return nil
}

func (m *memoryPersistence) MarkTaskCancelled(_ context.Context, id uuid.UUID, attempt int, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name, row, ok := m.taskByID(id)
	if !ok {
		return fmt.Errorf("missing task")
	}
	row.Status = persistence.TaskRunStatusCancelled
	row.Attempt = attempt
	row.ErrorMessage = &message
	row.RunStateSnapshot = nil
	m.tasks[name] = row
	m.taskTerminalCounts[name].Add(1)
	m.events[name] = append(m.events[name], persistence.TaskEvent{ID: uuid.New(), TaskRunID: id, EventType: persistence.TaskEventCancelled, Attempt: attempt, ErrorMessage: &message})
	return nil
}

func (m *memoryPersistence) MarkTaskSkipped(_ context.Context, id uuid.UUID, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name, row, ok := m.taskByID(id)
	if !ok {
		return fmt.Errorf("missing task")
	}
	row.Status = persistence.TaskRunStatusSkipped
	row.Attempt = 0
	row.ErrorMessage = &message
	m.tasks[name] = row
	m.taskTerminalCounts[name].Add(1)
	return nil
}

func (m *memoryPersistence) MarkRunTerminal(_ context.Context, _ uuid.UUID, status persistence.DAGRunStatus, _ *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runStatus = status
	m.runTerminalCount.Add(1)
	return nil
}

func (m *memoryPersistence) taskByID(id uuid.UUID) (string, persistence.TaskRun, bool) {
	for name, row := range m.tasks {
		if row.ID == id {
			return name, row, true
		}
	}
	return "", persistence.TaskRun{}, false
}
