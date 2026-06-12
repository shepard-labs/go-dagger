// Package orchestrator validates DAGs and coordinates persisted task execution.
package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
	dagpkg "github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/persistence"
	"github.com/shepard-labs/go-dagger/pkg/task"
	"go.uber.org/zap"
)

// GlobalInputs is the typed wrapper callers pass to Run to seed the run
// with an initial value of the DAG's state type S.
//
// Usage:
//
//	orch.Run(ctx, d)                                                  // zero-value S
//	orch.Run(ctx, d, GlobalInputs[RunState]{Value: RunState{...}})    // seeded S
//
// G must equal S (the type parameter of the Orchestrator). Passing
// multiple GlobalInputs, or a G that does not match S, returns
// ErrValidation.
type GlobalInputs[G any] struct {
	Value G
}

// Config controls persistence, scheduling, lifecycle, and logging behavior.
type Config struct {
	PostgresDSN        string
	PostgresPoolSize   int32
	PersistenceTimeout time.Duration
	PersistenceRetries int
	ConcurrencyLimit   int
	GlobalTimeout      time.Duration
	GracePeriod        time.Duration
	Logger             *zap.Logger
}

// DAGRun is the public run record returned by the orchestrator.
type DAGRun = persistence.DAGRun

// Orchestrator validates, runs, resumes, cancels, and queries DAG executions.
type Orchestrator[S any] struct {
	postgres     *persistence.Postgres
	persistence  runPersistence[S]
	dagQueries   dagQueryStore
	taskQueries  taskQueryStore
	eventQueries eventQueryStore
	logQueries   logQueryStore
	logger       *zap.Logger
	config       Config

	startMu    sync.Mutex
	closeMu    sync.Mutex
	mu         sync.Mutex
	activeRuns map[uuid.UUID]*activeRun
	closed     bool
	closeDone  bool
	closeErr   error
}

type activeRun struct {
	cancel   context.CancelFunc
	terminal bool
	done     chan struct{}
	err      error
}

// NewOrchestrator opens Postgres-backed persistence and returns an orchestrator.
func NewOrchestrator[S any](ctx context.Context, config Config) (*Orchestrator[S], error) {
	config = normalizeOrchestratorConfig(config)
	postgres, err := persistence.NewPostgres(ctx, persistence.Config{
		DSN:                config.PostgresDSN,
		PoolSize:           config.PostgresPoolSize,
		PersistenceTimeout: config.PersistenceTimeout,
		WriteRetries:       config.PersistenceRetries,
	})
	if err != nil {
		return nil, err
	}
	return newOrchestratorWithPersistence(config, postgres, newPostgresPersistence[S](postgres)), nil
}

// Close cancels active runs, waits up to the grace period, and closes persistence.
func (o *Orchestrator[S]) Close() error {
	if o == nil {
		return nil
	}
	o.closeMu.Lock()
	defer o.closeMu.Unlock()
	if o.closeDone {
		return o.closeErr
	}

	o.startMu.Lock()
	o.mu.Lock()
	o.closed = true
	active := make([]*activeRun, 0, len(o.activeRuns))
	for _, run := range o.activeRuns {
		active = append(active, run)
		run.cancel()
	}
	o.mu.Unlock()
	o.startMu.Unlock()

	var closeErr error
	if len(active) > 0 {
		waitCtx, cancel := context.WithTimeout(context.Background(), o.config.GracePeriod)
		defer cancel()
		for _, run := range active {
			select {
			case <-run.done:
				if closeErr == nil && isCloseFailure(run.err) {
					closeErr = run.err
				}
			case <-waitCtx.Done():
				closeErr = fmt.Errorf("%w: close grace period elapsed", waitCtx.Err())
				goto closePool
			}
		}
	}

closePool:
	if o.postgres != nil {
		o.postgres.Close()
	}
	o.closeErr = closeErr
	o.closeDone = true
	return o.closeErr
}

func isCloseFailure(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrRunTerminal)
}

// DryRun validates a DAG without creating a persisted run.
func (o *Orchestrator[S]) DryRun(d *dagpkg.DAG[S]) error {
	runtimeDAG := cloneDAG(d)
	return runtimeDAG.Validate()
}

// Run is the single entry point for starting a DAG run. It is variadic on
// the optional GlobalInputs value, so callers can pass a typed seed or omit
// it for the zero-value default behavior.
//
//	orch.Run(ctx, d)                                               // zero-value S
//	orch.Run(ctx, d, GlobalInputs[RunState]{Value: RunState{...}}) // typed seed
//
// GlobalInputs is a generic wrapper for documentation and IDE support; the
// type parameter is inferred at the call site from the argument. G must
// equal S; mismatches and more than one GlobalInputs argument return
// ErrValidation. Passing no GlobalInputs preserves the original
// zero-value-S behavior byte-for-byte.
func (o *Orchestrator[S]) Run(ctx context.Context, d *dagpkg.DAG[S], inputs ...GlobalInputs[S]) (*DAGRun, error) {
	if o == nil {
		return nil, fmt.Errorf("%w: orchestrator is nil", ErrOrchestratorClosed)
	}
	o.startMu.Lock()
	if o.isClosed() {
		o.startMu.Unlock()
		return nil, ErrOrchestratorClosed
	}
	runtimeDAG := cloneDAG(d)
	if err := runtimeDAG.Validate(); err != nil {
		o.startMu.Unlock()
		return nil, err
	}
	globalInputs, currentState, err := buildInitialState[S](inputs)
	if err != nil {
		o.startMu.Unlock()
		return nil, err
	}
	run, err := o.persistence.CreateRun(ctx, runtimeDAG, globalInputs)
	if err != nil {
		o.startMu.Unlock()
		return nil, err
	}
	runCtx, cancel := runContext(ctx, o.config.GlobalTimeout)
	if err := o.registerRun(run.ID, cancel); err != nil {
		cancel()
		o.startMu.Unlock()
		return run, err
	}
	o.startMu.Unlock()
	defer cancel()

	taskRuns, err := o.persistence.CreateTaskRuns(runCtx, run.ID, runtimeDAG)
	if err != nil {
		message := err.Error()
		_ = o.markRunTerminal(context.Background(), run.ID, persistence.DAGRunStatusFailed, &message)
		o.unregisterRun(run.ID, err)
		return run, err
	}
	err = o.executeRun(runCtx, cancel, runtimeDAG, run.ID, taskRuns, currentState)
	if err != nil {
		message := err.Error()
		status := persistence.DAGRunStatusFailed
		if errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			status = persistence.DAGRunStatusCancelled
		}
		if terminalErr := o.markRunTerminal(context.Background(), run.ID, status, &message); terminalErr != nil && !errors.Is(terminalErr, ErrRunTerminal) {
			combinedErr := fmt.Errorf("%w; additionally failed to persist run terminal state: %v", err, terminalErr)
			o.unregisterRun(run.ID, combinedErr)
			return run, combinedErr
		}
		o.unregisterRun(run.ID, err)
		return run, err
	}
	if err := o.markRunTerminal(ctx, run.ID, persistence.DAGRunStatusSuccess, nil); err != nil {
		o.unregisterRun(run.ID, err)
		return run, err
	}
	o.unregisterRun(run.ID, nil)
	return run, nil
}

func (o *Orchestrator[S]) executeRun(ctx context.Context, cancel context.CancelFunc, d *dagpkg.DAG[S], runID uuid.UUID, taskRuns map[string]persistence.TaskRun, currentState *S) error {
	return o.executeRunWithInitialStatus(ctx, cancel, d, runID, taskRuns, currentState, nil)
}

// buildInitialState turns the optional GlobalInputs slice handed to Run
// into the (persisted GlobalInputs JSON, in-memory initial *S) pair the
// scheduler consumes.
//
// Zero arguments -> zero-value S encoded as `{}` (current behavior).
// One argument  -> the typed Value, assigned to a fresh S via type
//
//	assertion; ErrValidation if Value's dynamic type is
//	not assignable to S.
//
// >1 arguments  -> ErrValidation; we refuse to silently "last one wins".
func buildInitialState[S any](inputs []GlobalInputs[S]) (json.RawMessage, *S, error) {
	if len(inputs) == 0 {
		return snapshotRunState(new(S))
	}
	if len(inputs) > 1 {
		return nil, nil, fmt.Errorf("%w: at most one GlobalInputs may be passed to Run", ErrValidation)
	}
	typed, ok := any(inputs[0].Value).(S)
	if !ok {
		return nil, nil, fmt.Errorf("%w: GlobalInputs type does not match DAG state type S", ErrValidation)
	}
	return snapshotRunState(&typed)
}

func (o *Orchestrator[S]) executeRunWithInitialStatus(ctx context.Context, cancel context.CancelFunc, d *dagpkg.DAG[S], runID uuid.UUID, taskRuns map[string]persistence.TaskRun, currentState *S, initialStatus map[string]persistence.TaskRunStatus) error {
	adjacency := copyAdjacency(d.Adjacency, d.TaskOrder)
	inDegree := copyInDegree(d.InDegree, d.TaskOrder)
	limit := effectiveConcurrency(d.ConcurrencyLimit, o.config.ConcurrencyLimit)
	ready := newReadyQueue(d.TaskOrder, d.Tasks)
	status := make(map[string]persistence.TaskRunStatus, len(d.TaskOrder))
	completed := 0
	for _, name := range d.TaskOrder {
		rowStatus := persistence.TaskRunStatusPending
		if initialStatus != nil {
			if initial, ok := initialStatus[name]; ok {
				rowStatus = initial
			}
		}
		if rowStatus == persistence.TaskRunStatusSuccess {
			status[name] = persistence.TaskRunStatusSuccess
			completed++
			for _, dependent := range adjacency[name] {
				inDegree[dependent]--
			}
			continue
		}
		status[name] = persistence.TaskRunStatusPending
	}
	for _, name := range d.TaskOrder {
		if status[name] == persistence.TaskRunStatusPending && inDegree[name] == 0 {
			ready.push(name)
		}
	}

	results := make(chan taskResult[S], len(d.TaskOrder))
	active := 0
	sequentialRunning := false
	stopScheduling := false
	var runErr error

	for completed < len(d.TaskOrder) {
		for !stopScheduling && active < limit {
			name, ok := ready.popRunnable(sequentialRunning)
			if !ok {
				break
			}
			status[name] = persistence.TaskRunStatusRunning
			active++
			if d.Tasks[name].Mode == task.ExecutionModeSequential {
				sequentialRunning = true
			}
			input := currentState
			go func(name string, taskDef *task.Task[S]) {
				results <- o.executeTaskWithRetries(ctx, d.Name, taskDef, taskRuns[name], input)
			}(name, d.Tasks[name])
		}

		if active == 0 {
			if stopScheduling {
				break
			}
			if len(ready.names) == 0 {
				break
			}
			continue
		}

		var result taskResult[S]
		if stopScheduling {
			result = <-results
		} else {
			select {
			case result = <-results:
			case <-ctx.Done():
				if runErr == nil {
					runErr = classifyRunContextError(ctx.Err())
				}
				stopScheduling = true
				cancel()
				completed += o.cancelPendingTasks(context.Background(), d.TaskOrder, taskRuns, status, runErr.Error())
				continue
			}
		}
		active--
		completed++
		if d.Tasks[result.name].Mode == task.ExecutionModeSequential {
			sequentialRunning = false
		}

		if result.status == persistence.TaskRunStatusFailed || result.err != nil && result.status != persistence.TaskRunStatusSuccess {
			if runErr == nil {
				if result.status == persistence.TaskRunStatusCancelled && errors.Is(result.err, context.Canceled) {
					runErr = result.err
				} else {
					runErr = &TaskFailureError{TaskName: result.name, Attempt: result.attempt, Err: result.err}
				}
			}
			status[result.name] = result.status
			stopScheduling = true
			cancel()
			if result.status == persistence.TaskRunStatusCancelled {
				completed += o.cancelPendingTasks(context.Background(), d.TaskOrder, taskRuns, status, result.err.Error())
			} else {
				completed += o.skipBlockedTasks(context.Background(), adjacency, taskRuns, status, result.name, runErr.Error())
			}
			continue
		}

		status[result.name] = persistence.TaskRunStatusSuccess
		if !stopScheduling {
			currentState = result.state
			for _, dependent := range adjacency[result.name] {
				inDegree[dependent]--
				if inDegree[dependent] == 0 && status[dependent] == persistence.TaskRunStatusPending {
					ready.push(dependent)
				}
			}
		}
	}

	if runErr != nil {
		return runErr
	}
	if completed != len(d.TaskOrder) {
		return fmt.Errorf("%w: scheduler stopped before all tasks completed for run %s", ErrPersistence, runID)
	}
	return nil
}

func (o *Orchestrator[S]) cancelPendingTasks(ctx context.Context, order []string, taskRuns map[string]persistence.TaskRun, status map[string]persistence.TaskRunStatus, reason string) int {
	cancelled := 0
	for _, name := range order {
		row := taskRuns[name]
		if status[name] != persistence.TaskRunStatusPending {
			continue
		}
		if err := o.persistence.MarkTaskCancelled(ctx, row.ID, 0, reason); err == nil {
			status[name] = persistence.TaskRunStatusCancelled
			cancelled++
		}
	}
	return cancelled
}

func (o *Orchestrator[S]) skipBlockedTasks(ctx context.Context, adjacency map[string][]string, taskRuns map[string]persistence.TaskRun, status map[string]persistence.TaskRunStatus, failedTask, reason string) int {
	queue := append([]string(nil), adjacency[failedTask]...)
	skipped := 0
	seen := map[string]struct{}{}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if status[name] == persistence.TaskRunStatusPending {
			if err := o.persistence.MarkTaskSkipped(ctx, taskRuns[name].ID, reason); err == nil {
				status[name] = persistence.TaskRunStatusSkipped
				skipped++
			}
		}
		queue = append(queue, adjacency[name]...)
	}
	return skipped
}

func copyAdjacency(source map[string][]string, order []string) map[string][]string {
	copied := make(map[string][]string, len(order))
	for _, name := range order {
		copied[name] = append([]string(nil), source[name]...)
	}
	return copied
}

func copyInDegree(source map[string]int, order []string) map[string]int {
	copied := make(map[string]int, len(order))
	for _, name := range order {
		copied[name] = source[name]
	}
	return copied
}

func cloneDAG[S any](source *dagpkg.DAG[S]) *dagpkg.DAG[S] {
	if source == nil {
		return nil
	}
	copied := &dagpkg.DAG[S]{
		Name:             source.Name,
		Version:          source.Version,
		ConcurrencyLimit: source.ConcurrencyLimit,
		Timeout:          source.Timeout,
		Tasks:            make(map[string]*task.Task[S], len(source.Tasks)),
		TaskOrder:        append([]string(nil), source.TaskOrder...),
		Adjacency:        copyStringSlices(source.Adjacency),
		InDegree:         make(map[string]int, len(source.InDegree)),
	}
	maps.Copy(copied.InDegree, source.InDegree)
	for name, taskDef := range source.Tasks {
		if taskDef == nil {
			copied.Tasks[name] = nil
			continue
		}
		taskCopy := *taskDef
		taskCopy.Tags = copyStringMap(taskDef.Tags)
		taskCopy.DependsOn = append([]string(nil), taskDef.DependsOn...)
		taskCopy.ToolNames = append([]string(nil), taskDef.ToolNames...)
		taskCopy.BeforeHookNames = append([]string(nil), taskDef.BeforeHookNames...)
		taskCopy.AfterHookNames = append([]string(nil), taskDef.AfterHookNames...)
		taskCopy.BeforeHooks = append([]task.BeforeHook[S](nil), taskDef.BeforeHooks...)
		taskCopy.AfterHooks = append([]task.AfterHook[S](nil), taskDef.AfterHooks...)
		taskCopy.Tools = make(task.ToolRegistry, len(taskDef.Tools))
		maps.Copy(taskCopy.Tools, taskDef.Tools)
		copied.Tasks[name] = &taskCopy
	}
	return copied
}

func copyStringSlices(source map[string][]string) map[string][]string {
	if source == nil {
		return nil
	}
	copied := make(map[string][]string, len(source))
	for key, values := range source {
		copied[key] = append([]string(nil), values...)
	}
	return copied
}

func copyStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	copied := make(map[string]string, len(source))
	maps.Copy(copied, source)
	return copied
}

func normalizeOrchestratorConfig(config Config) Config {
	if config.PersistenceTimeout == 0 {
		config.PersistenceTimeout = 10 * time.Second
	}
	if config.PersistenceRetries == 0 {
		config.PersistenceRetries = 3
	}
	if config.GracePeriod == 0 {
		config.GracePeriod = 30 * time.Second
	}
	if config.Logger == nil {
		config.Logger = zap.NewNop()
	}
	return config
}

func newOrchestratorWithPersistence[S any](config Config, postgres *persistence.Postgres, store runPersistence[S]) *Orchestrator[S] {
	config = normalizeOrchestratorConfig(config)
	if postgres != nil && postgres.Pool != nil {
		config.Logger = newFanOutLogger(config.Logger, persistence.NewLogStore(postgres.Pool), nil, nil)
	}
	o := &Orchestrator[S]{postgres: postgres, persistence: store, logger: config.Logger, config: config, activeRuns: map[uuid.UUID]*activeRun{}}
	if postgres != nil && postgres.Pool != nil {
		o.dagQueries = persistence.NewDAGStore(postgres.Pool)
		o.taskQueries = persistence.NewTaskStore[S](postgres.Pool)
		o.eventQueries = persistence.NewEventStore(postgres.Pool)
		o.logQueries = persistence.NewLogStore(postgres.Pool)
	}
	return o
}

func (o *Orchestrator[S]) isClosed() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.closed
}

func (o *Orchestrator[S]) registerRun(id uuid.UUID, cancel context.CancelFunc) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrOrchestratorClosed
	}
	o.activeRuns[id] = &activeRun{cancel: cancel, done: make(chan struct{})}
	return nil
}

func (o *Orchestrator[S]) markRunTerminal(ctx context.Context, id uuid.UUID, status persistence.DAGRunStatus, message *string) error {
	if err := o.persistence.MarkRunTerminal(ctx, id, status, message); err != nil {
		return err
	}
	o.mu.Lock()
	if run := o.activeRuns[id]; run != nil {
		run.terminal = true
	}
	o.mu.Unlock()
	return nil
}

func (o *Orchestrator[S]) unregisterRun(id uuid.UUID, runErr error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if run := o.activeRuns[id]; run != nil {
		run.err = runErr
		close(run.done)
		delete(o.activeRuns, id)
	}
}
