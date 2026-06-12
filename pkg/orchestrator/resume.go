package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	dagpkg "github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/persistence"
)

type resumePlan[S any] struct {
	taskRuns      map[string]persistence.TaskRun
	initialStatus map[string]persistence.TaskRunStatus
	state         *S
}

func (o *Orchestrator[S]) Resume(ctx context.Context, d *dagpkg.DAG[S], runID uuid.UUID) (run *DAGRun, err error) {
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
	var pool *pgxpool.Pool
	if o.postgres != nil {
		pool = o.postgres.Pool
	}
	lock, err := acquireRunAdvisoryLock(ctx, pool, runID)
	if err != nil {
		o.startMu.Unlock()
		return nil, err
	}
	defer func() {
		err = joinWithReleaseError(err, lock.release(context.Background()))
	}()

	run, plan, err := o.prepareResume(ctx, runtimeDAG, runID)
	if err != nil {
		o.startMu.Unlock()
		return run, err
	}
	runCtx, cancel := runContext(ctx, o.config.GlobalTimeout)
	if err := o.registerRun(runID, cancel); err != nil {
		cancel()
		o.startMu.Unlock()
		return run, err
	}
	o.startMu.Unlock()
	defer func() {
		cancel()
		o.unregisterRun(runID, err)
	}()

	err = o.executeRunWithInitialStatus(runCtx, cancel, runtimeDAG, runID, plan.taskRuns, plan.state, plan.initialStatus)
	if err != nil {
		message := err.Error()
		status := persistence.DAGRunStatusFailed
		if errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			status = persistence.DAGRunStatusCancelled
		}
		if terminalErr := o.markRunTerminal(context.Background(), runID, status, &message); terminalErr != nil && !errors.Is(terminalErr, ErrRunTerminal) {
			return run, fmt.Errorf("%w; additionally failed to persist run terminal state: %v", err, terminalErr)
		}
		return run, err
	}
	if err := o.markRunTerminal(ctx, runID, persistence.DAGRunStatusSuccess, nil); err != nil {
		return run, err
	}
	return run, nil
}

func (o *Orchestrator[S]) prepareResume(ctx context.Context, d *dagpkg.DAG[S], runID uuid.UUID) (*persistence.DAGRun, resumePlan[S], error) {
	run, err := o.persistence.GetRun(ctx, runID)
	if err != nil {
		return nil, resumePlan[S]{}, err
	}
	if run.Status != persistence.DAGRunStatusRunning {
		return run, resumePlan[S]{}, fmt.Errorf("%w: run %s has status %s", ErrRunTerminal, runID, run.Status)
	}
	if run.DAGName != d.Name {
		return run, resumePlan[S]{}, fmt.Errorf("%w: dag name mismatch for run %s", ErrValidation, runID)
	}
	persistedVersion := ""
	if run.DAGVersion != nil {
		persistedVersion = *run.DAGVersion
	}
	if persistedVersion != d.Version {
		return run, resumePlan[S]{}, fmt.Errorf("%w: dag version mismatch for run %s", ErrValidation, runID)
	}

	taskRuns, err := o.persistence.LoadTaskRunsForResume(ctx, runID)
	if err != nil {
		return run, resumePlan[S]{}, err
	}
	if err := reconcileResumeTaskRows(d, taskRuns); err != nil {
		return run, resumePlan[S]{}, err
	}
	state, err := hydrateResumeRunState[S](run, d.TaskOrder, taskRuns)
	if err != nil {
		return run, resumePlan[S]{}, err
	}
	initialStatus := make(map[string]persistence.TaskRunStatus, len(d.TaskOrder))
	for _, name := range d.TaskOrder {
		if taskRuns[name].Status == persistence.TaskRunStatusSuccess {
			initialStatus[name] = persistence.TaskRunStatusSuccess
		}
	}
	return run, resumePlan[S]{taskRuns: taskRuns, initialStatus: initialStatus, state: state}, nil
}

func reconcileResumeTaskRows[S any](d *dagpkg.DAG[S], taskRuns map[string]persistence.TaskRun) error {
	if len(taskRuns) != len(d.TaskOrder) {
		return fmt.Errorf("%w: task row count mismatch", ErrValidation)
	}
	for _, name := range d.TaskOrder {
		if _, ok := taskRuns[name]; !ok {
			return fmt.Errorf("%w: persisted run is missing task %q", ErrValidation, name)
		}
	}
	return nil
}

func hydrateResumeRunState[S any](run *persistence.DAGRun, order []string, taskRuns map[string]persistence.TaskRun) (*S, error) {
	latestIndex := -1
	var latestState *S
	for _, name := range order {
		row := taskRuns[name]
		if row.Status != persistence.TaskRunStatusSuccess {
			continue
		}
		if len(row.RunStateSnapshot) == 0 {
			return nil, fmt.Errorf("%w: successful task %q is missing RunState snapshot", ErrPersistence, row.TaskName)
		}
		state, err := decodeRunStateSnapshot[S](row.RunStateSnapshot, fmt.Sprintf("task %q snapshot", row.TaskName))
		if err != nil {
			return nil, err
		}
		if row.OrderIndex > latestIndex {
			latestIndex = row.OrderIndex
			latestState = state
		}
	}
	if latestIndex >= 0 {
		return latestState, nil
	}
	return decodeRunStateSnapshot[S](run.GlobalInputs, "dag global_inputs")
}

func decodeRunStateSnapshot[S any](raw json.RawMessage, source string) (*S, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var state S
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("%w: invalid %s: %v", ErrPersistence, source, err)
	}
	return &state, nil
}
