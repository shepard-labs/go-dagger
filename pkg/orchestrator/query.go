package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/shepard-labs/go-dagger/pkg/persistence"
)

// TaskRun is the public persisted task execution record.
type TaskRun = persistence.TaskRun

// TaskEvent is an append-only task lifecycle event.
type TaskEvent = persistence.TaskEvent

// TaskLog is a persisted log record for a DAG run or task run.
type TaskLog = persistence.TaskLog

type dagQueryStore interface {
	Get(context.Context, uuid.UUID) (*DAGRun, error)
	List(context.Context, int) ([]DAGRun, error)
}

type taskQueryStore interface {
	Get(context.Context, uuid.UUID) (*TaskRun, error)
	ListByRun(context.Context, uuid.UUID) ([]TaskRun, error)
}

type eventQueryStore interface {
	ListByTaskRun(context.Context, uuid.UUID) ([]TaskEvent, error)
}

type logQueryStore interface {
	ListByTaskRun(context.Context, uuid.UUID) ([]TaskLog, error)
	ListByDAGRun(context.Context, uuid.UUID) ([]TaskLog, error)
}

// GetDAGRun fetches one DAG run by ID.
func (o *Orchestrator[S]) GetDAGRun(ctx context.Context, runID uuid.UUID) (*DAGRun, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.dagQueries.Get(ctx, runID)
}

// GetTaskRun fetches one task run by ID.
func (o *Orchestrator[S]) GetTaskRun(ctx context.Context, taskRunID uuid.UUID) (*TaskRun, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.taskQueries.Get(ctx, taskRunID)
}

// GetTaskEvents lists events for a task run in creation order.
func (o *Orchestrator[S]) GetTaskEvents(ctx context.Context, taskRunID uuid.UUID) ([]TaskEvent, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.eventQueries.ListByTaskRun(ctx, taskRunID)
}

// GetTaskLogs lists logs scoped to a task run in creation order.
func (o *Orchestrator[S]) GetTaskLogs(ctx context.Context, taskRunID uuid.UUID) ([]TaskLog, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.logQueries.ListByTaskRun(ctx, taskRunID)
}

// GetDAGRunLogs lists all logs recorded for a DAG run.
func (o *Orchestrator[S]) GetDAGRunLogs(ctx context.Context, runID uuid.UUID) ([]TaskLog, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.logQueries.ListByDAGRun(ctx, runID)
}

// ListDAGRuns returns recent DAG runs, capped by the persistence layer.
func (o *Orchestrator[S]) ListDAGRuns(ctx context.Context, limit int) ([]DAGRun, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.dagQueries.List(ctx, limit)
}

// ListTaskRuns returns task runs for a DAG run in DAG order.
func (o *Orchestrator[S]) ListTaskRuns(ctx context.Context, runID uuid.UUID) ([]TaskRun, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.taskQueries.ListByRun(ctx, runID)
}

func (o *Orchestrator[S]) beginQuery() (func(), error) {
	if o == nil {
		return nil, fmt.Errorf("%w: orchestrator is nil", ErrOrchestratorClosed)
	}
	o.startMu.Lock()
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	if closed {
		o.startMu.Unlock()
		return nil, ErrOrchestratorClosed
	}
	if o.dagQueries == nil || o.taskQueries == nil || o.eventQueries == nil || o.logQueries == nil {
		o.startMu.Unlock()
		return nil, fmt.Errorf("%w: postgres is not configured", ErrPersistence)
	}
	return o.startMu.Unlock, nil
}
