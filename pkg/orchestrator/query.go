package orchestrator

import (
	"context"
	"fmt"

	"github.com/shepard-labs/go-dagger/pkg/persistence"
	"github.com/google/uuid"
)

type TaskRun = persistence.TaskRun
type TaskEvent = persistence.TaskEvent
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

func (o *Orchestrator[S]) GetDAGRun(ctx context.Context, runID uuid.UUID) (*DAGRun, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.dagQueries.Get(ctx, runID)
}

func (o *Orchestrator[S]) GetTaskRun(ctx context.Context, taskRunID uuid.UUID) (*TaskRun, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.taskQueries.Get(ctx, taskRunID)
}

func (o *Orchestrator[S]) GetTaskEvents(ctx context.Context, taskRunID uuid.UUID) ([]TaskEvent, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.eventQueries.ListByTaskRun(ctx, taskRunID)
}

func (o *Orchestrator[S]) GetTaskLogs(ctx context.Context, taskRunID uuid.UUID) ([]TaskLog, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.logQueries.ListByTaskRun(ctx, taskRunID)
}

func (o *Orchestrator[S]) GetDAGRunLogs(ctx context.Context, runID uuid.UUID) ([]TaskLog, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.logQueries.ListByDAGRun(ctx, runID)
}

func (o *Orchestrator[S]) ListDAGRuns(ctx context.Context, limit int) ([]DAGRun, error) {
	done, err := o.beginQuery()
	if err != nil {
		return nil, err
	}
	defer done()
	return o.dagQueries.List(ctx, limit)
}

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
