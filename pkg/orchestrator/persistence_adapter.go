package orchestrator

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	dagpkg "github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/persistence"
)

type runPersistence[S any] interface {
	CreateRun(context.Context, *dagpkg.DAG[S], json.RawMessage) (*persistence.DAGRun, error)
	CreateTaskRuns(context.Context, uuid.UUID, *dagpkg.DAG[S]) (map[string]persistence.TaskRun, error)
	GetRun(context.Context, uuid.UUID) (*persistence.DAGRun, error)
	LoadTaskRunsForResume(context.Context, uuid.UUID) (map[string]persistence.TaskRun, error)
	MarkTaskRunning(context.Context, uuid.UUID, int) error
	RecordTaskEvent(context.Context, uuid.UUID, persistence.TaskEventType, int, *string) error
	MarkTaskSuccess(context.Context, uuid.UUID, json.RawMessage, int) error
	MarkTaskFailed(context.Context, uuid.UUID, int, string) error
	MarkTaskCancelled(context.Context, uuid.UUID, int, string) error
	MarkTaskSkipped(context.Context, uuid.UUID, string) error
	MarkRunTerminal(context.Context, uuid.UUID, persistence.DAGRunStatus, *string) error
}

type postgresPersistence[S any] struct {
	postgres *persistence.Postgres
	dags     *persistence.DAGStore
	tasks    *persistence.TaskStore[S]
	events   *persistence.EventStore
}

func newPostgresPersistence[S any](postgres *persistence.Postgres) *postgresPersistence[S] {
	return &postgresPersistence[S]{
		postgres: postgres,
		dags:     persistence.NewDAGStore(postgres.Pool),
		tasks:    persistence.NewTaskStore[S](postgres.Pool),
		events:   persistence.NewEventStore(postgres.Pool),
	}
}

func (p *postgresPersistence[S]) CreateRun(ctx context.Context, d *dagpkg.DAG[S], globalInputs json.RawMessage) (*persistence.DAGRun, error) {
	var version *string
	if d.Version != "" {
		version = &d.Version
	}
	var run *persistence.DAGRun
	err := p.postgres.WithWriteRetry(ctx, "create dag run", func(writeCtx context.Context) error {
		created, err := p.dags.CreateRunning(writeCtx, d.Name, version, globalInputs)
		if err != nil {
			return err
		}
		run = created
		return nil
	})
	return run, err
}

func (p *postgresPersistence[S]) CreateTaskRuns(ctx context.Context, runID uuid.UUID, d *dagpkg.DAG[S]) (map[string]persistence.TaskRun, error) {
	var rows []persistence.TaskRun
	err := p.postgres.WithWriteRetry(ctx, "create task runs", func(writeCtx context.Context) error {
		created, err := p.tasks.CreateForDAG(writeCtx, runID, d)
		if err != nil {
			return err
		}
		rows = created
		return nil
	})
	if err != nil {
		return nil, err
	}
	byName := make(map[string]persistence.TaskRun, len(rows))
	for _, row := range rows {
		byName[row.TaskName] = row
	}
	return byName, nil
}

func (p *postgresPersistence[S]) GetRun(ctx context.Context, runID uuid.UUID) (*persistence.DAGRun, error) {
	return p.dags.Get(ctx, runID)
}

func (p *postgresPersistence[S]) LoadTaskRunsForResume(ctx context.Context, runID uuid.UUID) (map[string]persistence.TaskRun, error) {
	rows, err := p.tasks.LoadForResume(ctx, runID)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]persistence.TaskRun, len(rows))
	for _, row := range rows {
		byName[row.TaskName] = row
	}
	return byName, nil
}

func (p *postgresPersistence[S]) MarkTaskRunning(ctx context.Context, taskRunID uuid.UUID, attempt int) error {
	return p.postgres.WithWriteRetry(ctx, "mark task running", func(writeCtx context.Context) error {
		return p.tasks.MarkRunningAttempt(writeCtx, taskRunID, attempt)
	})
}

func (p *postgresPersistence[S]) MarkTaskSuccess(ctx context.Context, taskRunID uuid.UUID, snapshot json.RawMessage, attempt int) error {
	return p.postgres.WithWriteRetry(ctx, "mark task success", func(writeCtx context.Context) error {
		return p.tasks.MarkTaskSucceededWithSnapshotAndEvent(writeCtx, taskRunID, snapshot, attempt)
	})
}

func (p *postgresPersistence[S]) RecordTaskEvent(ctx context.Context, taskRunID uuid.UUID, eventType persistence.TaskEventType, attempt int, message *string) error {
	return p.postgres.WithWriteRetry(ctx, "record task event", func(writeCtx context.Context) error {
		_, err := p.events.Insert(writeCtx, taskRunID, eventType, attempt, message)
		return err
	})
}

func (p *postgresPersistence[S]) MarkTaskFailed(ctx context.Context, taskRunID uuid.UUID, attempt int, message string) error {
	return p.postgres.WithWriteRetry(ctx, "mark task failed", func(writeCtx context.Context) error {
		return p.tasks.MarkTerminalWithEvent(writeCtx, taskRunID, persistence.TaskRunStatusFailed, persistence.TaskEventFailed, attempt, &message)
	})
}

func (p *postgresPersistence[S]) MarkTaskCancelled(ctx context.Context, taskRunID uuid.UUID, attempt int, message string) error {
	return p.postgres.WithWriteRetry(ctx, "mark task cancelled", func(writeCtx context.Context) error {
		return p.tasks.MarkTerminalWithEvent(writeCtx, taskRunID, persistence.TaskRunStatusCancelled, persistence.TaskEventCancelled, attempt, &message)
	})
}

func (p *postgresPersistence[S]) MarkTaskSkipped(ctx context.Context, taskRunID uuid.UUID, message string) error {
	return p.postgres.WithWriteRetry(ctx, "mark task skipped", func(writeCtx context.Context) error {
		return p.tasks.MarkTerminalWithEvent(writeCtx, taskRunID, persistence.TaskRunStatusSkipped, persistence.TaskEventSkipped, 0, &message)
	})
}

func (p *postgresPersistence[S]) MarkRunTerminal(ctx context.Context, runID uuid.UUID, status persistence.DAGRunStatus, message *string) error {
	return p.postgres.WithWriteRetry(ctx, "mark dag terminal", func(writeCtx context.Context) error {
		return p.dags.MarkTerminal(writeCtx, runID, status, message)
	})
}
