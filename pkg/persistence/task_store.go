package persistence

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shepard-labs/go-dagger/internal/apperrors"
	dagpkg "github.com/shepard-labs/go-dagger/pkg/dag"
)

// TaskStore persists and queries task run rows.
type TaskStore[S any] struct{ pool *pgxpool.Pool }

// NewTaskStore returns a TaskStore backed by pool.
func NewTaskStore[S any](pool *pgxpool.Pool) *TaskStore[S] { return &TaskStore[S]{pool: pool} }

// CreateForDAG creates pending task rows for every task in DAG order.
func (s *TaskStore[S]) CreateForDAG(ctx context.Context, runID uuid.UUID, d *dagpkg.DAG[S]) ([]TaskRun, error) {
	taskRuns := make([]TaskRun, 0, len(d.TaskOrder))
	for orderIndex, name := range d.TaskOrder {
		t := d.Tasks[name]
		tags, err := json.Marshal(t.Tags)
		if err != nil {
			return nil, fmt.Errorf("%w: marshal task tags: %v", apperrors.ErrPersistence, err)
		}
		run := TaskRun{ID: NewTaskRunID(), DAGRunID: runID, TaskName: name, Status: TaskRunStatusPending, Description: t.Description, Tags: tags, Priority: t.Priority, OrderIndex: orderIndex}
		if d.Version != "" {
			run.DAGVersion = &d.Version
		}
		err = s.pool.QueryRow(ctx, `
INSERT INTO task_runs (id, dag_run_id, dag_version, task_name, status, attempt, description, tags, priority, order_index, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,0,$6,$7,$8,$9,NOW(),NOW())
RETURNING created_at, updated_at`, run.ID, run.DAGRunID, run.DAGVersion, run.TaskName, run.Status, run.Description, run.Tags, run.Priority, run.OrderIndex).
			Scan(&run.CreatedAt, &run.UpdatedAt)
		if err != nil {
			return nil, persistenceError("create task run", err, "")
		}
		taskRuns = append(taskRuns, run)
	}
	return taskRuns, nil
}

// MarkRunningAttempt records the currently executing attempt number.
func (s *TaskStore[S]) MarkRunningAttempt(ctx context.Context, taskRunID uuid.UUID, attempt int) error {
	tag, err := s.pool.Exec(ctx, `UPDATE task_runs SET status='running', attempt=$2, started_at=NOW(), finished_at=NULL, error_message=NULL, updated_at=NOW() WHERE id=$1 AND status IN ('pending','running','failed','skipped')`, taskRunID, attempt)
	if err != nil {
		return persistenceError("mark task running", err, "")
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: task run %s cannot be marked running", apperrors.ErrRunTerminal, taskRunID)
	}
	return nil
}

// MarkTaskSucceededWithSnapshotAndEvent stores the state snapshot and success event atomically.
func (s *TaskStore[S]) MarkTaskSucceededWithSnapshotAndEvent(ctx context.Context, taskRunID uuid.UUID, snapshot json.RawMessage, attempt int) error {
	if len(snapshot) == 0 {
		snapshot = json.RawMessage(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return persistenceError("begin task success transaction", err, "")
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE task_runs SET status='success', run_state_snapshot=$2, finished_at=NOW(), updated_at=NOW(), error_message=NULL WHERE id=$1 AND status IN ('pending','running','failed','skipped')`, taskRunID, snapshot)
	if err != nil {
		return persistenceError("mark task success", err, "")
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: task run %s cannot be marked success", apperrors.ErrRunTerminal, taskRunID)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (id, task_run_id, event_type, attempt, created_at) VALUES ($1,$2,$3,$4,NOW())`, NewTaskEventID(), taskRunID, TaskEventSucceeded, attempt); err != nil {
		return persistenceError("insert task success event", err, "")
	}
	if err := tx.Commit(ctx); err != nil {
		return persistenceError("commit task success transaction", err, "")
	}
	return nil
}

// MarkTerminalWithEvent stores a non-success terminal status and matching event atomically.
func (s *TaskStore[S]) MarkTerminalWithEvent(ctx context.Context, taskRunID uuid.UUID, status TaskRunStatus, eventType TaskEventType, attempt int, errorMessage *string) error {
	if status == TaskRunStatusSuccess {
		return fmt.Errorf("%w: use MarkTaskSucceededWithSnapshotAndEvent for success", apperrors.ErrValidation)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return persistenceError("begin task terminal transaction", err, "")
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE task_runs SET status=$2, finished_at=NOW(), updated_at=NOW(), error_message=$3, run_state_snapshot=NULL WHERE id=$1 AND status IN ('pending','running','failed','skipped')`, taskRunID, status, errorMessage)
	if err != nil {
		return persistenceError("mark task terminal", err, "")
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: task run %s cannot be marked terminal", apperrors.ErrRunTerminal, taskRunID)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO task_events (id, task_run_id, event_type, attempt, error_message, created_at) VALUES ($1,$2,$3,$4,$5,NOW())`, NewTaskEventID(), taskRunID, eventType, attempt, errorMessage); err != nil {
		return persistenceError("insert task terminal event", err, "")
	}
	if err := tx.Commit(ctx); err != nil {
		return persistenceError("commit task terminal transaction", err, "")
	}
	return nil
}

// Get fetches a task run by ID.
func (s *TaskStore[S]) Get(ctx context.Context, id uuid.UUID) (*TaskRun, error) {
	return scanTaskRun(s.pool.QueryRow(ctx, taskRunSelectSQL()+` WHERE id=$1`, id))
}

// ListByRun lists task runs for a DAG run in DAG order.
func (s *TaskStore[S]) ListByRun(ctx context.Context, runID uuid.UUID) ([]TaskRun, error) {
	rows, err := s.pool.Query(ctx, taskRunSelectSQL()+` WHERE dag_run_id=$1 ORDER BY order_index ASC`, runID)
	if err != nil {
		return nil, persistenceError("list task runs", err, "")
	}
	defer rows.Close()
	return collectTaskRuns(rows)
}

// LatestSuccessfulSnapshot returns the latest successful task row with a snapshot.
func (s *TaskStore[S]) LatestSuccessfulSnapshot(ctx context.Context, runID uuid.UUID) (*TaskRun, error) {
	return scanTaskRun(s.pool.QueryRow(ctx, taskRunSelectSQL()+` WHERE dag_run_id=$1 AND status='success' ORDER BY order_index DESC LIMIT 1`, runID))
}

// LoadForResume loads all task rows needed to build a resume plan.
func (s *TaskStore[S]) LoadForResume(ctx context.Context, runID uuid.UUID) ([]TaskRun, error) {
	return s.ListByRun(ctx, runID)
}

func taskRunSelectSQL() string {
	return `SELECT id, dag_run_id, dag_version, task_name, status, attempt, started_at, finished_at, error_message, description, tags, priority, order_index, run_state_snapshot, created_at, updated_at FROM task_runs`
}

func scanTaskRun(row pgx.Row) (*TaskRun, error) {
	var run TaskRun
	if err := row.Scan(&run.ID, &run.DAGRunID, &run.DAGVersion, &run.TaskName, &run.Status, &run.Attempt, &run.StartedAt, &run.FinishedAt, &run.ErrorMessage, &run.Description, &run.Tags, &run.Priority, &run.OrderIndex, &run.RunStateSnapshot, &run.CreatedAt, &run.UpdatedAt); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("%w: task run not found", apperrors.ErrTaskRunNotFound)
		}
		return nil, persistenceError("get task run", err, "")
	}
	return &run, nil
}

func collectTaskRuns(rows pgx.Rows) ([]TaskRun, error) {
	runs := []TaskRun{}
	for rows.Next() {
		run, err := scanTaskRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, *run)
	}
	if err := rows.Err(); err != nil {
		return nil, persistenceError("scan task runs", err, "")
	}
	return runs, nil
}
