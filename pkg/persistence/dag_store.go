package persistence

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shepard-labs/go-dagger/internal/apperrors"
)

// DAGStore persists and queries DAG run rows.
type DAGStore struct{ pool *pgxpool.Pool }

// NewDAGStore returns a DAGStore backed by pool.
func NewDAGStore(pool *pgxpool.Pool) *DAGStore { return &DAGStore{pool: pool} }

// CreateRunning inserts a new running DAG run.
func (s *DAGStore) CreateRunning(ctx context.Context, dagName string, dagVersion *string, globalInputs json.RawMessage) (*DAGRun, error) {
	if len(globalInputs) == 0 {
		globalInputs = json.RawMessage(`{}`)
	}
	run := &DAGRun{ID: NewDAGRunID(), DAGName: dagName, DAGVersion: dagVersion, GlobalInputs: globalInputs, Status: DAGRunStatusRunning}
	err := s.pool.QueryRow(ctx, `
INSERT INTO dag_runs (id, dag_name, dag_version, global_inputs, status, started_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NOW())
RETURNING started_at, created_at, updated_at`, run.ID, run.DAGName, run.DAGVersion, run.GlobalInputs, run.Status).
		Scan(&run.StartedAt, &run.CreatedAt, &run.UpdatedAt)
	if err != nil {
		return nil, persistenceError("create dag run", err, "")
	}
	return run, nil
}

// Get fetches a DAG run by ID.
func (s *DAGStore) Get(ctx context.Context, id uuid.UUID) (*DAGRun, error) {
	row := s.pool.QueryRow(ctx, `SELECT id, dag_name, dag_version, global_inputs, status, started_at, finished_at, error_message, created_at, updated_at FROM dag_runs WHERE id=$1`, id)
	return scanDAGRun(row)
}

// List returns recent DAG runs with default and maximum limits applied.
func (s *DAGStore) List(ctx context.Context, limit int) ([]DAGRun, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, `SELECT id, dag_name, dag_version, global_inputs, status, started_at, finished_at, error_message, created_at, updated_at FROM dag_runs ORDER BY started_at DESC, id ASC LIMIT $1`, limit)
	if err != nil {
		return nil, persistenceError("list dag runs", err, "")
	}
	defer rows.Close()
	return collectDAGRuns(rows)
}

// ListRunning returns active DAG runs ordered by start time.
func (s *DAGStore) ListRunning(ctx context.Context) ([]DAGRun, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, dag_name, dag_version, global_inputs, status, started_at, finished_at, error_message, created_at, updated_at FROM dag_runs WHERE status='running' ORDER BY started_at ASC, id ASC`)
	if err != nil {
		return nil, persistenceError("list running dag runs", err, "")
	}
	defer rows.Close()
	return collectDAGRuns(rows)
}

// MarkTerminal moves a running DAG run to a terminal status exactly once.
func (s *DAGStore) MarkTerminal(ctx context.Context, id uuid.UUID, status DAGRunStatus, errorMessage *string) error {
	if status == DAGRunStatusRunning {
		return fmt.Errorf("%w: running is not terminal", apperrors.ErrValidation)
	}
	tag, err := s.pool.Exec(ctx, `UPDATE dag_runs SET status=$2, finished_at=NOW(), error_message=$3, updated_at=NOW() WHERE id=$1 AND status='running'`, id, status, errorMessage)
	if err != nil {
		return persistenceError("mark dag terminal", err, "")
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: dag run %s is not running or does not exist", apperrors.ErrRunTerminal, id)
	}
	return nil
}

func scanDAGRun(row pgx.Row) (*DAGRun, error) {
	var run DAGRun
	if err := row.Scan(&run.ID, &run.DAGName, &run.DAGVersion, &run.GlobalInputs, &run.Status, &run.StartedAt, &run.FinishedAt, &run.ErrorMessage, &run.CreatedAt, &run.UpdatedAt); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("%w: dag run not found", apperrors.ErrRunNotFound)
		}
		return nil, persistenceError("get dag run", err, "")
	}
	return &run, nil
}

func collectDAGRuns(rows pgx.Rows) ([]DAGRun, error) {
	runs := []DAGRun{}
	for rows.Next() {
		run, err := scanDAGRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, *run)
	}
	if err := rows.Err(); err != nil {
		return nil, persistenceError("scan dag runs", err, "")
	}
	return runs, nil
}
