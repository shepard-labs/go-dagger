package persistence

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type LogStore struct{ pool *pgxpool.Pool }

func NewLogStore(pool *pgxpool.Pool) *LogStore { return &LogStore{pool: pool} }

func (s *LogStore) Insert(ctx context.Context, dagRunID uuid.UUID, taskRunID *uuid.UUID, level LogLevel, message string, fields json.RawMessage) (*TaskLog, error) {
	if len(fields) == 0 {
		fields = nil
	}
	log := &TaskLog{ID: NewTaskLogID(), DAGRunID: dagRunID, TaskRunID: taskRunID, Level: level, Message: message, Fields: fields}
	err := s.pool.QueryRow(ctx, `INSERT INTO task_logs (id, dag_run_id, task_run_id, level, message, fields, created_at) VALUES ($1,$2,$3,$4,$5,$6,NOW()) RETURNING created_at`, log.ID, log.DAGRunID, log.TaskRunID, log.Level, log.Message, log.Fields).Scan(&log.CreatedAt)
	if err != nil {
		return nil, persistenceError("insert task log", err, "")
	}
	return log, nil
}

func (s *LogStore) ListByDAGRun(ctx context.Context, dagRunID uuid.UUID) ([]TaskLog, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, dag_run_id, task_run_id, level, message, fields, created_at FROM task_logs WHERE dag_run_id=$1 ORDER BY created_at ASC, id ASC`, dagRunID)
	if err != nil {
		return nil, persistenceError("list dag run logs", err, "")
	}
	defer rows.Close()
	return collectTaskLogs(rows)
}

func (s *LogStore) ListByTaskRun(ctx context.Context, taskRunID uuid.UUID) ([]TaskLog, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, dag_run_id, task_run_id, level, message, fields, created_at FROM task_logs WHERE task_run_id=$1 ORDER BY created_at ASC, id ASC`, taskRunID)
	if err != nil {
		return nil, persistenceError("list task logs", err, "")
	}
	defer rows.Close()
	return collectTaskLogs(rows)
}

func collectTaskLogs(rows pgx.Rows) ([]TaskLog, error) {
	logs := []TaskLog{}
	for rows.Next() {
		var log TaskLog
		if err := rows.Scan(&log.ID, &log.DAGRunID, &log.TaskRunID, &log.Level, &log.Message, &log.Fields, &log.CreatedAt); err != nil {
			return nil, persistenceError("scan task log", err, "")
		}
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, persistenceError("scan task logs", err, "")
	}
	return logs, nil
}
