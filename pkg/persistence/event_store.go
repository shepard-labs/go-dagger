package persistence

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EventStore persists and queries task event rows.
type EventStore struct {
	pool   *pgxpool.Pool
	schema string
}

// NewEventStore returns an EventStore backed by pool.
// Schema qualifies every table reference (e.g. "spiderreach.task_events");
// empty leaves table names unqualified (resolved via the connection's
// search_path, typically "public").
func NewEventStore(pool *pgxpool.Pool, schema string) *EventStore {
	return &EventStore{pool: pool, schema: schema}
}

// q prefixes the table name with the configured schema, if any.
func (s *EventStore) q(table string) string {
	if s.schema == "" {
		return table
	}
	return s.schema + "." + table
}

// Insert appends a task event row.
func (s *EventStore) Insert(ctx context.Context, taskRunID uuid.UUID, eventType TaskEventType, attempt int, errorMessage *string) (*TaskEvent, error) {
	event := &TaskEvent{ID: NewTaskEventID(), TaskRunID: taskRunID, EventType: eventType, Attempt: attempt, ErrorMessage: errorMessage}
	err := s.pool.QueryRow(ctx, `INSERT INTO `+s.q("task_events")+` (id, task_run_id, event_type, attempt, error_message, created_at) VALUES ($1,$2,$3,$4,$5,NOW()) RETURNING created_at`, event.ID, event.TaskRunID, event.EventType, event.Attempt, event.ErrorMessage).Scan(&event.CreatedAt)
	if err != nil {
		return nil, persistenceError("insert task event", err, "")
	}
	return event, nil
}

// ListByTaskRun returns task events in creation order.
func (s *EventStore) ListByTaskRun(ctx context.Context, taskRunID uuid.UUID) ([]TaskEvent, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, task_run_id, event_type, attempt, error_message, created_at FROM `+s.q("task_events")+` WHERE task_run_id=$1 ORDER BY created_at ASC, id ASC`, taskRunID)
	if err != nil {
		return nil, persistenceError("list task events", err, "")
	}
	defer rows.Close()
	events := []TaskEvent{}
	for rows.Next() {
		var event TaskEvent
		if err := rows.Scan(&event.ID, &event.TaskRunID, &event.EventType, &event.Attempt, &event.ErrorMessage, &event.CreatedAt); err != nil {
			return nil, persistenceError("scan task event", err, "")
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, persistenceError("scan task events", err, "")
	}
	return events, nil
}
