package persistence

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// DAGRunStatus is the persisted lifecycle state of a DAG run.
type DAGRunStatus string

const (
	// DAGRunStatusRunning means a DAG run is still active or resumable.
	DAGRunStatusRunning DAGRunStatus = "running"
	// DAGRunStatusSuccess means all required tasks completed successfully.
	DAGRunStatusSuccess DAGRunStatus = "success"
	// DAGRunStatusFailed means the DAG run ended with an error.
	DAGRunStatusFailed DAGRunStatus = "failed"
	// DAGRunStatusCancelled means the DAG run was cancelled before completion.
	DAGRunStatusCancelled DAGRunStatus = "cancelled"
)

// TaskRunStatus is the persisted lifecycle state of a task run.
type TaskRunStatus string

const (
	// TaskRunStatusPending means a task has not started yet.
	TaskRunStatusPending TaskRunStatus = "pending"
	// TaskRunStatusRunning means a task attempt is in progress.
	TaskRunStatusRunning TaskRunStatus = "running"
	// TaskRunStatusSuccess means a task completed and stored a state snapshot.
	TaskRunStatusSuccess TaskRunStatus = "success"
	// TaskRunStatusFailed means the latest task attempt failed.
	TaskRunStatusFailed TaskRunStatus = "failed"
	// TaskRunStatusSkipped means a task did not run because a dependency failed.
	TaskRunStatusSkipped TaskRunStatus = "skipped"
	// TaskRunStatusCancelled means a task stopped because its run was cancelled.
	TaskRunStatusCancelled TaskRunStatus = "cancelled"
)

// TaskEventType names append-only task lifecycle events.
type TaskEventType string

const (
	// TaskEventStarted records the start of an attempt.
	TaskEventStarted TaskEventType = "started"
	// TaskEventSucceeded records successful task completion.
	TaskEventSucceeded TaskEventType = "succeeded"
	// TaskEventFailed records failed task completion.
	TaskEventFailed TaskEventType = "failed"
	// TaskEventRetried records a retry attempt being scheduled.
	TaskEventRetried TaskEventType = "retried"
	// TaskEventCancelled records task cancellation.
	TaskEventCancelled TaskEventType = "cancelled"
	// TaskEventSkipped records dependency-based skipping.
	TaskEventSkipped TaskEventType = "skipped"
	// TaskEventRetryExhausted records that no retry attempts remain.
	TaskEventRetryExhausted TaskEventType = "retry_exhausted"
	// TaskEventAfterHookFailed records a non-terminal after-hook failure.
	TaskEventAfterHookFailed TaskEventType = "after_hook_failed"
)

// LogLevel is the persisted severity for orchestrator logs.
type LogLevel string

const (
	// LogLevelDebug records diagnostic logs.
	LogLevelDebug LogLevel = "debug"
	// LogLevelInfo records informational logs.
	LogLevelInfo LogLevel = "info"
	// LogLevelWarn records warning logs.
	LogLevelWarn LogLevel = "warn"
	// LogLevelError records error logs.
	LogLevelError LogLevel = "error"
)

// DAGRun is one persisted execution of a DAG definition.
type DAGRun struct {
	ID           uuid.UUID
	DAGName      string
	DAGVersion   *string
	GlobalInputs json.RawMessage
	Status       DAGRunStatus
	StartedAt    time.Time
	FinishedAt   *time.Time
	ErrorMessage *string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// TaskRun is one persisted execution record for a DAG task.
type TaskRun struct {
	ID               uuid.UUID
	DAGRunID         uuid.UUID
	DAGVersion       *string
	TaskName         string
	Status           TaskRunStatus
	Attempt          int
	StartedAt        *time.Time
	FinishedAt       *time.Time
	ErrorMessage     *string
	Description      string
	Tags             json.RawMessage
	Priority         int
	OrderIndex       int
	RunStateSnapshot json.RawMessage
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// TaskEvent is an append-only lifecycle event for a task run.
type TaskEvent struct {
	ID           uuid.UUID
	TaskRunID    uuid.UUID
	EventType    TaskEventType
	Attempt      int
	ErrorMessage *string
	CreatedAt    time.Time
}

// TaskLog is a structured log line associated with a DAG run or task run.
type TaskLog struct {
	ID        uuid.UUID
	DAGRunID  uuid.UUID
	TaskRunID *uuid.UUID
	Level     LogLevel
	Message   string
	Fields    json.RawMessage
	CreatedAt time.Time
}

// NewDAGRunID creates a new DAG run identifier.
func NewDAGRunID() uuid.UUID { return uuid.New() }

// NewTaskRunID creates a new task run identifier.
func NewTaskRunID() uuid.UUID { return uuid.New() }

// NewTaskEventID creates a new task event identifier.
func NewTaskEventID() uuid.UUID { return uuid.New() }

// NewTaskLogID creates a new task log identifier.
func NewTaskLogID() uuid.UUID { return uuid.New() }
