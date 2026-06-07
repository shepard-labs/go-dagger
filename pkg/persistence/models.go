package persistence

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type DAGRunStatus string

const (
	DAGRunStatusRunning   DAGRunStatus = "running"
	DAGRunStatusSuccess   DAGRunStatus = "success"
	DAGRunStatusFailed    DAGRunStatus = "failed"
	DAGRunStatusCancelled DAGRunStatus = "cancelled"
)

type TaskRunStatus string

const (
	TaskRunStatusPending   TaskRunStatus = "pending"
	TaskRunStatusRunning   TaskRunStatus = "running"
	TaskRunStatusSuccess   TaskRunStatus = "success"
	TaskRunStatusFailed    TaskRunStatus = "failed"
	TaskRunStatusSkipped   TaskRunStatus = "skipped"
	TaskRunStatusCancelled TaskRunStatus = "cancelled"
)

type TaskEventType string

const (
	TaskEventStarted         TaskEventType = "started"
	TaskEventSucceeded       TaskEventType = "succeeded"
	TaskEventFailed          TaskEventType = "failed"
	TaskEventRetried         TaskEventType = "retried"
	TaskEventCancelled       TaskEventType = "cancelled"
	TaskEventSkipped         TaskEventType = "skipped"
	TaskEventRetryExhausted  TaskEventType = "retry_exhausted"
	TaskEventAfterHookFailed TaskEventType = "after_hook_failed"
)

type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

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

type TaskEvent struct {
	ID           uuid.UUID
	TaskRunID    uuid.UUID
	EventType    TaskEventType
	Attempt      int
	ErrorMessage *string
	CreatedAt    time.Time
}

type TaskLog struct {
	ID        uuid.UUID
	DAGRunID  uuid.UUID
	TaskRunID *uuid.UUID
	Level     LogLevel
	Message   string
	Fields    json.RawMessage
	CreatedAt time.Time
}

func NewDAGRunID() uuid.UUID    { return uuid.New() }
func NewTaskRunID() uuid.UUID   { return uuid.New() }
func NewTaskEventID() uuid.UUID { return uuid.New() }
func NewTaskLogID() uuid.UUID   { return uuid.New() }
