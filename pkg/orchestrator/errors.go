package orchestrator

import (
	"fmt"

	"github.com/shepard-labs/go-dagger/internal/apperrors"
)

var (
	// ErrValidation identifies invalid DAG, task, or run input.
	ErrValidation = apperrors.ErrValidation
	// ErrPersistence identifies storage-layer failures.
	ErrPersistence = apperrors.ErrPersistence
	// ErrRunNotFound identifies missing DAG runs.
	ErrRunNotFound = apperrors.ErrRunNotFound
	// ErrTaskRunNotFound identifies missing task runs.
	ErrTaskRunNotFound = apperrors.ErrTaskRunNotFound
	// ErrRunTerminal identifies attempts to mutate a terminal run or task.
	ErrRunTerminal = apperrors.ErrRunTerminal
	// ErrRunLocked identifies a run already locked for resume.
	ErrRunLocked = apperrors.ErrRunLocked
	// ErrOrchestratorClosed identifies operations after Close.
	ErrOrchestratorClosed = apperrors.ErrOrchestratorClosed
	// ErrFunctionNotRegistered identifies a YAML function missing from its registry.
	ErrFunctionNotRegistered = apperrors.ErrFunctionNotRegistered
)

// TaskFailureError wraps the task name and attempt that produced an error.
type TaskFailureError struct {
	TaskName string
	Attempt  int
	Err      error
}

// Error formats the failed task attempt.
func (e *TaskFailureError) Error() string {
	if e == nil {
		return "task failure"
	}
	if e.Err == nil {
		return fmt.Sprintf("task %q failed on attempt %d", e.TaskName, e.Attempt)
	}
	return fmt.Sprintf("task %q failed on attempt %d: %v", e.TaskName, e.Attempt, e.Err)
}

// Unwrap returns the underlying task error.
func (e *TaskFailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
