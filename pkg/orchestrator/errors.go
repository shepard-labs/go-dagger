package orchestrator

import (
	"fmt"

	"github.com/shepard-labs/go-dagger/internal/apperrors"
)

var (
	ErrValidation            = apperrors.ErrValidation
	ErrPersistence           = apperrors.ErrPersistence
	ErrRunNotFound           = apperrors.ErrRunNotFound
	ErrTaskRunNotFound       = apperrors.ErrTaskRunNotFound
	ErrRunTerminal           = apperrors.ErrRunTerminal
	ErrRunLocked             = apperrors.ErrRunLocked
	ErrOrchestratorClosed    = apperrors.ErrOrchestratorClosed
	ErrFunctionNotRegistered = apperrors.ErrFunctionNotRegistered
)

type TaskFailureError struct {
	TaskName string
	Attempt  int
	Err      error
}

func (e *TaskFailureError) Error() string {
	if e == nil {
		return "task failure"
	}
	if e.Err == nil {
		return fmt.Sprintf("task %q failed on attempt %d", e.TaskName, e.Attempt)
	}
	return fmt.Sprintf("task %q failed on attempt %d: %v", e.TaskName, e.Attempt, e.Err)
}

func (e *TaskFailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
