package apperrors

import "errors"

var (
	ErrValidation            = errors.New("validation error")
	ErrPersistence           = errors.New("persistence error")
	ErrRunNotFound           = errors.New("run not found")
	ErrTaskRunNotFound       = errors.New("task run not found")
	ErrRunTerminal           = errors.New("run terminal")
	ErrRunLocked             = errors.New("run locked")
	ErrOrchestratorClosed    = errors.New("orchestrator closed")
	ErrFunctionNotRegistered = errors.New("function not registered")
)
