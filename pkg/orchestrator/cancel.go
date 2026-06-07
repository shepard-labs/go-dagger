package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

func (o *Orchestrator[S]) Cancel(runID uuid.UUID) error {
	if o == nil {
		return fmt.Errorf("%w: orchestrator is nil", ErrOrchestratorClosed)
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return ErrOrchestratorClosed
	}
	run, ok := o.activeRuns[runID]
	if !ok {
		o.mu.Unlock()
		return fmt.Errorf("%w: run %s", ErrRunNotFound, runID)
	}
	if run.terminal {
		o.mu.Unlock()
		return fmt.Errorf("%w: run %s", ErrRunTerminal, runID)
	}
	run.cancel()
	o.mu.Unlock()
	return nil
}

func classifyRunContextError(err error) error {
	if err == nil {
		return nil
	}
	if err == context.DeadlineExceeded {
		return context.DeadlineExceeded
	}
	if err == context.Canceled {
		return context.Canceled
	}
	return err
}
