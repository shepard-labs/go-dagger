package orchestrator

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/shepard-labs/go-dagger/pkg/persistence"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type taskResult[S any] struct {
	name    string
	state   *S
	status  persistence.TaskRunStatus
	attempt int
	err     error
}

type attemptResult[S any] struct {
	state *S
	err   error
}

func (o *Orchestrator[S]) executeTaskWithRetries(runCtx context.Context, dagName string, t *task.Task[S], row persistence.TaskRun, state *S) taskResult[S] {
	maxAttempts := max(t.Retry.MaxAttempts, 1)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			if err := o.persistence.RecordTaskEvent(runCtx, row.ID, persistence.TaskEventRetried, attempt, nil); err != nil {
				return taskResult[S]{name: t.Name, status: persistence.TaskRunStatusFailed, attempt: attempt, err: err}
			}
			delay := task.RetryDelay(t.Retry, attempt-1, nil)
			if delay > 0 {
				if err := sleepContext(runCtx, delay); err != nil {
					return o.cancelledTaskResult(context.Background(), t.Name, row.ID, attempt-1, err)
				}
			}
		}

		if err := o.persistence.MarkTaskRunning(runCtx, row.ID, attempt); err != nil {
			return taskResult[S]{name: t.Name, status: persistence.TaskRunStatusFailed, attempt: attempt, err: err}
		}
		if err := o.persistence.RecordTaskEvent(runCtx, row.ID, persistence.TaskEventStarted, attempt, nil); err != nil {
			return taskResult[S]{name: t.Name, status: persistence.TaskRunStatusFailed, attempt: attempt, err: err}
		}

		result, err := o.runAttempt(runCtx, dagName, t, row, attempt, state)
		if err == nil {
			snapshot, storedState, snapErr := snapshotRunState(result)
			if snapErr != nil {
				return taskResult[S]{name: t.Name, status: persistence.TaskRunStatusFailed, attempt: attempt, err: snapErr}
			}
			if err := o.persistence.MarkTaskSuccess(context.Background(), row.ID, snapshot, attempt); err != nil {
				return taskResult[S]{name: t.Name, status: persistence.TaskRunStatusFailed, attempt: attempt, err: err}
			}
			return taskResult[S]{name: t.Name, state: storedState, status: persistence.TaskRunStatusSuccess, attempt: attempt}
		}

		lastErr = err
		if runCtx.Err() != nil {
			return o.cancelledTaskResult(context.Background(), t.Name, row.ID, attempt, classifyRunContextError(runCtx.Err()))
		}
		if errors.Is(err, context.Canceled) && errors.Is(runCtx.Err(), context.Canceled) {
			return o.cancelledTaskResult(context.Background(), t.Name, row.ID, attempt, err)
		}
		if attempt < maxAttempts {
			continue
		}
		message := err.Error()
		terminalCtx := runCtx
		if runCtx.Err() != nil {
			terminalCtx = context.Background()
		}
		if eventErr := o.persistence.RecordTaskEvent(terminalCtx, row.ID, persistence.TaskEventRetryExhausted, attempt, &message); eventErr != nil {
			return taskResult[S]{name: t.Name, status: persistence.TaskRunStatusFailed, attempt: attempt, err: eventErr}
		}
		failure := &TaskFailureError{TaskName: t.Name, Attempt: attempt, Err: err}
		if err := o.persistence.MarkTaskFailed(terminalCtx, row.ID, attempt, failure.Error()); err != nil {
			return taskResult[S]{name: t.Name, status: persistence.TaskRunStatusFailed, attempt: attempt, err: err}
		}
		return taskResult[S]{name: t.Name, status: persistence.TaskRunStatusFailed, attempt: attempt, err: lastErr}
	}
	return taskResult[S]{name: t.Name, status: persistence.TaskRunStatusFailed, attempt: maxAttempts, err: lastErr}
}

func (o *Orchestrator[S]) cancelledTaskResult(ctx context.Context, name string, taskRunID uuid.UUID, attempt int, err error) taskResult[S] {
	message := err.Error()
	if persistErr := o.persistence.MarkTaskCancelled(ctx, taskRunID, attempt, message); persistErr != nil {
		return taskResult[S]{name: name, status: persistence.TaskRunStatusFailed, attempt: attempt, err: persistErr}
	}
	return taskResult[S]{name: name, status: persistence.TaskRunStatusCancelled, attempt: attempt, err: err}
}

func (o *Orchestrator[S]) runAttempt(runCtx context.Context, dagName string, t *task.Task[S], row persistence.TaskRun, attempt int, state *S) (*S, error) {
	attemptCtx, cancel := attemptContext(runCtx, t.Timeout)
	attemptCtx = contextWithLogger(attemptCtx, o.taskLogger(row.DAGRunID, row.ID, dagName, t.Name, attempt))
	defer cancel()
	results := make(chan attemptResult[S], 1)
	go func() {
		state, err := o.executeTaskAttempt(attemptCtx, t, row.ID, attempt, state)
		results <- attemptResult[S]{state: state, err: err}
	}()

	select {
	case result := <-results:
		return result.state, result.err
	case <-attemptCtx.Done():
		cancel()
		select {
		case result := <-results:
			if result.err != nil {
				return nil, result.err
			}
			if attemptCtx.Err() != nil {
				return nil, attemptCtx.Err()
			}
			return result.state, nil
		case <-time.After(o.config.GracePeriod):
			return nil, attemptCtx.Err()
		}
	}
}

func (o *Orchestrator[S]) executeTaskAttempt(ctx context.Context, t *task.Task[S], taskRunID uuid.UUID, attempt int, state *S) (*S, error) {
	input, err := copyRunState(state)
	if err != nil {
		return nil, err
	}
	for _, hook := range t.BeforeHooks {
		if err := hook(ctx, input); err != nil {
			return nil, err
		}
	}
	result, err := t.Execute(ctx, input)
	execErr := err
	if result == nil {
		result = input
	}
	for _, hook := range t.AfterHooks {
		if hookErr := hook(ctx, result, execErr); hookErr != nil {
			message := hookErr.Error()
			_ = o.persistence.RecordTaskEvent(context.Background(), taskRunID, persistence.TaskEventAfterHookFailed, attempt, &message)
		}
	}
	if execErr != nil {
		return nil, execErr
	}
	output, err := copyRunState(result)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
