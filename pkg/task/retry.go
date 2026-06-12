package task

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/shepard-labs/go-dagger/internal/apperrors"
)

// BackoffType selects how retry delays grow between attempts.
type BackoffType string

const (
	// BackoffNone disables sleeping between retry attempts.
	BackoffNone BackoffType = "none"
	// BackoffLinear increases delay by one base interval per retry.
	BackoffLinear BackoffType = "linear"
	// BackoffExponential doubles delay from the base interval per retry.
	BackoffExponential BackoffType = "exponential"
)

// RetryConfig defines task retry limits and delay behavior.
type RetryConfig struct {
	MaxAttempts int
	Backoff     BackoffType
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      bool
}

// JitterSource supplies random values for retry jitter.
type JitterSource interface {
	Int63n(n int64) int64
}

// Normalize applies default retry values.
func (r *RetryConfig) Normalize() {
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 1
	}
	if r.Backoff == "" {
		r.Backoff = BackoffNone
	}
}

// Validate checks retry settings after applying defaults.
func (r RetryConfig) Validate() error {
	r.Normalize()
	if r.MaxAttempts < 1 {
		return fmt.Errorf("%w: max_attempts must be >= 1", apperrors.ErrValidation)
	}
	if r.BaseDelay < 0 {
		return fmt.Errorf("%w: backoff_base must be non-negative", apperrors.ErrValidation)
	}
	if r.MaxDelay < 0 {
		return fmt.Errorf("%w: backoff_max must be non-negative", apperrors.ErrValidation)
	}
	if r.MaxDelay > 0 && r.BaseDelay > 0 && r.MaxDelay < r.BaseDelay {
		return fmt.Errorf("%w: backoff_max must be >= backoff_base", apperrors.ErrValidation)
	}
	switch r.Backoff {
	case BackoffNone, BackoffLinear, BackoffExponential:
		return nil
	default:
		return fmt.Errorf("%w: invalid backoff %q", apperrors.ErrValidation, r.Backoff)
	}
}

// RetryDelay returns the wait duration before the given retry number.
func RetryDelay(config RetryConfig, retryNumber int, rng JitterSource) time.Duration {
	if retryNumber <= 0 {
		return 0
	}
	config.Normalize()
	base := config.BaseDelay
	if base == 0 {
		base = time.Second
	}
	maxDelay := config.MaxDelay
	if maxDelay == 0 {
		maxDelay = time.Minute
	}

	var delay time.Duration
	switch config.Backoff {
	case BackoffLinear:
		if retryNumber > int(math.MaxInt64/int64(base)) {
			delay = maxDelay
		} else {
			delay = base * time.Duration(retryNumber)
		}
	case BackoffExponential:
		shift := retryNumber - 1
		if shift >= 62 || int64(base) > math.MaxInt64/(int64(1)<<shift) {
			delay = maxDelay
		} else {
			delay = base * time.Duration(int64(1)<<shift)
		}
	default:
		return 0
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	if config.Jitter && delay > 0 {
		if rng == nil {
			rng = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
		bound := int64(delay / 2)
		if bound > 0 {
			delay += time.Duration(rng.Int63n(bound + 1))
		}
	}
	return delay
}
