package task

import (
	"testing"
	"time"
)

type fixedRNG struct{ value int64 }

func (f fixedRNG) Int63n(n int64) int64 {
	if f.value >= n {
		return n - 1
	}
	return f.value
}

func TestREQRETRY001NoBackoffIsImmediate(t *testing.T) {
	delay := RetryDelay(RetryConfig{Backoff: BackoffNone, BaseDelay: time.Second, MaxDelay: time.Minute}, 1, fixedRNG{})
	if delay != 0 {
		t.Fatalf("got %v, want 0", delay)
	}
}

func TestREQRETRY001LinearBackoffUsesRetryNumber(t *testing.T) {
	delay := RetryDelay(RetryConfig{Backoff: BackoffLinear, BaseDelay: 2 * time.Second, MaxDelay: time.Minute}, 3, nil)
	if delay != 6*time.Second {
		t.Fatalf("got %v, want 6s", delay)
	}
}

func TestREQRETRY001ExponentialBackoffUsesRetryNumber(t *testing.T) {
	delay := RetryDelay(RetryConfig{Backoff: BackoffExponential, BaseDelay: time.Second, MaxDelay: 90 * time.Second}, 4, nil)
	if delay != 8*time.Second {
		t.Fatalf("got %v, want 8s", delay)
	}
	capped := RetryDelay(RetryConfig{Backoff: BackoffExponential, BaseDelay: time.Second, MaxDelay: 3 * time.Second}, 4, nil)
	if capped != 3*time.Second {
		t.Fatalf("got %v, want 3s", capped)
	}
}

func TestREQRETRY001JitterWithinBounds(t *testing.T) {
	base := time.Second
	delay := RetryDelay(RetryConfig{Backoff: BackoffLinear, BaseDelay: base, MaxDelay: time.Minute, Jitter: true}, 2, fixedRNG{value: int64(base)})
	plain := 2 * base
	if delay < plain || delay > plain+plain/2 {
		t.Fatalf("jitter delay %v outside [%v,%v]", delay, plain, plain+plain/2)
	}
}
