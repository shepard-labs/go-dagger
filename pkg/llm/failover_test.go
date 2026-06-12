package llm

import (
	"context"
	"errors"
	"testing"
)

func TestREQFAILOVER001_WithFailoverIsClientDecorator(t *testing.T) {
	var _ Client = WithFailover(&mockClient{}, FailoverConfig{})
}

func TestREQFAILOVER002_CancelledContextNeverRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	primary := &mockClient{}
	fallback := &mockClient{}
	client := WithFailover(primary, FailoverConfig{
		ShouldFailover: func(context.Context, error) bool { return true },
		GetNext:        func(int) Client { return fallback },
	})

	_, err := client.Generate(ctx, GenerateOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if primary.callCount() != 0 || fallback.callCount() != 0 {
		t.Fatalf("calls primary=%d fallback=%d, want 0/0", primary.callCount(), fallback.callCount())
	}
}

func TestREQFAILOVER003_GetNextNilReturnsLastErrorNoPanic(t *testing.T) {
	want := errors.New("overloaded")
	primary := &mockClient{errors: []error{want}}
	client := WithFailover(primary, FailoverConfig{
		ShouldFailover: func(context.Context, error) bool { return true },
		GetNext:        func(int) Client { return nil },
	})

	_, err := client.Generate(context.Background(), GenerateOptions{})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestREQFAILOVER004_NoSameClientRetry(t *testing.T) {
	primaryErr := errors.New("primary")
	fallbackResult := &GenerateResult{FinishReason: FinishReasonStop}
	primary := &mockClient{errors: []error{primaryErr}}
	fallback := &mockClient{results: []*GenerateResult{fallbackResult}}
	client := WithFailover(primary, FailoverConfig{
		ShouldFailover: func(context.Context, error) bool { return true },
		GetNext:        func(int) Client { return fallback },
		MaxAttempts:    1,
	})

	result, err := client.Generate(context.Background(), GenerateOptions{})
	if err != nil {
		t.Fatalf("Generate error = %v", err)
	}
	if result != fallbackResult {
		t.Fatalf("result = %p, want fallback result %p", result, fallbackResult)
	}
	if primary.callCount() != 1 || fallback.callCount() != 1 {
		t.Fatalf("calls primary=%d fallback=%d, want 1/1", primary.callCount(), fallback.callCount())
	}
}

func TestREQFAILOVER005_ComposableWithCacheAndAdapter(t *testing.T) {
	primaryErr := errors.New("primary")
	fallbackResult := &GenerateResult{FinishReason: FinishReasonStop}
	primary := &mockClient{errors: []error{primaryErr}}
	fallback := &mockClient{results: []*GenerateResult{fallbackResult}}
	client := WithCache(WithFailover(primary, FailoverConfig{
		ShouldFailover: func(context.Context, error) bool { return true },
		GetNext:        func(int) Client { return fallback },
	}), newMemoryCache())

	for i := 0; i < 2; i++ {
		result, err := client.Generate(context.Background(), GenerateOptions{System: "same"})
		if err != nil {
			t.Fatalf("Generate %d error = %v", i, err)
		}
		if result != fallbackResult {
			t.Fatalf("Generate %d result = %p, want %p", i, result, fallbackResult)
		}
	}
	if primary.callCount() != 1 || fallback.callCount() != 1 {
		t.Fatalf("calls primary=%d fallback=%d, want 1/1", primary.callCount(), fallback.callCount())
	}
}
