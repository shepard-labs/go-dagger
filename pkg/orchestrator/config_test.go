package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestREQPERSIST001NewOrchestratorFailsWhenPgxPoolCannotPing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := NewOrchestrator[RunState](ctx, Config{PostgresDSN: "postgres://bad:bad@127.0.0.1:1/bad", PersistenceTimeout: 50 * time.Millisecond})
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("expected ErrPersistence, got %v", err)
	}
	if strings.Contains(err.Error(), "bad:bad") || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("error leaked dsn: %v", err)
	}
}
