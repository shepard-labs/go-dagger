package persistence

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shepard-labs/go-dagger/internal/apperrors"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Config controls Postgres connection and retry behavior.
type Config struct {
	DSN string
	// Schema sets the connection's search_path so unqualified table names
	// (dag_runs, task_runs, task_events, task_logs) resolve against this
	// schema. Empty leaves the server default (typically "public").
	Schema             string
	PoolSize           int32
	PersistenceTimeout time.Duration
	WriteRetries       int
	RetryBaseDelay     time.Duration
	PingRetries        int
	PingRetryDelay     time.Duration
}

// Postgres owns the pgx pool and persistence retry settings.
type Postgres struct {
	Pool   *pgxpool.Pool
	config Config
}

// NewPostgres opens a pgx pool and verifies connectivity with retry.
func NewPostgres(ctx context.Context, config Config) (*Postgres, error) {
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, persistenceError("parse postgres config", err, config.DSN)
	}
	if config.Schema != "" {
		poolConfig.ConnConfig.RuntimeParams["search_path"] = config.Schema
	}
	if config.PoolSize > 0 {
		poolConfig.MaxConns = config.PoolSize
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, persistenceError("create postgres pool", err, config.DSN)
	}
	postgres := &Postgres{Pool: pool, config: normalizeConfig(config)}
	if err := postgres.pingWithRetry(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return postgres, nil
}

// NewPostgresFromPool wraps an existing pgx pool.
func NewPostgresFromPool(pool *pgxpool.Pool, config Config) *Postgres {
	return &Postgres{Pool: pool, config: normalizeConfig(config)}
}

// Close closes the underlying pgx pool.
func (p *Postgres) Close() {
	if p != nil && p.Pool != nil {
		p.Pool.Close()
	}
}

// ApplyMigrations executes embedded SQL migrations in filename order.
func (p *Postgres) ApplyMigrations(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return persistenceError("read migrations", err, "")
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		sqlBytes, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return persistenceError("read migration "+name, err, "")
		}
		if _, err := p.Pool.Exec(ctx, string(sqlBytes)); err != nil {
			return persistenceError("apply migration "+name, err, "")
		}
	}
	return nil
}

// WithWriteRetry runs fn with persistence timeout and retry wrapping.
func (p *Postgres) WithWriteRetry(ctx context.Context, operation string, fn func(context.Context) error) error {
	return p.withWriteRetry(ctx, operation, fn)
}

func (p *Postgres) withWriteRetry(ctx context.Context, operation string, fn func(context.Context) error) error {
	config := normalizeConfig(p.config)
	var lastErr error
	for attempt := 0; attempt <= config.WriteRetries; attempt++ {
		writeCtx, cancel := context.WithTimeout(ctx, config.PersistenceTimeout)
		err := fn(writeCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == config.WriteRetries {
			break
		}
		select {
		case <-ctx.Done():
			return persistenceError(operation, ctx.Err(), config.DSN)
		case <-time.After(config.RetryBaseDelay * time.Duration(1<<attempt)):
		}
	}
	return persistenceError(operation, lastErr, config.DSN)
}

func (p *Postgres) pingWithRetry(ctx context.Context) error {
	config := normalizeConfig(p.config)
	var lastErr error
	for attempt := 0; attempt <= config.PingRetries; attempt++ {
		if err := p.Pool.Ping(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == config.PingRetries {
			break
		}
		select {
		case <-ctx.Done():
			return persistenceError("ping postgres", ctx.Err(), config.DSN)
		case <-time.After(config.PingRetryDelay):
		}
	}
	return persistenceError("ping postgres", lastErr, config.DSN)
}

func normalizeConfig(config Config) Config {
	if config.PersistenceTimeout == 0 {
		config.PersistenceTimeout = 10 * time.Second
	}
	if config.WriteRetries == 0 {
		config.WriteRetries = 3
	}
	if config.RetryBaseDelay == 0 {
		config.RetryBaseDelay = 50 * time.Millisecond
	}
	if config.PingRetries == 0 {
		config.PingRetries = 3
	}
	if config.PingRetryDelay == 0 {
		config.PingRetryDelay = 50 * time.Millisecond
	}
	return config
}

func persistenceError(operation string, err error, dsn string) error {
	if err == nil {
		err = errors.New("unknown persistence failure")
	}
	message := redactDSN(err.Error(), dsn)
	return fmt.Errorf("%w: %s: %s", apperrors.ErrPersistence, operation, message)
}

// RedactDSN removes Postgres connection strings and credentials from input.
func RedactDSN(input string) string {
	return redactDSN(input, input)
}

func redactDSN(input, dsn string) string {
	redacted := input
	if dsn != "" {
		redacted = strings.ReplaceAll(redacted, dsn, "[redacted-postgres-dsn]")
		if parsed, err := url.Parse(dsn); err == nil && parsed.User != nil {
			if password, ok := parsed.User.Password(); ok && password != "" {
				redacted = strings.ReplaceAll(redacted, password, "[redacted]")
			}
			username := parsed.User.Username()
			if username != "" {
				redacted = strings.ReplaceAll(redacted, username, "[redacted]")
			}
		}
	}
	for _, marker := range []string{"postgres://", "postgresql://"} {
		if strings.Contains(redacted, marker) {
			redacted = "[redacted-postgres-dsn]"
		}
	}
	return redacted
}
