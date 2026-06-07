package orchestrator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type advisoryLock struct {
	conn *pgxpool.Conn
	key1 int32
	key2 int32
}

func uuidAdvisoryLockKeys(id uuid.UUID) (int32, int32) {
	left := binary.BigEndian.Uint32(id[0:4]) ^ binary.BigEndian.Uint32(id[8:12])
	right := binary.BigEndian.Uint32(id[4:8]) ^ binary.BigEndian.Uint32(id[12:16])
	return int32(left), int32(right)
}

func acquireRunAdvisoryLock(ctx context.Context, pool *pgxpool.Pool, runID uuid.UUID) (*advisoryLock, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: postgres pool is not configured", ErrPersistence)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: acquire advisory lock connection: %v", ErrPersistence, err)
	}
	key1, key2 := uuidAdvisoryLockKeys(runID)
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, $2)`, key1, key2).Scan(&locked); err != nil {
		conn.Release()
		return nil, fmt.Errorf("%w: acquire advisory lock: %v", ErrPersistence, err)
	}
	if !locked {
		conn.Release()
		return nil, fmt.Errorf("%w: run %s", ErrRunLocked, runID)
	}
	return &advisoryLock{conn: conn, key1: key1, key2: key2}, nil
}

func (l *advisoryLock) release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	defer l.conn.Release()
	var unlocked bool
	if err := l.conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1, $2)`, l.key1, l.key2).Scan(&unlocked); err != nil {
		return fmt.Errorf("%w: release advisory lock: %v", ErrPersistence, err)
	}
	if !unlocked {
		return fmt.Errorf("%w: advisory lock was not held", ErrPersistence)
	}
	return nil
}

func joinWithReleaseError(primary, release error) error {
	if primary == nil {
		return release
	}
	if release == nil {
		return primary
	}
	return errors.Join(primary, release)
}
