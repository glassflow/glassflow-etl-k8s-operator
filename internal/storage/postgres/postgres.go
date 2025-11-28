package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQL connection retry constants - similar to NATS
const (
	PostgresConnectionTimeout = 1 * time.Minute
	PostgresConnectionRetries = 12
	PostgresInitialRetryDelay = 1 * time.Second
	PostgresMaxRetryDelay     = 30 * time.Second
	PostgresMaxConnectionWait = 2 * time.Minute
)

// PostgresStorage implements pipeline storage using PostgreSQL
type PostgresStorage struct {
	pool   *pgxpool.Pool
	logger logr.Logger
}

// NewPostgres creates a new PostgresStorage instance with retry logic
func NewPostgres(ctx context.Context, dsn string, logger logr.Logger) (*PostgresStorage, error) {
	connCtx, cancel := context.WithTimeout(ctx, PostgresMaxConnectionWait)
	defer cancel()

	retryDelay := PostgresInitialRetryDelay
	var pool *pgxpool.Pool
	var err error

	// Parse config first (no retry needed for this)
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		logger.Error(err, "failed to parse postgres config")
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	// Configure connection pool
	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 5 * time.Minute

	// Retry connection with exponential backoff
	for i := range PostgresConnectionRetries {
		select {
		case <-connCtx.Done():
			return nil, fmt.Errorf("timeout after %v waiting to connect to PostgreSQL", PostgresMaxConnectionWait)
		default:
		}

		// Create connection pool with timeout
		poolCtx, poolCancel := context.WithTimeout(connCtx, PostgresConnectionTimeout)
		pool, err = pgxpool.NewWithConfig(poolCtx, config)
		poolCancel()

		if err == nil {
			// Test connection with ping
			pingCtx, pingCancel := context.WithTimeout(connCtx, PostgresConnectionTimeout)
			err = pool.Ping(pingCtx)
			pingCancel()

			if err == nil {
				// Connection successful
				break
			}
			// Ping failed, close pool and retry
			pool.Close()
		}

		if i < PostgresConnectionRetries-1 {
			select {
			case <-time.After(retryDelay):
				logger.Info("retrying connection to PostgreSQL", "retry", i+1, "delay", retryDelay)
				// Continue with retry
			case <-connCtx.Done():
				return nil, fmt.Errorf("timeout during retry delay for PostgreSQL: %w", connCtx.Err())
			}
			// Exponential backoff
			retryDelay = minDuration(time.Duration(float64(retryDelay)*1.5), PostgresMaxRetryDelay)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL after %d retries: %w", PostgresConnectionRetries, err)
	}

	logger.Info("postgres connection established", "max_conns", 25, "min_conns", 5)

	return &PostgresStorage{pool: pool, logger: logger}, nil
}

// minDuration returns the minimum of two durations
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// Close closes the database connection pool
func (s *PostgresStorage) Close() error {
	s.pool.Close()
	return nil
}
