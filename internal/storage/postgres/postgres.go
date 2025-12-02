package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
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

	var pool *pgxpool.Pool

	// Retry connection with exponential backoff using retry.Do
	err = retry.Do(
		func() error {
			// Create connection pool with timeout
			poolCtx, poolCancel := context.WithTimeout(connCtx, PostgresConnectionTimeout)
			newPool, poolErr := pgxpool.NewWithConfig(poolCtx, config)
			poolCancel()

			if poolErr != nil {
				return poolErr
			}

			// Test connection with ping
			pingCtx, pingCancel := context.WithTimeout(connCtx, PostgresConnectionTimeout)
			pingErr := newPool.Ping(pingCtx)
			pingCancel()

			if pingErr != nil {
				// Ping failed, close pool and retry
				newPool.Close()
				return pingErr
			}

			// Connection successful - assign to outer variable
			pool = newPool
			return nil
		},
		retry.Attempts(PostgresConnectionRetries),
		retry.Delay(PostgresInitialRetryDelay),
		retry.MaxDelay(PostgresMaxRetryDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.Context(connCtx),
		retry.OnRetry(func(n uint, err error) {
			logger.Info("retrying connection to PostgreSQL",
				"retry", int(n+1),
				"max_attempts", PostgresConnectionRetries,
				"error", err)
		}),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL after %d retries: %w", PostgresConnectionRetries, err)
	}

	logger.Info("postgres connection established", "max_conns", 25, "min_conns", 5)

	return &PostgresStorage{pool: pool, logger: logger}, nil
}

// Close closes the database connection pool
func (s *PostgresStorage) Close() error {
	s.pool.Close()
	return nil
}
