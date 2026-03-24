package notifications

import (
	"context"
	"errors"
	"sync"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/go-logr/logr"
)

var (
	runtimeMu     sync.RWMutex
	runtimeClient Client      = &noOpClient{enabled: false}
	runtimeLogger logr.Logger = logr.Discard().WithName("notifications")
)

// Initialize configures the package-level notification client used across the operator.
func Initialize(ctx context.Context, cfg *Config, natsClient *nats.NATSClient, logger logr.Logger) error {
	client, err := NewClient(ctx, cfg, natsClient, logger)
	if err != nil {
		return err
	}

	runtimeMu.Lock()
	previous := runtimeClient
	runtimeClient = client
	runtimeLogger = logger.WithName("notifications")
	runtimeMu.Unlock()

	if previous != nil {
		if closeErr := previous.Close(); closeErr != nil {
			runtimeLogger.Error(closeErr, "failed to close previous notifications client")
		}
	}

	return nil
}

// Shutdown closes the current package-level notification client and resets it to a no-op client.
func Shutdown() error {
	runtimeMu.Lock()
	previous := runtimeClient
	runtimeClient = &noOpClient{enabled: false}
	runtimeMu.Unlock()

	if previous == nil {
		return nil
	}

	return previous.Close()
}

// IsEnabled returns whether notifications are enabled for the package-level client.
func IsEnabled() bool {
	runtimeMu.RLock()
	client := runtimeClient
	runtimeMu.RUnlock()
	return client != nil && client.IsEnabled()
}

// Publish uses the package-level client to publish a notification.
func Publish(ctx context.Context, notification *Notification) error {
	runtimeMu.RLock()
	client := runtimeClient
	logger := runtimeLogger
	runtimeMu.RUnlock()

	if client == nil {
		return nil
	}

	if err := client.Publish(ctx, notification); err != nil {
		if errors.Is(err, ErrNotificationDisabled) {
			return nil
		}

		logger.Error(err, "failed to publish notification",
			"pipeline_id", notification.PipelineID,
			"event_type", notification.EventType)
		return err
	}

	return nil
}
