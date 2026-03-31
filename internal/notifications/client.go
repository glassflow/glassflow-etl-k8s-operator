package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"
)

// Client is the interface for publishing notifications
type Client interface {
	// Publish publishes a notification to the NATS stream
	Publish(ctx context.Context, notification *Notification) error
	// IsEnabled returns whether notifications are enabled
	IsEnabled() bool
	// Close closes the notification client and releases resources
	Close() error
}

// notificationClient is the implementation of the Client interface
type notificationClient struct {
	cfg        *Config
	js         jetstream.JetStream
	streamName string
	logger     logr.Logger
}

// NewClient creates a new notification client
func NewClient(ctx context.Context, cfg *Config, natsClient *nats.Client, logger logr.Logger) (Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// If notifications are disabled, return a no-op client
	if !cfg.Enabled {
		return &noOpClient{enabled: false}, nil
	}

	if natsClient == nil {
		return nil, fmt.Errorf("nats client is required when notifications are enabled")
	}

	js := natsClient.JetStream()
	streamName := DefaultStreamName

	// Avoid overriding stream settings owned by the notifier service.
	if _, err := js.Stream(ctx, streamName); err != nil {
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("failed to check notifications stream: %w", err)
		}

		_, createErr := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamName + ".*"},
			Storage:  jetstream.FileStorage,
		})
		if createErr != nil {
			return nil, fmt.Errorf("failed to create notifications stream: %w", createErr)
		}
	}

	return &notificationClient{
		cfg:        cfg,
		js:         js,
		streamName: streamName,
		logger:     logger.WithName("client"),
	}, nil
}

// Publish publishes a notification to the NATS stream
func (c *notificationClient) Publish(ctx context.Context, notification *Notification) error {
	if !c.cfg.Enabled {
		return ErrNotificationDisabled
	}

	if err := c.validateNotification(notification); err != nil {
		return fmt.Errorf("invalid notification: %w", err)
	}

	// Serialize notification to JSON
	payload, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	// Publish to the stream
	// Use the stream name as the subject (NATS will route it to the stream)
	subject := c.streamName + ".notifications"
	_, err = c.js.Publish(ctx, subject, payload)
	if err != nil {
		c.logger.Error(err, "failed to publish notification",
			"stream", c.streamName,
			"subject", subject,
			"pipeline_id", notification.PipelineID,
			"event_type", notification.EventType)
		return fmt.Errorf("failed to publish notification: %w", err)
	}

	return nil
}

// IsEnabled returns whether notifications are enabled
func (c *notificationClient) IsEnabled() bool {
	return c.cfg.Enabled
}

// Close closes the notification client
// Note: We don't close the NATS client as it may be shared with other components
func (c *notificationClient) Close() error {
	// No cleanup needed as we're using the shared NATS client
	return nil
}

// validateNotification validates a notification before publishing
func (c *notificationClient) validateNotification(n *Notification) error {
	if n == nil {
		return ErrInvalidNotification{Reason: "notification cannot be nil"}
	}
	if n.NotificationID == "" {
		return ErrInvalidNotification{Reason: "notification_id is required"}
	}
	if n.PipelineID == "" {
		return ErrInvalidNotification{Reason: "pipeline_id is required"}
	}
	if n.Timestamp == "" {
		return ErrInvalidNotification{Reason: "timestamp is required"}
	}
	if n.EventType == "" {
		return ErrInvalidNotification{Reason: "event_type is required"}
	}
	if n.Title == "" {
		return ErrInvalidNotification{Reason: "title is required"}
	}
	if n.Message == "" {
		return ErrInvalidNotification{Reason: "message is required"}
	}
	return nil
}

// noOpClient is a no-op implementation used when notifications are disabled
type noOpClient struct {
	enabled bool
}

func (c *noOpClient) Publish(_ context.Context, _ *Notification) error {
	return ErrNotificationDisabled
}

func (c *noOpClient) IsEnabled() bool {
	return c.enabled
}

func (c *noOpClient) Close() error {
	return nil
}
