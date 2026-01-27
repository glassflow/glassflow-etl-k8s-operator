package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
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
	cfg             *Config
	natsClient      *nats.NATSClient
	js              jetstream.JetStream
	streamName      string
	postgresStorage Storage
}

// NewClient creates a new notification client
func NewClient(ctx context.Context, cfg *Config, natsClient *nats.NATSClient, postgresStorage Storage) (Client, error) {
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

	js := natsClient.JetStream()

	// Create or update the notifications stream with the configured settings
	streamConfig := jetstream.StreamConfig{
		Name:      cfg.StreamName,
		Subjects:  []string{cfg.StreamName + ".*"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    cfg.StreamMaxAge,
		MaxBytes:  cfg.StreamMaxBytes,
		Discard:   jetstream.DiscardOld,
	}

	_, err := js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create or update notifications stream: %w", err)
	}

	return &notificationClient{
		cfg:             cfg,
		natsClient:      natsClient,
		js:              js,
		streamName:      cfg.StreamName,
		postgresStorage: postgresStorage,
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

	// Store in PostgreSQL first (if storage is available)
	if c.postgresStorage != nil {
		err := c.postgresStorage.StoreNotification(ctx, notification)
		if err != nil {
			// Log the error but don't fail the operation
			log.Printf("Failed to store notification in PostgreSQL: %v", err)
			// Continue to publish to NATS even if PostgreSQL storage fails
		}
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
		// Log the error but don't fail the operation
		log.Printf("Failed to publish notification to NATS stream %s: %v", c.streamName, err)
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

func (c *noOpClient) Publish(ctx context.Context, notification *Notification) error {
	return ErrNotificationDisabled
}

func (c *noOpClient) IsEnabled() bool {
	return c.enabled
}

func (c *noOpClient) Close() error {
	return nil
}
