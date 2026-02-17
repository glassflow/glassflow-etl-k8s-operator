package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/notifications"
	"github.com/google/uuid"
)

// StoreNotification stores a notification in PostgreSQL
// This implements the notifications.Storage interface
func (s *PostgresStorage) StoreNotification(ctx context.Context, notification *notifications.Notification) error {
	// Parse notification_id as UUID
	notificationID, err := uuid.Parse(notification.NotificationID)
	if err != nil {
		return fmt.Errorf("invalid notification_id: %w", err)
	}

	// Parse timestamp
	timestamp, err := notification.GetTimestamp()
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	// Marshal metadata to JSONB
	metadataJSON, err := json.Marshal(notification.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	// Insert notification into database
	_, err = s.pool.Exec(ctx, `
		INSERT INTO notifications (
			notification_id,
			pipeline_id,
			timestamp,
			severity,
			event_type,
			title,
			message,
			metadata
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (notification_id) DO NOTHING
	`,
		notificationID,
		notification.PipelineID,
		timestamp,
		string(notification.Severity),
		string(notification.EventType),
		notification.Title,
		notification.Message,
		metadataJSON,
	)
	if err != nil {
		return fmt.Errorf("insert notification: %w", err)
	}

	s.logger.Info("notification stored in postgres",
		"notification_id", notification.NotificationID,
		"pipeline_id", notification.PipelineID,
		"event_type", notification.EventType)

	return nil
}
