package notifications

import (
	"time"

	"github.com/google/uuid"
)

const (
	// DefaultSource is the default source for notifications from the operator
	DefaultSource = "glassflow-operator"
)

// SendPipelineDeployedNotification creates a notification for a pipeline deployment event
func SendPipelineDeployedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	now := time.Now().UTC()
	notification := &Notification{
		NotificationID: uuid.New().String(),
		PipelineID:     pipelineID,
		Timestamp:      now.Format(time.RFC3339Nano),
		Severity:       SeverityInfo,
		EventType:      EventTypePipelineDeployed,
		Title:          title,
		Message:        message,
		Metadata: Metadata{
			Source:       DefaultSource,
			Tags:         []string{"deployment"},
			CustomFields: metadata,
		},
	}

	// Set default title if not provided
	if title == "" {
		notification.Title = "Pipeline Deployed Successfully"
	}

	// Set default message if not provided
	if message == "" {
		notification.Message = "Pipeline has been deployed successfully"
	}

	return notification
}

// SendPipelineStoppedNotification creates a notification for a pipeline stopped event
func SendPipelineStoppedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	now := time.Now().UTC()
	notification := &Notification{
		NotificationID: uuid.New().String(),
		PipelineID:     pipelineID,
		Timestamp:      now.Format(time.RFC3339Nano),
		Severity:       SeverityWarning,
		EventType:      EventTypePipelineStopped,
		Title:          title,
		Message:        message,
		Metadata: Metadata{
			Source:       DefaultSource,
			Tags:         []string{"deployment"},
			CustomFields: metadata,
		},
	}

	// Set default title if not provided
	if title == "" {
		notification.Title = "Pipeline Stopped"
	}

	// Set default message if not provided
	if message == "" {
		notification.Message = "Pipeline has been stopped"
	}

	return notification
}

// SendPipelineResumedNotification creates a notification for a pipeline resumed event
func SendPipelineResumedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	now := time.Now().UTC()
	notification := &Notification{
		NotificationID: uuid.New().String(),
		PipelineID:     pipelineID,
		Timestamp:      now.Format(time.RFC3339Nano),
		Severity:       SeverityInfo,
		EventType:      EventTypePipelineResumed,
		Title:          title,
		Message:        message,
		Metadata: Metadata{
			Source:       DefaultSource,
			Tags:         []string{"deployment"},
			CustomFields: metadata,
		},
	}

	// Set default title if not provided
	if title == "" {
		notification.Title = "Pipeline Resumed"
	}

	// Set default message if not provided
	if message == "" {
		notification.Message = "Pipeline has been resumed"
	}

	return notification
}

// SendPipelineDeletedNotification creates a notification for a pipeline deleted event
func SendPipelineDeletedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	now := time.Now().UTC()
	notification := &Notification{
		NotificationID: uuid.New().String(),
		PipelineID:     pipelineID,
		Timestamp:      now.Format(time.RFC3339Nano),
		Severity:       SeverityWarning,
		EventType:      EventTypePipelineDeleted,
		Title:          title,
		Message:        message,
		Metadata: Metadata{
			Source:       DefaultSource,
			Tags:         []string{"deployment"},
			CustomFields: metadata,
		},
	}

	// Set default title if not provided
	if title == "" {
		notification.Title = "Pipeline Deleted"
	}

	// Set default message if not provided
	if message == "" {
		notification.Message = "Pipeline has been deleted"
	}

	return notification
}

// SendPipelineFailedNotification creates a notification for a pipeline failed event
func SendPipelineFailedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	now := time.Now().UTC()
	notification := &Notification{
		NotificationID: uuid.New().String(),
		PipelineID:     pipelineID,
		Timestamp:      now.Format(time.RFC3339Nano),
		Severity:       SeverityError,
		EventType:      EventTypePipelineFailed,
		Title:          title,
		Message:        message,
		Metadata: Metadata{
			Source:       DefaultSource,
			Tags:         []string{"deployment", "error"},
			CustomFields: metadata,
		},
	}

	// Set default title if not provided
	if title == "" {
		notification.Title = "Pipeline Failed"
	}

	// Set default message if not provided
	if message == "" {
		notification.Message = "Pipeline has failed"
	}

	return notification
}
