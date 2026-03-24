package notifications

import (
	"time"

	"github.com/google/uuid"
)

const (
	// DefaultSource is the default source for notifications from the operator
	DefaultSource = "glassflow-operator"
)

func newPipelineNotification(
	pipelineID string,
	severity Severity,
	eventType EventType,
	title string,
	message string,
	tags []string,
	metadata map[string]interface{},
) *Notification {
	now := time.Now().UTC()
	return &Notification{
		NotificationID: uuid.New().String(),
		PipelineID:     pipelineID,
		Timestamp:      now.Format(time.RFC3339Nano),
		Severity:       severity,
		EventType:      eventType,
		Title:          title,
		Message:        message,
		Metadata: Metadata{
			Source:       DefaultSource,
			Tags:         tags,
			CustomFields: metadata,
		},
	}
}

// NewPipelineDeployedNotification creates a notification for a pipeline deployment event.
func NewPipelineDeployedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	notification := newPipelineNotification(
		pipelineID,
		SeverityInfo,
		EventTypePipelineDeployed,
		title,
		message,
		[]string{"deployment"},
		metadata,
	)

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

// NewPipelineStoppedNotification creates a notification for a pipeline stopped event.
func NewPipelineStoppedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	notification := newPipelineNotification(
		pipelineID,
		SeverityWarning,
		EventTypePipelineStopped,
		title,
		message,
		[]string{"deployment"},
		metadata,
	)

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

// NewPipelineResumedNotification creates a notification for a pipeline resumed event.
func NewPipelineResumedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	notification := newPipelineNotification(
		pipelineID,
		SeverityInfo,
		EventTypePipelineResumed,
		title,
		message,
		[]string{"deployment"},
		metadata,
	)

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

// NewPipelineDeletedNotification creates a notification for a pipeline deleted event.
func NewPipelineDeletedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	notification := newPipelineNotification(
		pipelineID,
		SeverityWarning,
		EventTypePipelineDeleted,
		title,
		message,
		[]string{"deployment"},
		metadata,
	)

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

// NewPipelineFailedNotification creates a notification for a pipeline failed event.
func NewPipelineFailedNotification(pipelineID, title, message string, metadata map[string]interface{}) *Notification {
	notification := newPipelineNotification(
		pipelineID,
		SeverityError,
		EventTypePipelineFailed,
		title,
		message,
		[]string{"deployment", "error"},
		metadata,
	)

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
