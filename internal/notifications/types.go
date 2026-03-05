package notifications

import "time"

// Severity represents the severity level of a notification
type Severity string

const (
	// SeverityInfo represents an informational notification
	SeverityInfo Severity = "info"
	// SeverityWarning represents a warning notification
	SeverityWarning Severity = "warning"
	// SeverityError represents an error notification
	SeverityError Severity = "error"
	// SeverityCritical represents a critical notification
	SeverityCritical Severity = "critical"
)

// EventType represents the type of event that triggered the notification
type EventType string

const (
	// EventTypePipelineDeployed indicates a pipeline was successfully deployed
	EventTypePipelineDeployed EventType = "pipeline_deployed"
	// EventTypePipelineStopped indicates a pipeline was stopped
	EventTypePipelineStopped EventType = "pipeline_stopped"
	// EventTypePipelineResumed indicates a pipeline was resumed
	EventTypePipelineResumed EventType = "pipeline_resumed"
	// EventTypePipelineDeleted indicates a pipeline was deleted
	EventTypePipelineDeleted EventType = "pipeline_deleted"
	// EventTypePipelineFailed indicates a pipeline failed
	EventTypePipelineFailed EventType = "pipeline_failed"
)

// Metadata contains additional metadata for a notification
type Metadata struct {
	Source       string                 `json:"source"`
	Tags         []string               `json:"tags,omitempty"`
	CustomFields map[string]interface{} `json:"custom_fields,omitempty"`
}

// Notification represents a notification to be sent to the notification service
type Notification struct {
	NotificationID string    `json:"notification_id"`
	PipelineID     string    `json:"pipeline_id"`
	Timestamp      string    `json:"timestamp"`
	Severity       Severity  `json:"severity"`
	EventType      EventType `json:"event_type"`
	Title          string    `json:"title"`
	Message        string    `json:"message"`
	Metadata       Metadata  `json:"metadata"`
}

func (n *Notification) GetTimestamp() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, n.Timestamp)
}

func (n *Notification) SetTimestamp(t time.Time) {
	n.Timestamp = t.UTC().Format(time.RFC3339Nano)
}
