package controller

import (
	"errors"
	"testing"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/notifications"
)

func TestBuildOperationSuccessNotification(t *testing.T) {
	pipelineID := "pipeline-123"
	metadata := map[string]interface{}{"operation": "test"}

	tests := []struct {
		name      string
		operation string
		eventType notifications.EventType
		title     string
	}{
		{
			name:      "create",
			operation: constants.OperationCreate,
			eventType: notifications.EventTypePipelineDeployed,
			title:     "Pipeline Deployed Successfully",
		},
		{
			name:      "resume",
			operation: constants.OperationResume,
			eventType: notifications.EventTypePipelineResumed,
			title:     "Pipeline Resumed",
		},
		{
			name:      "stop",
			operation: constants.OperationStop,
			eventType: notifications.EventTypePipelineStopped,
			title:     "Pipeline Stopped",
		},
		{
			name:      "terminate",
			operation: constants.OperationTerminate,
			eventType: notifications.EventTypePipelineStopped,
			title:     "Pipeline Terminated",
		},
		{
			name:      "edit",
			operation: constants.OperationEdit,
			eventType: notifications.EventTypePipelineDeployed,
			title:     "Pipeline Updated Successfully",
		},
		{
			name:      "delete",
			operation: constants.OperationDelete,
			eventType: notifications.EventTypePipelineDeleted,
			title:     "Pipeline Deleted",
		},
		{
			name:      "helm uninstall",
			operation: constants.OperationHelmUninstall,
			eventType: notifications.EventTypePipelineDeleted,
			title:     "Pipeline Uninstalled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildOperationSuccessNotification(tt.operation, pipelineID, metadata)
			if got == nil {
				t.Fatalf("notification is nil")
			}
			if got.PipelineID != pipelineID {
				t.Fatalf("pipeline_id = %q, want %q", got.PipelineID, pipelineID)
			}
			if got.EventType != tt.eventType {
				t.Fatalf("event_type = %q, want %q", got.EventType, tt.eventType)
			}
			if got.Title != tt.title {
				t.Fatalf("title = %q, want %q", got.Title, tt.title)
			}
		})
	}
}

func TestBuildOperationSuccessNotificationUnknownOperation(t *testing.T) {
	got := buildOperationSuccessNotification("unknown", "pipeline-123", nil)
	if got != nil {
		t.Fatalf("expected nil notification for unknown operation")
	}
}

func TestBuildOperationFailureNotification(t *testing.T) {
	pipelineID := "pipeline-123"
	opErr := errors.New("operation failed")
	metadata := map[string]interface{}{"operation": "create"}

	got := buildOperationFailureNotification(constants.OperationCreate, pipelineID, opErr, metadata)
	if got == nil {
		t.Fatalf("notification is nil")
	}
	if got.PipelineID != pipelineID {
		t.Fatalf("pipeline_id = %q, want %q", got.PipelineID, pipelineID)
	}
	if got.EventType != notifications.EventTypePipelineFailed {
		t.Fatalf("event_type = %q, want %q", got.EventType, notifications.EventTypePipelineFailed)
	}
	if got.Severity != notifications.SeverityError {
		t.Fatalf("severity = %q, want %q", got.Severity, notifications.SeverityError)
	}
	if got.Title == "" {
		t.Fatalf("title should not be empty")
	}
	if got.Message == "" {
		t.Fatalf("message should not be empty")
	}
}
