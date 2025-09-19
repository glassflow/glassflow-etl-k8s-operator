package status

import (
	"testing"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
)

func TestValidateStatusTransition(t *testing.T) {
	tests := []struct {
		name        string
		from        nats.PipelineStatus
		to          nats.PipelineStatus
		expectError bool
		errorMsg    string
	}{
		// Valid transitions
		{
			name:        "Created to Running",
			from:        nats.PipelineStatusCreated,
			to:          nats.PipelineStatusRunning,
			expectError: false,
		},
		{
			name:        "Running to Pausing",
			from:        nats.PipelineStatusRunning,
			to:          nats.PipelineStatusPausing,
			expectError: false,
		},
		{
			name:        "Running to Stopping",
			from:        nats.PipelineStatusRunning,
			to:          nats.PipelineStatusStopping,
			expectError: false,
		},
		{
			name:        "Running to Terminating",
			from:        nats.PipelineStatusRunning,
			to:          nats.PipelineStatusTerminating,
			expectError: false,
		},
		{
			name:        "Pausing to Paused",
			from:        nats.PipelineStatusPausing,
			to:          nats.PipelineStatusPaused,
			expectError: false,
		},
		{
			name:        "Paused to Resuming",
			from:        nats.PipelineStatusPaused,
			to:          nats.PipelineStatusResuming,
			expectError: false,
		},
		{
			name:        "Paused to Stopping",
			from:        nats.PipelineStatusPaused,
			to:          nats.PipelineStatusStopping,
			expectError: false,
		},
		{
			name:        "Paused to Terminating",
			from:        nats.PipelineStatusPaused,
			to:          nats.PipelineStatusTerminating,
			expectError: false,
		},
		{
			name:        "Resuming to Running",
			from:        nats.PipelineStatusResuming,
			to:          nats.PipelineStatusRunning,
			expectError: false,
		},
		{
			name:        "Stopping to Stopped",
			from:        nats.PipelineStatusStopping,
			to:          nats.PipelineStatusStopped,
			expectError: false,
		},
		{
			name:        "Terminating to Terminated",
			from:        nats.PipelineStatusTerminating,
			to:          nats.PipelineStatusTerminated,
			expectError: false,
		},

		// Invalid transitions
		{
			name:        "Created to Paused (invalid)",
			from:        nats.PipelineStatusCreated,
			to:          nats.PipelineStatusPaused,
			expectError: true,
		},
		{
			name:        "Running to Running (invalid)",
			from:        nats.PipelineStatusRunning,
			to:          nats.PipelineStatusRunning,
			expectError: true,
		},
		{
			name:        "Paused to Paused (invalid)",
			from:        nats.PipelineStatusPaused,
			to:          nats.PipelineStatusPaused,
			expectError: true,
		},
		{
			name:        "Stopped to Running (invalid - terminal state)",
			from:        nats.PipelineStatusStopped,
			to:          nats.PipelineStatusRunning,
			expectError: true,
		},
		{
			name:        "Terminated to Running (invalid - terminal state)",
			from:        nats.PipelineStatusTerminated,
			to:          nats.PipelineStatusRunning,
			expectError: true,
		},
		{
			name:        "Failed to Running (invalid - terminal state)",
			from:        nats.PipelineStatusFailed,
			to:          nats.PipelineStatusRunning,
			expectError: true,
		},
		{
			name:        "Running to Resuming (invalid)",
			from:        nats.PipelineStatusRunning,
			to:          nats.PipelineStatusResuming,
			expectError: true,
		},
		{
			name:        "Paused to Pausing (invalid)",
			from:        nats.PipelineStatusPaused,
			to:          nats.PipelineStatusPausing,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStatusTransition(tt.from, tt.to)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestStatusValidationMatrixCompleteness ensures all status constants are covered
func TestStatusValidationMatrixCompleteness(t *testing.T) {
	allStatuses := []nats.PipelineStatus{
		nats.PipelineStatusCreated,
		nats.PipelineStatusRunning,
		nats.PipelineStatusPausing,
		nats.PipelineStatusPaused,
		nats.PipelineStatusResuming,
		nats.PipelineStatusStopping,
		nats.PipelineStatusStopped,
		nats.PipelineStatusTerminating,
		nats.PipelineStatusTerminated,
		nats.PipelineStatusFailed,
	}

	for _, status := range allStatuses {
		_, exists := StatusValidationMatrix[status]
		if !exists {
			t.Errorf("status %s is not defined in StatusValidationMatrix", status)
		}
	}
}
