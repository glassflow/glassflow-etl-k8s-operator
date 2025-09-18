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

func TestGetValidTransitions(t *testing.T) {
	tests := []struct {
		name           string
		from           nats.PipelineStatus
		expectedCount  int
		expectedStatus []nats.PipelineStatus
		expectError    bool
	}{
		{
			name:          "Created status transitions",
			from:          nats.PipelineStatusCreated,
			expectedCount: 1,
			expectedStatus: []nats.PipelineStatus{
				nats.PipelineStatusRunning,
			},
			expectError: false,
		},
		{
			name:          "Running status transitions",
			from:          nats.PipelineStatusRunning,
			expectedCount: 3,
			expectedStatus: []nats.PipelineStatus{
				nats.PipelineStatusPausing,
				nats.PipelineStatusStopping,
				nats.PipelineStatusTerminating,
			},
			expectError: false,
		},
		{
			name:          "Paused status transitions",
			from:          nats.PipelineStatusPaused,
			expectedCount: 2,
			expectedStatus: []nats.PipelineStatus{
				nats.PipelineStatusResuming,
				nats.PipelineStatusStopping,
			},
			expectError: false,
		},
		{
			name:           "Stopped status transitions (terminal)",
			from:           nats.PipelineStatusStopped,
			expectedCount:  0,
			expectedStatus: []nats.PipelineStatus{},
			expectError:    false,
		},
		{
			name:        "Unknown status",
			from:        nats.PipelineStatus("Unknown"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transitions, err := GetValidTransitions(tt.from)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(transitions) != tt.expectedCount {
				t.Errorf("expected %d transitions, got %d", tt.expectedCount, len(transitions))
			}

			// Check that all expected transitions are present
			for _, expectedStatus := range tt.expectedStatus {
				found := false
				for _, transition := range transitions {
					if transition == expectedStatus {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected transition to %s not found", expectedStatus)
				}
			}
		})
	}
}

func TestIsTerminalStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   nats.PipelineStatus
		expected bool
	}{
		{
			name:     "Stopped is terminal",
			status:   nats.PipelineStatusStopped,
			expected: true,
		},
		{
			name:     "Terminated is terminal",
			status:   nats.PipelineStatusTerminated,
			expected: true,
		},
		{
			name:     "Failed is terminal",
			status:   nats.PipelineStatusFailed,
			expected: true,
		},
		{
			name:     "Running is not terminal",
			status:   nats.PipelineStatusRunning,
			expected: false,
		},
		{
			name:     "Paused is not terminal",
			status:   nats.PipelineStatusPaused,
			expected: false,
		},
		{
			name:     "Created is not terminal",
			status:   nats.PipelineStatusCreated,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTerminalStatus(tt.status)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIsTransitionalStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   nats.PipelineStatus
		expected bool
	}{
		{
			name:     "Pausing is transitional",
			status:   nats.PipelineStatusPausing,
			expected: true,
		},
		{
			name:     "Resuming is transitional",
			status:   nats.PipelineStatusResuming,
			expected: true,
		},
		{
			name:     "Stopping is transitional",
			status:   nats.PipelineStatusStopping,
			expected: true,
		},
		{
			name:     "Terminating is transitional",
			status:   nats.PipelineStatusTerminating,
			expected: true,
		},
		{
			name:     "Running is not transitional",
			status:   nats.PipelineStatusRunning,
			expected: false,
		},
		{
			name:     "Paused is not transitional",
			status:   nats.PipelineStatusPaused,
			expected: false,
		},
		{
			name:     "Created is not transitional",
			status:   nats.PipelineStatusCreated,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTransitionalStatus(tt.status)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestGetStatusDescription(t *testing.T) {
	tests := []struct {
		name     string
		status   nats.PipelineStatus
		expected string
	}{
		{
			name:     "Created status description",
			status:   nats.PipelineStatusCreated,
			expected: "Pipeline created and ready to start",
		},
		{
			name:     "Running status description",
			status:   nats.PipelineStatusRunning,
			expected: "Pipeline is actively processing data",
		},
		{
			name:     "Paused status description",
			status:   nats.PipelineStatusPaused,
			expected: "Pipeline is paused and not processing data",
		},
		{
			name:     "Unknown status description",
			status:   nats.PipelineStatus("Unknown"),
			expected: "Unknown status: Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetStatusDescription(tt.status)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestValidatePipelineStatusTransition(t *testing.T) {
	tests := []struct {
		name          string
		currentStatus nats.PipelineStatus
		newStatus     nats.PipelineStatus
		expectError   bool
	}{
		{
			name:          "Valid transition from Created to Running",
			currentStatus: nats.PipelineStatusCreated,
			newStatus:     nats.PipelineStatusRunning,
			expectError:   false,
		},
		{
			name:          "Invalid transition from Running to Paused",
			currentStatus: nats.PipelineStatusRunning,
			newStatus:     nats.PipelineStatusPaused,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePipelineStatusTransition(tt.currentStatus, tt.newStatus)

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

func TestGetStatusTransitionDescription(t *testing.T) {
	tests := []struct {
		name     string
		from     nats.PipelineStatus
		to       nats.PipelineStatus
		expected string
	}{
		{
			name:     "Known transition",
			from:     nats.PipelineStatusCreated,
			to:       nats.PipelineStatusRunning,
			expected: "Start pipeline execution",
		},
		{
			name:     "Unknown transition",
			from:     nats.PipelineStatusRunning,
			to:       nats.PipelineStatusPaused,
			expected: "Transition from Running to Paused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetStatusTransitionDescription(tt.from, tt.to)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
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
