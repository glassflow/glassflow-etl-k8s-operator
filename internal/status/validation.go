package status

import (
	"fmt"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
)

// StatusTransition represents a valid transition from one status to another
type StatusTransition struct {
	From   nats.PipelineStatus
	To     nats.PipelineStatus
	Action string // Description of the action that triggers this transition
}

// StatusValidationMatrix defines all valid pipeline status transitions
var StatusValidationMatrix = map[nats.PipelineStatus][]nats.PipelineStatus{

	nats.PipelineStatusCreated: {
		nats.PipelineStatusRunning,
		nats.PipelineStatusTerminating,
		nats.PipelineStatusFailed,
	},

	nats.PipelineStatusRunning: {
		nats.PipelineStatusPausing,
		nats.PipelineStatusStopping,
		nats.PipelineStatusTerminating,
		nats.PipelineStatusFailed,
	},

	nats.PipelineStatusPausing: {
		nats.PipelineStatusPaused,
		nats.PipelineStatusTerminating,
		nats.PipelineStatusFailed,
	},

	nats.PipelineStatusPaused: {
		nats.PipelineStatusResuming,
		nats.PipelineStatusStopping,
		nats.PipelineStatusTerminating,
		nats.PipelineStatusFailed,
	},

	nats.PipelineStatusResuming: {
		nats.PipelineStatusRunning,
		nats.PipelineStatusTerminating,
		nats.PipelineStatusFailed,
	},

	nats.PipelineStatusStopping: {
		nats.PipelineStatusStopped,
		nats.PipelineStatusTerminating,
		nats.PipelineStatusFailed,
	},

	nats.PipelineStatusStopped: {},

	nats.PipelineStatusTerminating: {
		nats.PipelineStatusTerminated,
		nats.PipelineStatusFailed,
	},

	nats.PipelineStatusTerminated: {},

	nats.PipelineStatusFailed: {},
}

// ValidateStatusTransition checks if a transition from one status to another is valid
func ValidateStatusTransition(from, to nats.PipelineStatus) error {
	// Check if the transition is valid according to the matrix
	validTransitions, exists := StatusValidationMatrix[from]
	if !exists {
		return fmt.Errorf("unknown source status: %s", from)
	}

	// Check if the target status is in the list of valid transitions
	for _, validStatus := range validTransitions {
		if validStatus == to {
			return nil
		}
	}

	return fmt.Errorf("invalid status transition from %s to %s", from, to)
}
