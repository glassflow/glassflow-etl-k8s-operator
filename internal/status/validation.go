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
	// Created status can only transition to Running
	nats.PipelineStatusCreated: {
		nats.PipelineStatusRunning,
	},

	// Running status can transition to Pausing, Stopping, or Terminating
	nats.PipelineStatusRunning: {
		nats.PipelineStatusPausing,
		nats.PipelineStatusStopping,
		nats.PipelineStatusTerminating,
	},

	// Pausing status can only transition to Paused
	nats.PipelineStatusPausing: {
		nats.PipelineStatusPaused,
	},

	// Paused status can transition to Resuming or Stopping
	nats.PipelineStatusPaused: {
		nats.PipelineStatusResuming,
		nats.PipelineStatusStopping,
	},

	// Resuming status can only transition to Running
	nats.PipelineStatusResuming: {
		nats.PipelineStatusRunning,
	},

	// Stopping status can only transition to Stopped
	nats.PipelineStatusStopping: {
		nats.PipelineStatusStopped,
	},

	// Stopped status has no valid transitions (terminal state)
	nats.PipelineStatusStopped: {},

	// Terminating status can only transition to Terminated
	nats.PipelineStatusTerminating: {
		nats.PipelineStatusTerminated,
	},

	// Terminated status has no valid transitions (terminal state)
	nats.PipelineStatusTerminated: {},

	// Failed status has no valid transitions (terminal state)
	nats.PipelineStatusFailed: {},
}

// StatusTransitionDescriptions provides human-readable descriptions for transitions
var StatusTransitionDescriptions = map[string]string{
	"Created->Running":        "Start pipeline execution",
	"Running->Pausing":        "Pause pipeline",
	"Running->Stopping":       "Stop pipeline",
	"Running->Terminating":    "Terminate pipeline",
	"Pausing->Paused":         "Complete pause operation",
	"Paused->Resuming":        "Resume pipeline",
	"Paused->Stopping":        "Stop paused pipeline",
	"Resuming->Running":       "Complete resume operation",
	"Stopping->Stopped":       "Complete stop operation",
	"Terminating->Terminated": "Complete termination operation",
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

	// If we get here, the transition is invalid
	transitionKey := fmt.Sprintf("%s->%s", from, to)
	description, hasDescription := StatusTransitionDescriptions[transitionKey]

	if hasDescription {
		return fmt.Errorf("invalid status transition: %s (attempted: %s)", description, transitionKey)
	}

	return fmt.Errorf("invalid status transition from %s to %s", from, to)
}

// GetValidTransitions returns all valid transitions from a given status
func GetValidTransitions(from nats.PipelineStatus) ([]nats.PipelineStatus, error) {
	validTransitions, exists := StatusValidationMatrix[from]
	if !exists {
		return nil, fmt.Errorf("unknown status: %s", from)
	}

	// Return a copy to prevent external modification
	result := make([]nats.PipelineStatus, len(validTransitions))
	copy(result, validTransitions)
	return result, nil
}

// IsTerminalStatus checks if a status is a terminal state (no further transitions allowed)
func IsTerminalStatus(status nats.PipelineStatus) bool {
	validTransitions, exists := StatusValidationMatrix[status]
	return exists && len(validTransitions) == 0
}

// IsTransitionalStatus checks if a status is transitional (temporary state during operations)
func IsTransitionalStatus(status nats.PipelineStatus) bool {
	transitionalStatuses := []nats.PipelineStatus{
		nats.PipelineStatusPausing,
		nats.PipelineStatusResuming,
		nats.PipelineStatusStopping,
		nats.PipelineStatusTerminating,
	}

	for _, transitional := range transitionalStatuses {
		if status == transitional {
			return true
		}
	}
	return false
}

// GetStatusDescription returns a human-readable description of a status
func GetStatusDescription(status nats.PipelineStatus) string {
	descriptions := map[nats.PipelineStatus]string{
		nats.PipelineStatusCreated:     "Pipeline created and ready to start",
		nats.PipelineStatusRunning:     "Pipeline is actively processing data",
		nats.PipelineStatusPausing:     "Pipeline is being paused",
		nats.PipelineStatusPaused:      "Pipeline is paused and not processing data",
		nats.PipelineStatusResuming:    "Pipeline is being resumed",
		nats.PipelineStatusStopping:    "Pipeline is being stopped",
		nats.PipelineStatusStopped:     "Pipeline has been stopped",
		nats.PipelineStatusTerminating: "Pipeline is being terminated",
		nats.PipelineStatusTerminated:  "Pipeline has been terminated",
		nats.PipelineStatusFailed:      "Pipeline has failed",
	}

	if description, exists := descriptions[status]; exists {
		return description
	}
	return fmt.Sprintf("Unknown status: %s", status)
}

// ValidatePipelineStatusTransition validates a status transition for a pipeline
func ValidatePipelineStatusTransition(currentStatus nats.PipelineStatus, newStatus nats.PipelineStatus) error {
	return ValidateStatusTransition(currentStatus, newStatus)
}

// GetStatusTransitionDescription returns a description for a specific transition
func GetStatusTransitionDescription(from, to nats.PipelineStatus) string {
	transitionKey := fmt.Sprintf("%s->%s", from, to)
	if description, exists := StatusTransitionDescriptions[transitionKey]; exists {
		return description
	}
	return fmt.Sprintf("Transition from %s to %s", from, to)
}
