package models

import (
	"fmt"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/errs"
)

type PipelineHealth struct {
	OverallStatus PipelineStatus `json:"overall_status"`
}

// PipelineStatus represents the overall status of a pipeline
type PipelineStatus string

// Pipeline status constants
const (
	PipelineStatusCreated     PipelineStatus = "Created"
	PipelineStatusRunning     PipelineStatus = "Running"
	PipelineStatusResuming    PipelineStatus = "Resuming"
	PipelineStatusStopping    PipelineStatus = "Stopping"
	PipelineStatusStopped     PipelineStatus = "Stopped"
	PipelineStatusTerminating PipelineStatus = "Terminating"
	PipelineStatusFailed      PipelineStatus = "Failed"
)

var StatusValidationMatrix = map[PipelineStatus][]PipelineStatus{
	// All non terminal states can transition to Terminating / Failed

	// Created status can only transition to Running
	PipelineStatusCreated: {
		PipelineStatusRunning,
		PipelineStatusTerminating,
		PipelineStatusFailed,
	},

	// Running status can transition to Stopping or Terminating
	PipelineStatusRunning: {
		PipelineStatusStopping,
		PipelineStatusTerminating,
		PipelineStatusFailed,
	},

	// Resuming status can only transition to Running or Terminating
	PipelineStatusResuming: {
		PipelineStatusRunning,
		PipelineStatusTerminating,
		PipelineStatusFailed,
	},

	// Stopping status can only transition to Stopped or Terminating
	PipelineStatusStopping: {
		PipelineStatusStopped,
		PipelineStatusTerminating,
		PipelineStatusFailed,
	},

	// Stopped status allows resume and can transition to failed
	PipelineStatusStopped: {
		PipelineStatusResuming,
		PipelineStatusFailed,
	},

	// Terminating status can only transition to Stopped
	PipelineStatusTerminating: {
		PipelineStatusStopped,
		PipelineStatusFailed,
	},

	// Failed status can be deleted, edited or resumed
	PipelineStatusFailed: {
		PipelineStatusResuming,
	},
}

func ValidateStatusTransition(from, to PipelineStatus) error {
	// Check if the transition is valid according to the matrix
	validTransitions, exists := StatusValidationMatrix[from]
	if !exists {
		return fmt.Errorf("%w: %s", errs.ErrUnknownStatus, from)
	}

	// Check if the target status is in the list of valid transitions
	for _, validStatus := range validTransitions {
		if validStatus == to {
			return nil
		}
	}

	// If we get here, the transition is invalid
	return fmt.Errorf("%w: from: %s, to %s", errs.ErrPipelineInvalidStatusTransition, from, to)
}

// ValidatePipelineOperation validates a pipeline operation and returns specific errors
func ValidatePipelineOperation(currentPipelineStatus PipelineStatus, toStatus PipelineStatus) error {
	if currentPipelineStatus == toStatus {
		return nil
	}

	// Check if pipeline is in a transitional state (but allow terminating in case of failures)
	if IsTransitionalStatus(currentPipelineStatus) && toStatus != PipelineStatusTerminating {
		return fmt.Errorf("%w, current status: %s", errs.ErrPipelineAlreadyInTransition, currentPipelineStatus)
	}

	// Validate the transition
	return ValidateStatusTransition(currentPipelineStatus, toStatus)
}

// IsTransitionalStatus checks if a status is transitional (temporary state during operations)
func IsTransitionalStatus(status PipelineStatus) bool {
	transitionalStatuses := []PipelineStatus{
		PipelineStatusResuming,
		PipelineStatusStopping,
		PipelineStatusTerminating,
	}

	for _, transitional := range transitionalStatuses {
		if status == transitional {
			return true
		}
	}
	return false
}
