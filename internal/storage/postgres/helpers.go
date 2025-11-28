package postgres

import (
	"fmt"

	"github.com/google/uuid"
)

// ErrPipelineNotExists is returned when a pipeline is not found
var ErrPipelineNotExists = fmt.Errorf("pipeline not found")

// parsePipelineID parses a pipeline ID string into a UUID
func parsePipelineID(id string) (uuid.UUID, error) {
	pipelineID, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid pipeline ID format: %w", err)
	}
	return pipelineID, nil
}

// checkRowsAffected checks if any rows were affected and returns ErrPipelineNotExists if none
func checkRowsAffected(rowsAffected int64) error {
	if rowsAffected == 0 {
		return ErrPipelineNotExists
	}
	return nil
}

// handleTransformationIDs handles NULL transformation_ids from database
func handleTransformationIDs(transformationIDsPtr *[]uuid.UUID) []uuid.UUID {
	if transformationIDsPtr != nil {
		return *transformationIDsPtr
	}
	return []uuid.UUID{}
}
