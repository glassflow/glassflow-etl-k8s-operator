package postgres

import (
	"fmt"
)

// ErrPipelineNotExists is returned when a pipeline is not found
var ErrPipelineNotExists = fmt.Errorf("pipeline not found")

// checkRowsAffected checks if any rows were affected and returns ErrPipelineNotExists if none
func checkRowsAffected(rowsAffected int64) error {
	if rowsAffected == 0 {
		return ErrPipelineNotExists
	}
	return nil
}
