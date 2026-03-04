package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"
)

// UpdatePipelineStatus updates the pipeline status and creates a history event
func (s *PostgresStorage) UpdatePipelineStatus(ctx context.Context, pipelineID string, status models.PipelineStatus, errors []string) error {

	// Begin transaction
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if err := tx.Rollback(ctx); err != nil {
				s.logger.Error(err, "failed to rollback transaction")
			}
		}
	}()

	// Update pipeline status
	statusStr := string(status)
	now := time.Now().UTC()

	commandTag, err := tx.Exec(ctx, `
		UPDATE pipelines
		SET status = $1, updated_at = $2
		WHERE id = $3
	`, statusStr, now, pipelineID)
	if err != nil {
		return fmt.Errorf("update pipeline status: %w", err)
	}

	if err := checkRowsAffected(commandTag.RowsAffected()); err != nil {
		return err
	}

	// Create history event with type "status" or "error" (depending on errors parameter)
	err = s.insertPipelineHistoryEvent(ctx, tx, pipelineID, statusStr, errors)
	if err != nil {
		// Log but don't fail the status update
		s.logger.Info("failed to insert pipeline history event", "pipeline_id", pipelineID, "error", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	committed = true

	s.logger.Info("pipeline status updated", "pipeline_id", pipelineID, "status", statusStr)

	return nil
}

// HistoryEntry represents a pipeline history entry
type HistoryEntry struct {
	Type     string          `json:"type"`     // "history", "error", or "status"
	Pipeline json.RawMessage `json:"pipeline"` // Full pipeline JSON
	Errors   []string        `json:"errors"`   // Array of error messages (for error type)
}

// insertPipelineHistoryEvent inserts a pipeline history event
func (s *PostgresStorage) insertPipelineHistoryEvent(ctx context.Context, tx pgx.Tx, pipelineID string, status string, errors []string) error {
	// Determine event type: "error" if errors present, otherwise "status"
	eventType := "status"
	if len(errors) > 0 {
		eventType = "error"
	}

	// Build minimal pipeline JSON with just status (operator doesn't have full pipeline config readily available)
	// The status can be extracted from pipeline_history->event->'pipeline'->'status'->'overall_status' in queries
	pipelineJSON := json.RawMessage(fmt.Sprintf(`{"status":{"overall_status":"%s"}}`, status))

	// Build event object matching HistoryEntry structure
	event := HistoryEntry{
		Type:     eventType,
		Pipeline: pipelineJSON,
		Errors:   errors,
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal pipeline history event: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO pipeline_history (pipeline_id, type, event)
		VALUES ($1, $2, $3)
	`, pipelineID, eventType, eventJSON)
	if err != nil {
		return fmt.Errorf("insert pipeline history event: %w", err)
	}

	return nil
}

// DeletePipeline deletes a pipeline and all associated entities
func (s *PostgresStorage) DeletePipeline(ctx context.Context, pipelineID string) error {
	s.logger.Info("deleting pipeline", "pipeline_id", pipelineID)

	// Delete pipeline
	commandTag, err := s.pool.Exec(ctx, ` DELETE FROM pipelines WHERE id = $1 `, pipelineID)
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}

	if err := checkRowsAffected(commandTag.RowsAffected()); err != nil {
		return err
	}

	s.logger.Info("pipeline and all associated entities deleted successfully", "pipeline_id", pipelineID)
	return nil
}
