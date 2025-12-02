package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

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

// pipelineRow represents a row from the pipelines table
type pipelineRow struct {
	pipelineID           string
	name                 string
	status               string
	sourceID             uuid.UUID
	sinkID               uuid.UUID
	transformationIDsPtr *[]uuid.UUID
	metadataJSON         []byte
	createdAt            time.Time
	updatedAt            time.Time
}

// loadPipelineRow loads a pipeline row from the database
func (s *PostgresStorage) loadPipelineRow(ctx context.Context, pipelineID string) (*pipelineRow, error) {
	var row pipelineRow
	var transformationIDsArray pgtype.Array[pgtype.UUID]

	err := s.pool.QueryRow(ctx, `
		SELECT id, name, status, source_id, sink_id, transformation_ids, metadata, created_at, updated_at
		FROM pipelines
		WHERE id = $1
	`, pipelineID).Scan(
		&row.pipelineID,
		&row.name,
		&row.status,
		&row.sourceID,
		&row.sinkID,
		&transformationIDsArray,
		&row.metadataJSON,
		&row.createdAt,
		&row.updatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			s.logger.V(1).Info("pipeline not found", "pipeline_id", pipelineID)
			return nil, ErrPipelineNotExists
		}
		s.logger.Error(err, "failed to load pipeline row", "pipeline_id", pipelineID)
		return nil, fmt.Errorf("get pipeline: %w", err)
	}

	// Convert pgtype UUID array to []uuid.UUID
	if transformationIDsArray.Valid {
		transformationIDs := make([]uuid.UUID, 0, len(transformationIDsArray.Elements))
		for _, elem := range transformationIDsArray.Elements {
			if elem.Valid {
				transformationIDs = append(transformationIDs, elem.Bytes)
			}
		}
		row.transformationIDsPtr = &transformationIDs
	}

	return &row, nil
}

// UpdatePipelineStatus updates the pipeline status and creates a history event
func (s *PostgresStorage) UpdatePipelineStatus(ctx context.Context, pipelineID string, status PipelineStatus, errors []string) error {

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

	// Get pipeline row to find associated entity IDs
	row, err := s.loadPipelineRow(ctx, pipelineID)
	if err != nil {
		if err == ErrPipelineNotExists {
			return err
		}
		return fmt.Errorf("load pipeline row: %w", err)
	}

	// Get transformation IDs
	transformationIDs := handleTransformationIDs(row.transformationIDsPtr)

	// Get connection IDs from source and sink
	var kafkaConnID, chConnID uuid.UUID
	err = s.pool.QueryRow(ctx, `
		SELECT connection_id FROM sources WHERE id = $1
	`, row.sourceID).Scan(&kafkaConnID)
	if err != nil {
		return fmt.Errorf("get kafka connection ID: %w", err)
	}

	err = s.pool.QueryRow(ctx, `
		SELECT connection_id FROM sinks WHERE id = $1
	`, row.sinkID).Scan(&chConnID)
	if err != nil {
		return fmt.Errorf("get clickhouse connection ID: %w", err)
	}

	// Begin transaction for atomic deletion
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

	// 1. Delete transformations (no foreign key constraints)
	if len(transformationIDs) > 0 {
		_, err = tx.Exec(ctx, `
			DELETE FROM transformations WHERE id = ANY($1)
		`, transformationIDs)
		if err != nil {
			return fmt.Errorf("delete transformations: %w", err)
		}
	}

	// 2. Delete pipeline (CASCADE will delete schemas and pipeline_history)
	commandTag, err := tx.Exec(ctx, `
		DELETE FROM pipelines WHERE id = $1
	`, pipelineID)
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}

	if err := checkRowsAffected(commandTag.RowsAffected()); err != nil {
		return err
	}

	// 3. Delete sources (no longer referenced by pipeline)
	_, err = tx.Exec(ctx, `
		DELETE FROM sources WHERE id = $1
	`, row.sourceID)
	if err != nil {
		return fmt.Errorf("delete source: %w", err)
	}

	// 4. Delete sinks (no longer referenced by pipeline)
	_, err = tx.Exec(ctx, `
		DELETE FROM sinks WHERE id = $1
	`, row.sinkID)
	if err != nil {
		return fmt.Errorf("delete sink: %w", err)
	}

	// 5. Delete connections (no longer referenced by sources/sinks)
	_, err = tx.Exec(ctx, `
		DELETE FROM connections WHERE id = $1
	`, kafkaConnID)
	if err != nil {
		return fmt.Errorf("delete kafka connection: %w", err)
	}

	// Only delete ClickHouse connection if it's different from Kafka connection
	if chConnID != kafkaConnID {
		_, err = tx.Exec(ctx, `
			DELETE FROM connections WHERE id = $1
		`, chConnID)
		if err != nil {
			return fmt.Errorf("delete clickhouse connection: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	committed = true

	s.logger.Info("pipeline and all associated entities deleted successfully",
		"pipeline_id", pipelineID,
		"transformations_deleted", len(transformationIDs),
		"source_id", row.sourceID.String(),
		"sink_id", row.sinkID.String())

	return nil
}
