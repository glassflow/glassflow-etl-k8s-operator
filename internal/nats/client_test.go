package nats

import (
	"testing"
	"time"
)

func TestPipelineStatusConstants(t *testing.T) {
	// Test that all status constants are defined correctly
	expectedStatuses := map[string]PipelineStatus{
		"Created":     PipelineStatusCreated,
		"Running":     PipelineStatusRunning,
		"Terminating": PipelineStatusTerminating,
		"Stopped":     PipelineStatusStopped,
		"Failed":      PipelineStatusFailed,
	}

	for name, status := range expectedStatuses {
		if string(status) != name {
			t.Errorf("Expected status %s, got %s", name, string(status))
		}
	}
}

func TestPipelineHealth(t *testing.T) {
	now := time.Now().UTC()
	health := PipelineHealth{
		PipelineID:    "test-pipeline",
		PipelineName:  "Test Pipeline",
		OverallStatus: PipelineStatusCreated,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if health.PipelineID != "test-pipeline" {
		t.Errorf("Expected PipelineID 'test-pipeline', got %s", health.PipelineID)
	}

	if health.OverallStatus != PipelineStatusCreated {
		t.Errorf("Expected OverallStatus 'Created', got %s", health.OverallStatus)
	}
}

func TestPipelineConfig(t *testing.T) {
	now := time.Now().UTC()
	config := PipelineConfig{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Status: PipelineHealth{
			PipelineID:    "test-pipeline",
			PipelineName:  "Test Pipeline",
			OverallStatus: PipelineStatusRunning,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
		CreatedAt: now,
	}

	if config.ID != "test-pipeline" {
		t.Errorf("Expected ID 'test-pipeline', got %s", config.ID)
	}

	if config.Status.OverallStatus != PipelineStatusRunning {
		t.Errorf("Expected status 'Running', got %s", config.Status.OverallStatus)
	}
}
