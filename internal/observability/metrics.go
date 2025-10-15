package observability

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RecordReconcileOperation records a reconcile operation
func (m *Meter) RecordReconcileOperation(ctx context.Context, operation, status, pipelineID string, duration float64) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.String("status", status),
		attribute.String("pipeline_id", pipelineID),
	}

	m.ReconcileOperationsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.ReconcileDurationSeconds.Record(ctx, duration, metric.WithAttributes(attrs...))
}

// RecordReconcileError records a reconcile error
func (m *Meter) RecordReconcileError(ctx context.Context, operation, errorType, pipelineID string) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.String("error_type", errorType),
		attribute.String("pipeline_id", pipelineID),
	}

	m.ReconcileErrorsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordStatusTransition records a pipeline status transition
func (m *Meter) RecordStatusTransition(ctx context.Context, fromStatus, toStatus, pipelineID string) {
	attrs := []attribute.KeyValue{
		attribute.String("from_status", fromStatus),
		attribute.String("to_status", toStatus),
		attribute.String("pipeline_id", pipelineID),
	}

	m.PipelineStatusTransitionsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordNATSOperation records a NATS operation
func (m *Meter) RecordNATSOperation(ctx context.Context, operation, status string) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.String("status", status),
	}

	m.NATSOperationsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordK8sResourceOperation records a K8s resource operation
func (m *Meter) RecordK8sResourceOperation(ctx context.Context, resourceType, operation, status string) {
	attrs := []attribute.KeyValue{
		attribute.String("resource_type", resourceType),
		attribute.String("operation", operation),
		attribute.String("status", status),
	}

	m.K8sResourceOperationsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordDeploymentReady records deployment readiness time
func (m *Meter) RecordDeploymentReady(ctx context.Context, component, pipelineID string, duration float64) {
	attrs := []attribute.KeyValue{
		attribute.String("component", component),
		attribute.String("pipeline_id", pipelineID),
	}

	m.DeploymentReadyDurationSeconds.Record(ctx, duration, metric.WithAttributes(attrs...))
}

// ClassifyError classifies an error into a category for metrics
func ClassifyError(err error) string {
	if err == nil {
		return "none"
	}

	errStr := err.Error()

	// Classify common error types
	switch {
	case contains(errStr, "not found"):
		return "not_found"
	case contains(errStr, "already exists"):
		return "already_exists"
	case contains(errStr, "timeout"):
		return "timeout"
	case contains(errStr, "connection"):
		return "connection_error"
	case contains(errStr, "permission"):
		return "permission_denied"
	case contains(errStr, "invalid"):
		return "invalid_request"
	case contains(errStr, "conflict"):
		return "conflict"
	default:
		return "unknown"
	}
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			(len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					containsSubstring(s, substr))))
}

// containsSubstring is a simple substring check
func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
