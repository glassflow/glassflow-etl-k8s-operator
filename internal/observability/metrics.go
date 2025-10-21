package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RecordReconcileOperation records a reconcile operation
func (m *Meter) RecordReconcileOperation(ctx context.Context, operation, status, pipelineID string) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.String("status", status),
		attribute.String("pipeline_id", pipelineID),
	}

	m.ReconcileOperationsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordReconcileError records a reconcile error
func (m *Meter) RecordReconcileError(ctx context.Context, operation, errMsg, pipelineID string) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.String("error_msg", errMsg),
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
		attribute.Int64("timestamp", time.Now().Unix()),
	}

	m.PipelineStatusTransitionsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordNATSOperation records a NATS operation
func (m *Meter) RecordNATSOperation(ctx context.Context, operation, status, pipelineID string) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.String("status", status),
		attribute.String("pipeline_id", pipelineID),
	}

	m.NATSOperationsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}
