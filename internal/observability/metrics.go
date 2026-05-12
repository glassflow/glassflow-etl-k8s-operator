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

// RecordOrphanSweep records the outcome of handling an orphan during the stop-time sweep.
// outcome ∈ {swept, sweep_failed, stream_empty, already_deleted}. The per-stream identity is
// intentionally kept out of the metric (logs carry it) to bound time-series cardinality.
func (m *Meter) RecordSweepedMessages(ctx context.Context, pipelineID, outcome string) {
	attrs := []attribute.KeyValue{
		attribute.String("pipeline_id", pipelineID),
		attribute.String("outcome", outcome),
	}

	m.SweepedMessagesTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordOrphanSweepDuration records how long the orphan sweep step took for a pipeline.
func (m *Meter) RecordSweepDuration(ctx context.Context, pipelineID string, duration time.Duration) {
	attrs := []attribute.KeyValue{
		attribute.String("pipeline_id", pipelineID),
	}

	m.SweepDurationSeconds.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}
