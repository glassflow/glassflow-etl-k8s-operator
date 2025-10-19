package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Meter holds all the metrics for the operator
type Meter struct {
	// Reconcile operation metrics
	ReconcileOperationsTotal metric.Int64Counter
	ReconcileErrorsTotal     metric.Int64Counter

	// Pipeline status metrics
	PipelineStatusTransitionsTotal metric.Int64Counter

	// NATS operation metrics
	NATSOperationsTotal metric.Int64Counter
}

const GfmOperatorMetricPrefix = "gfm_operator"

// ConfigureMeter creates and configures metrics based on the provided configuration
func ConfigureMeter(cfg *Config, log logr.Logger) *Meter {
	if !cfg.MetricsEnabled {
		// Return nil meter when metrics are disabled
		return nil
	}

	// Set up OTLP metrics exporter
	ctx := context.Background()

	// Create OTLP metrics exporter with default configuration
	// This will automatically read OTEL_EXPORTER_OTLP_ENDPOINT from environment
	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		log.Error(err, "Failed to create OTLP metrics exporter")
		return nil
	}

	// Create resource with service information
	attrs := buildResourceAttributes(cfg)

	res, err := resource.New(ctx, resource.WithAttributes(attrs...))
	if err != nil {
		log.Error(err, "Failed to create resource")
		return nil
	}

	// Create MeterProvider with OTLP exporter
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(10*time.Second),
		)),
	)

	// Set the global MeterProvider
	otel.SetMeterProvider(meterProvider)

	return NewMeter()
}

// NewMeter creates a new Meter instance with all the required metrics
func NewMeter() *Meter {
	meter := otel.Meter("glassflow-operator")

	return &Meter{
		ReconcileOperationsTotal: mustCreateCounter(meter, GfmOperatorMetricPrefix+"_reconcile_operations_total",
			"Total number of reconcile operations by operation type and status"),
		ReconcileErrorsTotal: mustCreateCounter(meter, GfmOperatorMetricPrefix+"_reconcile_errors_total",
			"Total number of reconcile errors by error type"),
		PipelineStatusTransitionsTotal: mustCreateCounter(meter, GfmOperatorMetricPrefix+"_pipeline_status_transitions_total",
			"Total number of pipeline status transitions"),
		NATSOperationsTotal: mustCreateCounter(meter, GfmOperatorMetricPrefix+"_nats_operations_total",
			"Total number of NATS operations"),
	}
}

// Helper functions to create metrics with error handling

func mustCreateCounter(meter metric.Meter, name, description string) metric.Int64Counter {
	counter, err := meter.Int64Counter(
		name,
		metric.WithDescription(description),
		metric.WithUnit("1"), // unit for counters
	)
	if err != nil {
		slog.Error("Failed to create counter", "name", name, "error", err)
		panic("failed to create counter " + name + ": " + err.Error())
	}
	return counter
}

func mustCreateHistogram(meter metric.Meter, name, description string) metric.Float64Histogram {
	histogram, err := meter.Float64Histogram(
		name,
		metric.WithDescription(description),
		metric.WithUnit("s"), // seconds
		metric.WithExplicitBucketBoundaries(
			0.001, // 1ms
			0.005, // 5ms
			0.01,  // 10ms
			0.025, // 25ms
			0.05,  // 50ms
			0.1,   // 100ms
			0.25,  // 250ms
			0.5,   // 500ms
			1.0,   // 1s
			2.5,   // 2.5s
			5.0,   // 5s
			10.0,  // 10s
		),
	)
	if err != nil {
		slog.Error("Failed to create histogram", "name", name, "error", err)
		panic("failed to create histogram " + name + ": " + err.Error())
	}
	return histogram
}
