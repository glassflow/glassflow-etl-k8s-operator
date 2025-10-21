package observability

import (
	"context"
	"log/slog"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// ConfigureLogger creates and configures a logger based on the provided configuration
func ConfigureLogger(cfg *Config) logr.Logger {
	if cfg.LogsEnabled {
		return configureOTelLogger(cfg)
	}

	// Fallback to standard zap logger when OTel is disabled
	return createStandardLogger()
}

// configureOTelLogger sets up OpenTelemetry logging with fallback to standard logging
func configureOTelLogger(cfg *Config) logr.Logger {
	// Create resource with service information
	attributes := buildResourceAttributes(cfg)

	res, err := resource.New(context.Background(),
		resource.WithAttributes(attributes...),
	)
	if err != nil {
		// Fallback to standard logging if OTel setup fails
		slog.Error("Failed to create OTel resource, falling back to standard logging", "error", err)
		return createStandardLogger()
	}

	// Create OTLP HTTP exporter
	exporter, err := otlploghttp.New(context.Background())
	if err != nil {
		// Fallback to standard logging if OTel setup fails
		slog.Error("Failed to create OTel exporter, falling back to standard logging", "error", err)
		return createStandardLogger()
	}

	// Create log processor and provider
	processor := log.NewBatchProcessor(exporter)
	provider := log.NewLoggerProvider(
		log.WithResource(res),
		log.WithProcessor(processor),
	)

	// Set global logger provider
	global.SetLoggerProvider(provider)

	// Create zap logger with OTEL core
	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)

	// Create OTEL zap core
	otelCore := otelzap.NewCore(cfg.ServiceName, otelzap.WithLoggerProvider(provider))

	// Create zap logger with both console and OTEL cores
	zapLogger, err := zapConfig.Build(
		zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewTee(core, otelCore)
		}),
	)
	if err != nil {
		slog.Error("Failed to create zap logger, falling back to standard logging", "error", err)
		return createStandardLogger()
	}

	// Convert zap logger to logr.Logger for controller-runtime
	// We need to use the zap logger directly with logr
	logrLogger := logr.New(&zapLogrSink{logger: zapLogger})

	return logrLogger
}

// createStandardLogger creates a standard zap logger without OTel
func createStandardLogger() logr.Logger {
	opts := ctrlzap.Options{
		Development: true,
		Level:       zapcore.InfoLevel,
	}
	return ctrlzap.New(ctrlzap.UseFlagOptions(&opts))
}

// zapLogrSink bridges zap.Logger to logr.LogSink
type zapLogrSink struct {
	logger *zap.Logger
}

func (s *zapLogrSink) Init(info logr.RuntimeInfo) {
	// No initialization needed
}

func (s *zapLogrSink) Enabled(level int) bool {
	// Convert logr level to zap level
	zapLevel := zapcore.Level(-level) // logr levels are negative
	return s.logger.Core().Enabled(zapLevel)
}

func (s *zapLogrSink) Info(level int, msg string, keysAndValues ...interface{}) {
	// Convert logr level to zap level
	zapLevel := zapcore.Level(-level) // logr levels are negative

	// Convert key-value pairs to zap fields
	fields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			fields = append(fields, zap.Any(key, keysAndValues[i+1]))
		}
	}

	s.logger.Log(zapLevel, msg, fields...)
}

func (s *zapLogrSink) Error(err error, msg string, keysAndValues ...interface{}) {
	// Convert key-value pairs to zap fields
	fields := make([]zap.Field, 0, len(keysAndValues)/2+1)
	fields = append(fields, zap.Error(err))

	for i := 0; i < len(keysAndValues)-1; i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			fields = append(fields, zap.Any(key, keysAndValues[i+1]))
		}
	}

	s.logger.Error(msg, fields...)
}

func (s *zapLogrSink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	// Convert key-value pairs to zap fields
	fields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			fields = append(fields, zap.Any(key, keysAndValues[i+1]))
		}
	}

	// Create a new logger with the fields
	newLogger := s.logger.With(fields...)
	return &zapLogrSink{logger: newLogger}
}

func (s *zapLogrSink) WithName(name string) logr.LogSink {
	// Create a new logger with the name
	newLogger := s.logger.Named(name)
	return &zapLogrSink{logger: newLogger}
}
