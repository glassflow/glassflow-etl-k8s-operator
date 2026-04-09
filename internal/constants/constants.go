package constants

import "time"

// Kubernetes annotation constants for pipeline operations
const (
	// PipelineFinalizerName is the name of the finalizer added to Pipeline resources
	PipelineFinalizerName = "pipeline.etl.glassflow.io/finalizer"

	// Pipeline operation annotations
	PipelineCreateAnnotation        = "pipeline.etl.glassflow.io/create"
	PipelineResumeAnnotation        = "pipeline.etl.glassflow.io/resume"
	PipelineStopAnnotation          = "pipeline.etl.glassflow.io/stop"
	PipelineTerminateAnnotation     = "pipeline.etl.glassflow.io/terminate"
	PipelineDeleteAnnotation        = "pipeline.etl.glassflow.io/delete"
	PipelineEditAnnotation          = "pipeline.etl.glassflow.io/edit"
	PipelineHelmUninstallAnnotation = "pipeline.etl.glassflow.io/helm-uninstall"

	// Pipeline operation start time annotation - tracks when an operation started
	PipelineOperationStartTimeAnnotation = "pipeline.etl.glassflow.io/operation-start-time"

	// PipelineStopLastPendingCountAnnotation tracks the last observed total pending message count
	// during a stop operation, used to detect whether messages are being consumed.
	PipelineStopLastPendingCountAnnotation = "pipeline.etl.glassflow.io/stop-last-pending-count"

	// PipelineEditDrainLastMsgCountAnnotation tracks the last observed total message count across
	// stale OTLP source streams during an edit-downscale drain, used to detect whether the
	// NATS Stream Source transfer is making progress before extending the operation timeout.
	PipelineEditDrainLastMsgCountAnnotation = "pipeline.etl.glassflow.io/edit-drain-last-msg-count"

	// PipelineOTLPDownscaleOldSubjectCountAnnotation persists the old OTLP source stream count
	// across reconcile passes during an edit-downscale, so the subject override stays in effect
	// even after stale streams have been drained and deleted (and before the edit fully completes).
	PipelineOTLPDownscaleOldSubjectCountAnnotation = "pipeline.etl.glassflow.io/otlp-downscale-old-subject-count"

	// PipelineStopReasonAnnotation stores the reason a pipeline was stopped (e.g. from a component signal).
	// If absent, the stop was triggered via API.
	PipelineStopReasonAnnotation = "pipeline.etl.glassflow.io/stop-reason"

	// DefaultReconcileTimeout is the default maximum duration a reconcile operation can run before timing out
	DefaultReconcileTimeout = 15 * time.Minute

	DefaultMinReplicas = 1
)

var (
	// ReconcileTimeout is the maximum duration a reconcile operation can run before timing out
	ReconcileTimeout = DefaultReconcileTimeout
)

// Pipeline operation type constants
const (
	OperationCreate        = "create"
	OperationResume        = "resume"
	OperationStop          = "stop"
	OperationEdit          = "edit"
	OperationDelete        = "delete"
	OperationTerminate     = "terminate"
	OperationHelmUninstall = "helm-uninstall"
)

// resource name constants (used for prefix / suffix when naming resources)
const (
	SecretSuffix      = "secret"
	DedupComponent    = "dedup"
	SinkComponent     = "sink"
	IngestorComponent = "ingestor"
	JoinComponent     = "join"
)

// NATS stream and consumer names for critical messages
const (
	ComponentSignalsStream = "component-signals"
	OperatorConsumer       = "operator-consumer"
)

// Component secrets created in pipeline namespace (same DB/encryption as API)
const (
	ComponentDatabaseSecretName   = "glassflow-component-database-url"
	ComponentDatabaseSecretKey    = "connection-url"
	ComponentEncryptionSecretName = "glassflow-component-encryption-key"
	ComponentEncryptionSecretKey  = "encryption-key"
)
