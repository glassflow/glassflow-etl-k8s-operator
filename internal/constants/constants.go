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

	// ReconcileTimeout is the maximum duration a reconcile operation can run before timing out
	ReconcileTimeout = 5 * time.Minute
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
	ComponentDatabaseSecretKey   = "connection-url"
	ComponentEncryptionSecretName = "glassflow-component-encryption-key"
	ComponentEncryptionSecretKey  = "encryption-key"
)
