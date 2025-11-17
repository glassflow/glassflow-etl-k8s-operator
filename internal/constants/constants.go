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
	ReconcileTimeout = 10 * time.Minute
)
