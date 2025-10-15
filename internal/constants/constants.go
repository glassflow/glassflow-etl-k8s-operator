package constants

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
)
