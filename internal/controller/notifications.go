package controller

import (
	"context"
	"fmt"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/notifications"
)

func (r *PipelineReconciler) successNotificationMetadata(operation string) map[string]interface{} {
	return map[string]interface{}{
		"operation":        operation,
		"status":           "success",
		"cluster_provider": r.Config.ClusterProvider,
	}
}

func (r *PipelineReconciler) failureNotificationMetadata(operation string, err error) map[string]interface{} {
	return map[string]interface{}{
		"operation":        operation,
		"status":           "failure",
		"error":            err.Error(),
		"cluster_provider": r.Config.ClusterProvider,
	}
}

func (r *PipelineReconciler) sendOperationSuccessNotification(
	ctx context.Context, operation, pipelineID, message string,
) {
	if !notifications.IsEnabled() {
		return
	}

	metadata := r.successNotificationMetadata(operation)
	notification := buildOperationSuccessNotification(operation, pipelineID, message, metadata)
	if notification == nil {
		return
	}

	_ = notifications.Publish(ctx, notification)
}

func buildOperationSuccessNotification(
	operation string,
	pipelineID string,
	message string,
	metadata map[string]interface{},
) *notifications.Notification {

	switch operation {
	case constants.OperationCreate:
		return notifications.NewPipelineDeployedNotification(pipelineID, "", "", metadata)
	case constants.OperationResume:
		return notifications.NewPipelineResumedNotification(pipelineID, "", "", metadata)
	case constants.OperationStop:
		return notifications.NewPipelineStoppedNotification(pipelineID, "", message, metadata)
	case constants.OperationTerminate:
		return notifications.NewPipelineStoppedNotification(
			pipelineID,
			"Pipeline Terminated",
			"Pipeline has been terminated",
			metadata,
		)
	case constants.OperationEdit:
		return notifications.NewPipelineDeployedNotification(
			pipelineID,
			"Pipeline Updated Successfully",
			"Pipeline has been updated successfully",
			metadata,
		)
	case constants.OperationDelete:
		return notifications.NewPipelineDeletedNotification(pipelineID, "", "", metadata)
	case constants.OperationHelmUninstall:
		return notifications.NewPipelineDeletedNotification(
			pipelineID,
			"Pipeline Uninstalled",
			"Pipeline has been uninstalled",
			metadata,
		)
	default:
		return nil
	}
}

func (r *PipelineReconciler) sendOperationFailureNotification(
	ctx context.Context, operation, pipelineID string, err error,
) {
	if !notifications.IsEnabled() {
		return
	}

	metadata := r.failureNotificationMetadata(operation, err)
	notification := buildOperationFailureNotification(operation, pipelineID, err, metadata)
	_ = notifications.Publish(ctx, notification)
}

func buildOperationFailureNotification(
	operation string,
	pipelineID string,
	err error,
	metadata map[string]interface{},
) *notifications.Notification {
	title := fmt.Sprintf("Pipeline Operation Failed: %s", operation)
	message := fmt.Sprintf("Operation %s failed: %s", operation, err.Error())
	return notifications.NewPipelineFailedNotification(pipelineID, title, message, metadata)
}
