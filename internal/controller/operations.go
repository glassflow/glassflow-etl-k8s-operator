package controller

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
)

type pipelineOperationHandler func(context.Context, logr.Logger, etlv1alpha1.Pipeline) (ctrl.Result, error)

func (r *PipelineReconciler) operationHandlerFor(operation string) pipelineOperationHandler {
	handlers := map[string]pipelineOperationHandler{
		constants.OperationHelmUninstall: r.reconcileHelmUninstall,
		constants.OperationCreate: func(ctx context.Context, _ logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
			return r.createPipeline(ctx, p)
		},
		constants.OperationResume:    r.reconcileResume,
		constants.OperationStop:      r.reconcileStop,
		constants.OperationEdit:      r.reconcileEdit,
		constants.OperationTerminate: r.reconcileTerminate,
		constants.OperationDelete:    r.reconcileDelete,
	}

	return handlers[operation]
}

func (r *PipelineReconciler) dispatchOperation(
	ctx context.Context,
	log logr.Logger,
	operation string,
	p etlv1alpha1.Pipeline,
) (ctrl.Result, error) {
	handler := r.operationHandlerFor(operation)
	if handler == nil {
		return ctrl.Result{}, nil
	}

	return handler(ctx, log, p)
}

func (r *PipelineReconciler) checkOperationTimeoutAndLogProgress(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
) (ctrl.Result, bool, error) {
	timedOut, elapsed := r.checkOperationTimeout(log, p)
	if timedOut {
		result, err := r.handleOperationTimeout(ctx, log, p)
		return result, true, err
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", p.Spec.ID, "elapsed", elapsed)
	}

	return ctrl.Result{}, false, nil
}

func (r *PipelineReconciler) setOperationStartTimeBestEffort(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline) {
	if err := r.setOperationStartTime(ctx, p); err != nil {
		log.Error(err, "failed to set operation start time", "pipeline_id", p.Spec.ID)
	}
}

func (r *PipelineReconciler) clearOperationAnnotationAndStatus(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	annotationKey string,
	finalStatus models.PipelineStatus,
	clearStartTime bool,
) {
	annotations := p.GetAnnotations()
	if annotations == nil {
		return
	}

	delete(annotations, annotationKey)
	if clearStartTime {
		r.clearOperationStartTime(p)
	}

	p.SetAnnotations(annotations)
	p.Status = etlv1alpha1.PipelineStatus(finalStatus)
	if err := r.Update(ctx, p); err != nil {
		log.Error(err, "failed to clear operation annotation", "pipeline_id", p.Spec.ID, "annotation", annotationKey)
	}
}

func (r *PipelineReconciler) recordOperationSuccess(
	ctx context.Context,
	metricsOperation string,
	usageStatsOperation string,
	pipelineID string,
) {
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, metricsOperation, "success", pipelineID)
	})
	r.sendReconcileSuccessEvent(ctx, usageStatsOperation, pipelineID)
}

func defaultOperationRequeueResult() ctrl.Result {
	return ctrl.Result{Requeue: true, RequeueAfter: time.Second}
}
