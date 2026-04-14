/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
	"github.com/glassflow/glassflow-etl-k8s-operator/pkg/usagestats"
)

// -------------------------------------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------------------------------------

// pipelineOperationPredicate filters events to trigger reconcile on spec changes or annotation changes
// but not on status-only changes, does not filter requeue
var pipelineOperationPredicate = predicate.Funcs{
	CreateFunc: func(e event.CreateEvent) bool {
		return true // Always reconcile on create
	},
	DeleteFunc: func(e event.DeleteEvent) bool {
		return true // Always reconcile on delete
	},
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldObj := e.ObjectOld
		newObj := e.ObjectNew

		// Trigger on spec changes (generation changes)
		if oldObj.GetGeneration() != newObj.GetGeneration() {
			return true
		}

		// Trigger on annotation changes (our operation triggers)
		oldAnnotations := oldObj.GetAnnotations()
		newAnnotations := newObj.GetAnnotations()
		return !reflect.DeepEqual(oldAnnotations, newAnnotations)
	},
	GenericFunc: func(e event.GenericEvent) bool {
		return true // Always reconcile on generic events
	},
}

// pipelineStorage is the subset of postgres.PostgresStorage used by the controller.
type pipelineStorage interface {
	DeletePipeline(ctx context.Context, pipelineID string) error
	UpdatePipelineStatus(ctx context.Context, pipelineID string, status models.PipelineStatus, errors []string, reason string) error
}

// -------------------------------------------------------------------------------------------------------------------

// PipelineReconciler reconciles a Pipeline object
type PipelineReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	Meter            *observability.Meter
	NATSClient       *nats.NATSClient
	PostgresStorage  pipelineStorage
	Config           ReconcilerConfig
	UsageStatsClient *usagestats.Client
}

// -------------------------------------------------------------------------------------------------------------------

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.Config.Validate(); err != nil {
		return fmt.Errorf("validate reconciler config: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&etlv1alpha1.Pipeline{}).
		Named("pipeline").
		WithEventFilter(pipelineOperationPredicate).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

// +kubebuilder:rbac:groups=etl.glassflow.io,resources=pipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=etl.glassflow.io,resources=pipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=etl.glassflow.io,resources=pipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;delete

// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.0/pkg/reconcile
func (r *PipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	log.Info("reconcile event", "request", req.String())

	var p etlv1alpha1.Pipeline

	err := r.Get(ctx, req.NamespacedName, &p)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("pipeline not found", "request", req.String())
			return ctrl.Result{}, client.IgnoreNotFound(err)
		} else {
			log.Error(err, "unable to fetch pipeline")
			return ctrl.Result{}, fmt.Errorf("get pipeline: %w", err)
		}
	}

	operation := getPipelineOperationFromAnnotations(p.GetAnnotations())
	if operation != "" {
		if operation == constants.OperationHelmUninstall {
			log.Info("HELM UNINSTALL detected - interrupting any ongoing operations", "pipeline_id", p.Spec.ID)
		}
		return r.dispatchOperation(ctx, log, operation, p)
	}

	// No operation needed
	return ctrl.Result{}, nil
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) createPipeline(ctx context.Context, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	err := r.addFinalizer(ctx, &p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
	}

	return r.reconcileCreate(ctx, log, p)
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) reconcileCreate(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline creation", "pipeline_id", pipelineID)

	// Check if pipeline is already running
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusRunning) {
		log.Info("pipeline already running", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}

	if result, handled, err := r.checkOperationTimeoutAndLogProgress(ctx, log, &p); handled || err != nil {
		return result, err
	}

	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusCreated) {
		log.Info("pipeline is already being created", "pipeline_id", p.Spec.ID)
		// Continue with the creation process
	} else {
		// Transition to Creation status first
		p.Status = etlv1alpha1.PipelineStatus(models.PipelineStatusCreated)

		// Update CRD status
		err := r.Status().Update(ctx, &p, &client.SubResourceUpdateOptions{})
		if err != nil {
			r.recordReconcileError(ctx, "create", pipelineID, err)
			return ctrl.Result{}, fmt.Errorf("update pipeline CRD status: %w", err)
		}

		r.setOperationStartTimeBestEffort(ctx, log, &p)
	}

	ns, err := r.createNamespace(ctx, p)
	if err != nil {
		r.recordReconcileError(ctx, "create", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("setup namespace: %w", err)
	}

	err = r.createNATSStreams(ctx, p)
	if err != nil {
		r.recordReconcileError(ctx, "create", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("setup streams: %w", err)
	}

	labels := preparePipelineLabels(p)

	secretName := r.getResourceName(p)
	secret, err := r.createSecret(ctx, types.NamespacedName{Namespace: ns.GetName(), Name: secretName}, labels, p)
	if err != nil {
		if errors.Is(err, ErrPipelineConfigSecretNotFound) {
			log.Info("pipeline config secret not found, requeuing to wait for API to create it", "pipeline_id", pipelineID, "error", err)
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 2}, nil
		}
		return ctrl.Result{}, fmt.Errorf("create secret for pipeline config %s: %w", p.Spec.ID, err)
	}

	if err = r.ensureComponentSecretsInPipelineNamespace(ctx, r.getTargetNamespace(p)); err != nil {
		r.recordReconcileError(ctx, "create", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("ensure component secrets: %w", err)
	}

	// Ensure all deployments are ready
	result, err := r.createPipelineComponents(ctx, log, &p, ns, labels, secret)
	if err != nil || result.Requeue {
		return result, err
	}
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusFailed) {
		return ctrl.Result{}, nil
	}

	// All deployments are ready, update status to Running
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusRunning, nil, "create")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to running: %w", err)
	}

	r.clearOperationAnnotationAndStatus(
		ctx,
		log,
		&p,
		constants.PipelineCreateAnnotation,
		models.PipelineStatusRunning,
		true,
	)
	r.recordOperationSuccess(ctx, "create", pipelineID, "")

	log.Info("pipeline creation completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileTerminate(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline termination", "pipeline_id", pipelineID)

	// Check if pipeline is already stopped
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusStopped) {
		log.Info("pipeline already stopped", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}

	if result, handled, err := r.checkOperationTimeoutAndLogProgress(ctx, log, &p); handled || err != nil {
		return result, err
	}

	r.setOperationStartTimeBestEffort(ctx, log, &p)

	// Stop all pipeline components
	result, err := r.terminatePipelineComponents(ctx, log, &p)
	if err != nil || result.Requeue {
		return result, err
	}

	// Remove all NATS streams/KV stores for this pipeline before marking it Stopped.
	err = r.cleanupNATSPipelineResources(ctx, log, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("cleanup NATS resources for terminate: %w", err)
	}

	// Update pipeline status to "Stopped"
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusStopped, nil, "terminate")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to stopped: %w", err)
	}

	r.clearOperationAnnotationAndStatus(
		ctx,
		log,
		&p,
		constants.PipelineTerminateAnnotation,
		models.PipelineStatusStopped,
		false,
	)
	r.recordOperationSuccess(ctx, "terminate", pipelineID, "")

	log.Info("pipeline termination completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileDelete(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline deletion", "pipeline_id", pipelineID)

	// Check if pipeline is stopped
	if p.Status != etlv1alpha1.PipelineStatus(models.PipelineStatusStopped) {
		log.Info("pipeline is not stopped but attempting to delete", "pipeline_id", p.Spec.ID)
	}

	// only if pipelines have individual NS
	err := r.deleteNamespace(ctx, log, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("delete pipeline namespace: %w", err)
	}

	err = r.deleteSecret(ctx, log, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("delete pipeline secret: %w", err)
	}

	// Clean up NATS streams
	err = r.cleanupNATSPipelineResources(ctx, log, p)
	if err != nil {
		log.Info("failed to cleanup NATS resources during termination", "pipeline_id", p.Spec.ID)
		// Don't return error here as namespace is already deleted
		// Just log and continue
	}

	// Clean up PVCs for dedup StatefulSets
	err = r.cleanupDedupPVCs(ctx, log, p)
	if err != nil {
		log.Error(err, "failed to cleanup dedup PVCs")
		// Don't fail the deletion, just log
	}

	// Clean up pipeline configuration from PostgreSQL
	err = r.PostgresStorage.DeletePipeline(ctx, p.Spec.ID)
	if err != nil {
		log.Info("failed to delete pipeline configuration from PostgreSQL", "pipeline_id", p.Spec.ID, "error", err)
		// Don't return error here - we're in force cleanup mode
	} else {
		log.Info("successfully deleted pipeline configuration from PostgreSQL", "pipeline_id", p.Spec.ID)
	}

	r.clearOperationAnnotationAndStatus(
		ctx,
		log,
		&p,
		constants.PipelineDeleteAnnotation,
		models.PipelineStatusStopped,
		false,
	)

	// Remove finalizer
	err = r.removeFinalizer(ctx, &p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}

	// Delete the pipeline CRD
	err = r.Delete(ctx, &p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("delete pipeline CRD: %w", err)
	}

	r.recordOperationSuccess(ctx, "delete", pipelineID, "")

	log.Info("pipeline deletion completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileHelmUninstall(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID
	log.Info("reconciling pipeline helm uninstall - FORCING cleanup regardless of current status", "pipeline_id", pipelineID, "current_status", p.Status)

	// FORCE cleanup regardless of current status - this is helm uninstall!
	log.Info("HELM UNINSTALL: Forcing immediate cleanup of pipeline", "pipeline_id", pipelineID)

	// Check if pipeline is already stopped
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusStopped) {
		log.Info("pipeline already stopped during helm uninstall", "pipeline_id", pipelineID)

		// Delete pipeline configuration from PostgreSQL for stopped pipelines
		if err := r.PostgresStorage.DeletePipeline(ctx, pipelineID); err != nil {
			log.Info("failed to delete stopped pipeline configuration from PostgreSQL", "pipeline_id", pipelineID, "error", err)
		} else {
			log.Info("successfully deleted stopped pipeline configuration from PostgreSQL", "pipeline_id", pipelineID)
		}

		// Remove helm uninstall annotation and finalizer to allow cleanup
		annotations := p.GetAnnotations()
		if annotations != nil {
			delete(annotations, constants.PipelineHelmUninstallAnnotation)
			p.SetAnnotations(annotations)
			err := r.Update(ctx, &p)
			if err != nil {
				log.Error(err, "failed to remove helm uninstall annotation", "pipeline_id", pipelineID)
			}
		}
		err := r.removeFinalizer(ctx, &p)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
		err = r.Delete(ctx, &p)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("delete pipeline CRD: %w", err)
		}
		return ctrl.Result{}, nil
	}

	// FORCE cleanup - skip normal termination process for helm uninstall
	log.Info("HELM UNINSTALL: Force deleting pipeline namespace and resources", "pipeline_id", pipelineID)

	// Force delete namespace for this pipeline (this will delete all resources in the namespace: StatefulSets, Services, Deployments, etc.)
	err := r.deleteNamespace(ctx, log, p)
	if err != nil {
		log.Error(err, "failed to delete pipeline namespace during helm uninstall", "pipeline_id", pipelineID)
		// Continue anyway - we're in force cleanup mode
	}

	// Clean up NATS streams but keep the pipeline configuration
	err = r.cleanupNATSPipelineResources(ctx, log, p)
	if err != nil {
		log.Info("failed to cleanup NATS resources during helm uninstall", "pipeline_id", pipelineID)
		// Don't return error here - we're in force cleanup mode
	}

	// Clean up pipeline configuration from PostgreSQL
	err = r.PostgresStorage.DeletePipeline(ctx, pipelineID)
	if err != nil {
		log.Info("failed to delete pipeline configuration from PostgreSQL", "pipeline_id", pipelineID, "error", err)
		// Don't return error here - we're in force cleanup mode
	} else {
		log.Info("successfully deleted pipeline configuration from PostgreSQL", "pipeline_id", pipelineID)
	}

	// Remove all pipeline operation annotations
	annotations := p.GetAnnotations()
	if annotations != nil {
		// Remove all pipeline operation annotations
		for _, annotation := range []string{
			constants.PipelineHelmUninstallAnnotation,
			constants.PipelineCreateAnnotation,
			constants.PipelineTerminateAnnotation,
			constants.PipelineStopAnnotation,
			constants.PipelineResumeAnnotation,
		} {
			delete(annotations, annotation)
		}
		p.SetAnnotations(annotations)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove annotations during helm uninstall", "pipeline_id", pipelineID)
		}
	}

	// Remove finalizer
	err = r.removeFinalizer(ctx, &p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}

	// Delete the pipeline CRD
	err = r.Delete(ctx, &p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("delete pipeline CRD: %w", err)
	}

	r.recordOperationSuccess(ctx, "helm-uninstall", pipelineID, "")

	log.Info("pipeline helm uninstall completed successfully - FORCE CLEANUP", "pipeline", p.Name, "pipeline_id", pipelineID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileResume(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline resume", "pipeline_id", pipelineID)

	namespace := r.getTargetNamespace(p)

	// Check if pipeline is already running or resuming
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusRunning) {
		log.Info("pipeline already running", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}

	if result, handled, err := r.checkOperationTimeoutAndLogProgress(ctx, log, &p); handled || err != nil {
		return result, err
	}

	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusResuming) {
		log.Info("pipeline is already being resumed", "pipeline_id", p.Spec.ID)
		// Continue with the resume process
	} else {
		// Transition to Resuming status first
		err := r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusResuming, nil, "resume")
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("update pipeline status to resuming: %w", err)
		}

		r.setOperationStartTimeBestEffort(ctx, log, &p)

		// Requeue to continue with the resume process
		return defaultOperationRequeueResult(), nil
	}

	// Get namespace
	var ns v1.Namespace
	err := r.Get(ctx, types.NamespacedName{Name: namespace}, &ns)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get namespace %s: %w", namespace, err)
	}

	// Get secret
	secretName := types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p)}
	var secret v1.Secret
	err = r.Get(ctx, secretName, &secret)
	if err != nil {
		if apierrors.IsNotFound(err) {

			labels := preparePipelineLabels(p)
			secret, err = r.createSecret(ctx, secretName, labels, p)
			if err != nil {
				if errors.Is(err, ErrPipelineConfigSecretNotFound) {
					log.Info("pipeline config secret not found during resume, requeuing to wait for API to create it", "pipeline_id", pipelineID, "error", err)
					return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 2}, nil
				}
				return ctrl.Result{}, fmt.Errorf("create secret for resume: %w", err)
			}
		} else {
			return ctrl.Result{}, fmt.Errorf("get secret %s: %w", secretName, err)
		}
	}

	labels := preparePipelineLabels(p)

	err = r.createNATSStreams(ctx, p)
	if err != nil {
		r.recordReconcileError(ctx, "edit", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("setup streams: %w", err)
	}

	if err = r.ensureComponentSecretsInPipelineNamespace(ctx, r.getTargetNamespace(p)); err != nil {
		r.recordReconcileError(ctx, "resume", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("ensure component secrets: %w", err)
	}

	// Ensure all deployments are ready
	result, err := r.createPipelineComponents(ctx, log, &p, ns, labels, secret)
	if err != nil || result.Requeue {
		return result, err
	}
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusFailed) {
		return ctrl.Result{}, nil
	}

	// All deployments are ready, update status to Running
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusRunning, nil, "resume")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to running: %w", err)
	}

	r.clearOperationAnnotationAndStatus(
		ctx,
		log,
		&p,
		constants.PipelineResumeAnnotation,
		models.PipelineStatusRunning,
		true,
	)
	r.recordOperationSuccess(ctx, "resume", pipelineID, "")

	log.Info("pipeline resume completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileStop(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline stop", "pipeline_id", pipelineID)

	// Check if pipeline is already stopped
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusStopped) {
		log.Info("pipeline already stopped", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}

	// For stop operations, check whether pending messages are still decreasing before failing on timeout.
	// If progress is being made, extend the timeout window instead of marking the pipeline failed.
	timedOut, elapsed := r.checkOperationTimeout(log, &p)
	if timedOut {
		if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusStopping) {
			extended, extErr := r.tryExtendStopTimeout(ctx, log, &p)
			if extErr != nil {
				log.Error(extErr, "failed to check pending progress during stop timeout", "pipeline_id", pipelineID)
				// Fall through to normal timeout handling on error.
			} else if extended {
				return ctrl.Result{Requeue: true, RequeueAfter: pendingMessagesRequeueDelay}, nil
			}
		}
		result, err := r.handleOperationTimeout(ctx, log, &p)
		return result, err
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", pipelineID, "elapsed", elapsed)
	}

	// Check if pipeline is already stopping
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusStopping) {
		log.Info("pipeline is already being stopped", "pipeline_id", p.Spec.ID)
		// Continue with the stop process
	} else {
		// Transition to Stopping status first
		err := r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusStopping, nil, "stop")
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("update pipeline status to stopping: %w", err)
		}

		r.setOperationStartTimeBestEffort(ctx, log, &p)

		// Requeue to continue with the stop process
		return defaultOperationRequeueResult(), nil
	}

	// Stop all pipeline components
	result, err := r.stopPipelineComponents(ctx, log, &p)
	if err != nil || result.Requeue {
		return result, err
	}
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusFailed) {
		return ctrl.Result{}, nil
	}

	// Remove NATS streams/KV stores for this pipeline before marking it Stopped, while preserving DLQ.
	err = r.cleanupNATSPipelineResourcesKeepDLQ(ctx, log, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("cleanup NATS resources for stop: %w", err)
	}

	// Update status to Stopped
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusStopped, nil, "stop")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to stopped: %w", err)
	}

	// Read stop reason before clearing annotations. A non-empty value means the stop was
	// triggered by a component signal; absence means the stop was requested via the API.
	stopMessage := "Pipeline stopped by user via API"
	if annotations := p.GetAnnotations(); annotations != nil {
		if reason, ok := annotations[constants.PipelineStopReasonAnnotation]; ok && reason != "" {
			stopMessage = reason
		}
	}

	r.clearStopLastPendingCount(&p)
	r.clearOperationAnnotationAndStatus(
		ctx,
		log,
		&p,
		constants.PipelineStopAnnotation,
		models.PipelineStatusStopped,
		true,
	)
	r.recordOperationSuccess(ctx, "stop", pipelineID, stopMessage)

	log.Info("pipeline stop completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileEdit(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline edit", "pipeline_id", pipelineID)

	namespace := r.getTargetNamespace(p)

	// Discover stale OTLP source streams early — before the timeout check — so that we can use
	// them to decide whether a timed-out edit should be extended (drain still making progress).
	var (
		staleNames      []string
		oldSubjectCount int
	)
	var err error
	if p.Spec.IsOTLPSource() {
		staleNames, oldSubjectCount, err = r.discoverStaleStreams(ctx, p)
		if err != nil {
			r.recordReconcileError(ctx, "edit", pipelineID, err)
			return ctrl.Result{}, fmt.Errorf("discover stale streams: %w", err)
		}
		if len(staleNames) > 0 {
			// Persist so the subject override survives passes after stale streams are deleted.
			if persistErr := r.setOTLPDownscaleSubjectCount(ctx, &p, oldSubjectCount); persistErr != nil {
				log.Error(persistErr, "failed to persist downscale subject count", "pipeline_id", pipelineID)
			}
		} else if saved, ok := getOTLPDownscaleSubjectCount(&p); ok {
			// Stale streams already gone — restore count from the previous pass.
			oldSubjectCount = saved
		}
	}

	// Timeout check — extend if a drain is still making progress (mirrors reconcileStop).
	timedOut, elapsed := r.checkOperationTimeout(log, &p)
	if timedOut {
		if len(staleNames) > 0 {
			if extended, extErr := r.tryExtendEditDrainTimeout(ctx, log, &p, staleNames); extErr != nil {
				log.Error(extErr, "failed to check drain progress during edit timeout", "pipeline_id", pipelineID)
			} else if extended {
				return ctrl.Result{Requeue: true, RequeueAfter: 2 * time.Second}, nil
			}
		}
		return r.handleOperationTimeout(ctx, log, &p)
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", p.Spec.ID, "elapsed", elapsed)
	}

	// Update the pipeline config secret with new config
	labels := preparePipelineLabels(p)
	secretName := types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p)}
	secret, err := r.updateSecret(ctx, secretName, labels, p)
	if err != nil {
		if errors.Is(err, ErrPipelineConfigSecretNotFound) {
			log.Info("pipeline config secret not found during edit, requeuing to wait for API to create it", "pipeline_id", pipelineID, "error", err)
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 2}, nil
		}
		return ctrl.Result{}, fmt.Errorf("update secret for edit: %w", err)
	}

	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusRunning) {
		log.Info("pipeline is already being resumed", "pipeline_id", p.Spec.ID)
		// Continue with the resume process
	} else {
		// Transition status to Resuming
		err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusResuming, nil, "edit")
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("update pipeline status to resuming: %w", err)
		}

		r.setOperationStartTimeBestEffort(ctx, log, &p)
	}

	// Get namespace for deployment creation
	var ns v1.Namespace
	err = r.Get(ctx, types.NamespacedName{Name: namespace}, &ns)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get namespace %s: %w", namespace, err)
	}

	// Dedup downscale protection: stale streams + dedup enabled means dedup replicas
	// were decreased, which corrupts pod-local Badger state.
	if p.Spec.IsOTLPSource() && len(staleNames) > 0 && p.Spec.Transform.IsDedupEnabled {
		msg := "dedup replica count cannot be decreased: dedup stores pod-local state"
		log.Info(msg, "pipeline_id", pipelineID, "stale_streams", staleNames)
		// Re-fetch to get the latest ResourceVersion — prior Status and annotation
		// writes may have bumped it, leaving our local copy stale.
		if err = r.Get(ctx, types.NamespacedName{Name: p.Name, Namespace: p.Namespace}, &p); err != nil {
			return ctrl.Result{}, fmt.Errorf("refresh pipeline before status update: %w", err)
		}
		if err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusFailed, []string{msg}, "edit"); err != nil {
			return ctrl.Result{}, fmt.Errorf("update pipeline status: %w", err)
		}
		return ctrl.Result{}, nil
	}

	if result, err := r.setupEditNATSStreams(ctx, log, pipelineID, p, staleNames, oldSubjectCount); err != nil || result.Requeue {
		return result, err
	}

	// Clean up PVCs for dedup instances that are now disabled.
	if err = r.cleanupDisabledDedupPVCs(ctx, log, p); err != nil {
		r.recordReconcileError(ctx, "edit", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("cleanup disabled dedup PVCs: %w", err)
	}

	if err = r.ensureComponentSecretsInPipelineNamespace(ctx, r.getTargetNamespace(p)); err != nil {
		r.recordReconcileError(ctx, "edit", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("ensure component secrets: %w", err)
	}

	// Ensure all deployments are ready
	result, err := r.createPipelineComponents(ctx, log, &p, ns, labels, secret)
	if err != nil || result.Requeue {
		return result, err
	}
	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusFailed) {
		return ctrl.Result{}, nil
	}

	// All deployments are ready, update status to Running
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusRunning, nil, "edit")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to running: %w", err)
	}

	r.clearOTLPDownscaleSubjectCount(&p)
	r.clearOperationAnnotationAndStatus(
		ctx,
		log,
		&p,
		constants.PipelineEditAnnotation,
		models.PipelineStatusRunning,
		true,
	)
	r.recordOperationSuccess(ctx, "edit", pipelineID, "")

	log.Info("pipeline edit completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

// -------------------------------------------------------------------------------------------------------------------
