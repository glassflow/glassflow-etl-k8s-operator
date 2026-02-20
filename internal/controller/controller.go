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
	postgresstorage "github.com/glassflow/glassflow-etl-k8s-operator/internal/storage/postgres"
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

// -------------------------------------------------------------------------------------------------------------------

// PipelineReconciler reconciles a Pipeline object
type PipelineReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Meter             *observability.Meter
	NATSClient        *nats.NATSClient
	PostgresStorage   *postgresstorage.PostgresStorage
	ComponentNATSAddr string
	// NATS stream configurations
	NATSMaxStreamAge   string
	NATSMaxStreamBytes string
	// Component image configurations
	IngestorImage string
	JoinImage     string
	SinkImage     string
	DedupImage    string
	// Component image pull policy configurations
	IngestorPullPolicy string
	JoinPullPolicy     string
	SinkPullPolicy     string
	DedupPullPolicy    string
	// Component resource configurations
	IngestorCPURequest    string
	IngestorCPULimit      string
	IngestorMemoryRequest string
	IngestorMemoryLimit   string
	JoinCPURequest        string
	JoinCPULimit          string
	JoinMemoryRequest     string
	JoinMemoryLimit       string
	SinkCPURequest        string
	SinkCPULimit          string
	SinkMemoryRequest     string
	SinkMemoryLimit       string
	DedupCPURequest       string
	DedupCPULimit         string
	DedupMemoryRequest    string
	DedupMemoryLimit      string
	// Dedup storage configurations
	DedupDefaultStorageSize  string
	DedupDefaultStorageClass string
	// Component affinity configurations
	IngestorAffinity string
	JoinAffinity     string
	SinkAffinity     string
	DedupAffinity    string
	// Observability configurations
	ObservabilityLogsEnabled    string
	ObservabilityMetricsEnabled string
	ObservabilityOTelEndpoint   string
	IngestorLogLevel            string
	JoinLogLevel                string
	SinkLogLevel                string
	DedupLogLevel               string
	IngestorImageTag            string
	JoinImageTag                string
	SinkImageTag                string
	DedupImageTag               string
	// Pipelines namespace configuration
	PipelinesNamespaceAuto bool
	PipelinesNamespaceName string

	GlassflowNamespace string // currently the same as operator namespace
	// Usage stats client
	UsageStatsClient *usagestats.Client
	// Usage stats configuration (values passed directly to components)
	UsageStatsEnabled        bool
	UsageStatsEndpoint       string
	UsageStatsUsername       string
	UsageStatsPassword       string
	UsageStatsInstallationID string
	// Cluster provider (e.g., GKE, EKS, IBM, etc.)
	ClusterProvider string

	// Component database and encryption (same as API). DatabaseURL is passed from operator env (GLASSFLOW_DATABASE_URL).
	DatabaseURL string
	// Encryption: when enabled, operator copies this secret into pipeline namespace and mounts at /etc/glassflow/secrets (same as API).
	EncryptionEnabled         bool
	EncryptionSecretName      string
	EncryptionSecretKey       string
	EncryptionSecretNamespace string
}

// -------------------------------------------------------------------------------------------------------------------

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
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

	// Check for operation annotations
	annotations := p.GetAnnotations()
	if annotations != nil {
		// Determine which operation to perform based on annotations
		// Helm uninstall has highest priority - it interrupts any ongoing operation
		var operation string
		if _, hasHelmUninstall := annotations[constants.PipelineHelmUninstallAnnotation]; hasHelmUninstall {
			operation = constants.OperationHelmUninstall
			log.Info("HELM UNINSTALL detected - interrupting any ongoing operations", "pipeline_id", p.Spec.ID)
		} else if _, hasTerminate := annotations[constants.PipelineDeleteAnnotation]; hasTerminate {
			operation = constants.OperationDelete
		} else if _, hasTerminate := annotations[constants.PipelineTerminateAnnotation]; hasTerminate {
			operation = constants.OperationTerminate
		} else if _, hasCreate := annotations[constants.PipelineCreateAnnotation]; hasCreate {
			operation = constants.OperationCreate
		} else if _, hasStop := annotations[constants.PipelineStopAnnotation]; hasStop {
			operation = constants.OperationStop
		} else if _, hasResume := annotations[constants.PipelineResumeAnnotation]; hasResume {
			operation = constants.OperationResume
		} else if _, hasEdit := annotations[constants.PipelineEditAnnotation]; hasEdit {
			operation = constants.OperationEdit
		}

		// Execute the appropriate operation
		switch operation {
		case constants.OperationHelmUninstall:
			return r.reconcileHelmUninstall(ctx, log, p)
		case constants.OperationCreate:
			return r.createPipeline(ctx, p)
		case constants.OperationResume:
			return r.reconcileResume(ctx, log, p)
		case constants.OperationStop:
			return r.reconcileStop(ctx, log, p)
		case constants.OperationEdit:
			return r.reconcileEdit(ctx, log, p)
		case constants.OperationTerminate:
			return r.reconcileTerminate(ctx, log, p)
		case constants.OperationDelete:
			return r.reconcileDelete(ctx, log, p)
		}
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

	// Check for timeout before proceeding
	timedOut, elapsed := r.checkOperationTimeout(log, &p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, &p, constants.OperationCreate)
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", pipelineID, "elapsed", elapsed)
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

		// Set operation start time when transitioning to Created status
		err = r.setOperationStartTime(ctx, &p)
		if err != nil {
			log.Error(err, "failed to set operation start time", "pipeline_id", pipelineID)
			// Continue anyway - this is not critical
		}
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

	secretName := r.getResourceName(p, constants.SecretSuffix)
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
	result, err := r.ensureAllDeploymentsReady(ctx, log, &p, ns, labels, secret, constants.OperationCreate)
	if err != nil || result.Requeue {
		return result, err
	}

	// All deployments are ready, update status to Running
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusRunning, nil)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to running: %w", err)
	}

	// Remove create annotation and clear operation start time
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineCreateAnnotation)
		r.clearOperationStartTime(&p)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(models.PipelineStatusRunning)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove create annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "create", "success", pipelineID)
	})

	// Send usage stats event for reconcile success
	r.sendReconcileSuccessEvent(ctx, "create", pipelineID)

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

	// Check for timeout before proceeding
	timedOut, elapsed := r.checkOperationTimeout(log, &p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, &p, constants.OperationTerminate)
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", pipelineID, "elapsed", elapsed)
	}

	// Set operation start time if not already set (for timeout usage stats)
	err := r.setOperationStartTime(ctx, &p)
	if err != nil {
		log.Error(err, "failed to set operation start time", "pipeline_id", pipelineID)
		// Continue anyway - this is not critical
	}

	// Stop all pipeline components
	result, err := r.terminatePipelineComponents(ctx, log, p)
	if err != nil || result.Requeue {
		return result, err
	}

	// Update pipeline status to "Stopped"
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusStopped, nil)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to stopped: %w", err)
	}

	// Remove terminate annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineTerminateAnnotation)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(models.PipelineStatusStopped)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove terminate annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "terminate", "success", pipelineID)
	})

	// Send usage stats event for reconcile success
	r.sendReconcileSuccessEvent(ctx, "terminate", pipelineID)

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
	if r.PostgresStorage != nil {
		err = r.PostgresStorage.DeletePipeline(ctx, p.Spec.ID)
		if err != nil {
			log.Info("failed to delete pipeline configuration from PostgreSQL", "pipeline_id", p.Spec.ID, "error", err)
			// Don't return error here - we're in force cleanup mode
		} else {
			log.Info("successfully deleted pipeline configuration from PostgreSQL", "pipeline_id", p.Spec.ID)
		}
	}

	// Remove terminate annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineDeleteAnnotation)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(models.PipelineStatusStopped)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove terminate annotation", "pipeline_id", p.Spec.ID)
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

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "delete", "success", pipelineID)
	})

	// Send usage stats event for reconcile success
	r.sendReconcileSuccessEvent(ctx, "delete", pipelineID)

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
	if r.PostgresStorage != nil {
		err = r.PostgresStorage.DeletePipeline(ctx, pipelineID)
		if err != nil {
			log.Info("failed to delete pipeline configuration from PostgreSQL", "pipeline_id", pipelineID, "error", err)
			// Don't return error here - we're in force cleanup mode
		} else {
			log.Info("successfully deleted pipeline configuration from PostgreSQL", "pipeline_id", pipelineID)
		}
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

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "uninstall", "success", pipelineID)
	})

	// Send usage stats event for reconcile success
	r.sendReconcileSuccessEvent(ctx, "helm-uninstall", pipelineID)

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

	// Check for timeout before proceeding
	timedOut, elapsed := r.checkOperationTimeout(log, &p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, &p, constants.OperationResume)
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", pipelineID, "elapsed", elapsed)
	}

	if p.Status == etlv1alpha1.PipelineStatus(models.PipelineStatusResuming) {
		log.Info("pipeline is already being resumed", "pipeline_id", p.Spec.ID)
		// Continue with the resume process
	} else {
		// Transition to Resuming status first
		err := r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusResuming, nil)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("update pipeline status to resuming: %w", err)
		}

		// Set operation start time when transitioning to Resuming status
		err = r.setOperationStartTime(ctx, &p)
		if err != nil {
			log.Error(err, "failed to set operation start time", "pipeline_id", pipelineID)
			// Continue anyway - this is not critical
		}

		// Requeue to continue with the resume process
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}

	// Get namespace
	var ns v1.Namespace
	err := r.Get(ctx, types.NamespacedName{Name: namespace}, &ns)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get namespace %s: %w", namespace, err)
	}

	// Get secret
	secretName := types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p, constants.SecretSuffix)}
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
	result, err := r.ensureAllDeploymentsReady(ctx, log, &p, ns, labels, secret, constants.OperationResume)
	if err != nil || result.Requeue {
		return result, err
	}

	// All deployments are ready, update status to Running
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusRunning, nil)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to running: %w", err)
	}

	// Remove resume annotation and clear operation start time
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineResumeAnnotation)
		r.clearOperationStartTime(&p)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(models.PipelineStatusRunning)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove resume annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "resume", "success", pipelineID)
	})

	// Send usage stats event for reconcile success
	r.sendReconcileSuccessEvent(ctx, "resume", pipelineID)

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

	// Check for timeout before proceeding
	timedOut, elapsed := r.checkOperationTimeout(log, &p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, &p, constants.OperationStop)
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
		err := r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusStopping, nil)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("update pipeline status to stopping: %w", err)
		}

		// Set operation start time when transitioning to Stopping status
		err = r.setOperationStartTime(ctx, &p)
		if err != nil {
			log.Error(err, "failed to set operation start time", "pipeline_id", pipelineID)
			// Continue anyway - this is not critical
		}

		// Requeue to continue with the stop process
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}

	// Stop all pipeline components
	result, err := r.stopPipelineComponents(ctx, log, &p)
	if err != nil || result.Requeue {
		return result, err
	}

	// Update status to Stopped
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusStopped, nil)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to stopped: %w", err)
	}

	// Remove stop annotation and clear operation start time
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineStopAnnotation)
		r.clearOperationStartTime(&p)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(models.PipelineStatusStopped)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove stop annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "stop", "success", pipelineID)
	})

	// Send usage stats event for reconcile success
	r.sendReconcileSuccessEvent(ctx, "stop", pipelineID)

	log.Info("pipeline stop completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileEdit(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline edit", "pipeline_id", pipelineID)

	namespace := r.getTargetNamespace(p)

	// Check for timeout before proceeding
	timedOut, elapsed := r.checkOperationTimeout(log, &p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, &p, constants.OperationEdit)
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", pipelineID, "elapsed", elapsed)
	}

	// Update the pipeline config secret with new config
	labels := preparePipelineLabels(p)
	secretName := types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p, constants.SecretSuffix)}
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
		err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusResuming, nil)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("update pipeline status to resuming: %w", err)
		}

		// Set operation start time when transitioning to Resuming status
		err = r.setOperationStartTime(ctx, &p)
		if err != nil {
			log.Error(err, "failed to set operation start time", "pipeline_id", pipelineID)
			// Continue anyway - this is not critical
		}
	}

	// Get namespace for deployment creation
	var ns v1.Namespace
	err = r.Get(ctx, types.NamespacedName{Name: namespace}, &ns)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get namespace %s: %w", namespace, err)
	}

	err = r.createNATSStreams(ctx, p)
	if err != nil {
		r.recordReconcileError(ctx, "edit", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("setup streams: %w", err)
	}

	if err = r.ensureComponentSecretsInPipelineNamespace(ctx, r.getTargetNamespace(p)); err != nil {
		r.recordReconcileError(ctx, "edit", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("ensure component secrets: %w", err)
	}

	// Ensure all deployments are ready
	result, err := r.ensureAllDeploymentsReady(ctx, log, &p, ns, labels, secret, constants.OperationEdit)
	if err != nil || result.Requeue {
		return result, err
	}

	// All deployments are ready, update status to Running
	err = r.updatePipelineStatus(ctx, log, &p, models.PipelineStatusRunning, nil)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to running: %w", err)
	}

	// Remove edit annotation and clear operation start time
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineEditAnnotation)
		r.clearOperationStartTime(&p)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(models.PipelineStatusRunning)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove edit annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "edit", "success", pipelineID)
	})

	// Send usage stats event for reconcile success
	r.sendReconcileSuccessEvent(ctx, "edit", pipelineID)

	log.Info("pipeline edit completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

// -------------------------------------------------------------------------------------------------------------------

// ensureDedupStatefulSetsReady ensures all dedup StatefulSets are ready for a pipeline.
// It checks if any streams have deduplication enabled, verifies their readiness,
// creates them if needed, and handles timeout checking.
//
// Returns:
//   - ctrl.Result: Requeue result if dedups need more time to become ready
//   - error: Error if readiness check or creation fails
func (r *PipelineReconciler) ensureDedupStatefulSetsReady(
	ctx context.Context,
	log logr.Logger,
	p etlv1alpha1.Pipeline,
	ns v1.Namespace,
	labels map[string]string,
	secret v1.Secret,
	operationName string,
) (ctrl.Result, error) {
	namespace := r.getTargetNamespace(p)

	// Step 3: Create Dedup StatefulSets (if any stream has dedup enabled)
	anyDedupEnabled := false
	for _, stream := range p.Spec.Ingestor.Streams {
		if stream.Deduplication != nil && stream.Deduplication.Enabled {
			anyDedupEnabled = true
			break
		}
	}

	if anyDedupEnabled {
		// Check if all dedup StatefulSets are ready
		allDedupReady := true
		for i, stream := range p.Spec.Ingestor.Streams {
			if stream.Deduplication == nil || !stream.Deduplication.Enabled {
				continue
			}

			dedupName := r.getResourceName(p, fmt.Sprintf("%s-%d", constants.DedupComponent, i))
			ready, err := r.isStatefulSetReady(ctx, namespace, dedupName)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("check dedup-%d statefulset: %w", i, err)
			}
			if !ready {
				allDedupReady = false
				break
			}
		}

		if !allDedupReady {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(log, &p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, &p, operationName)
			}

			log.Info("creating dedup statefulsets", "namespace", namespace)
			err := r.createDedups(ctx, log, ns, labels, secret, p)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("create dedup statefulsets: %w", err)
			}
			log.Info("waiting for dedup statefulsets to be ready", "namespace", namespace)
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("dedup statefulsets are ready", "namespace", namespace)
		}
	}

	return ctrl.Result{}, nil
}

// ensureDeploymentReady checks if a deployment is ready, creates it if not, and handles timeouts.
func (r *PipelineReconciler) ensureDeploymentReady(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	namespace,
	deploymentName,
	operationName string,
	createFn func(context.Context, v1.Namespace, map[string]string, v1.Secret, etlv1alpha1.Pipeline) error,
	ns v1.Namespace,
	labels map[string]string,
	secret v1.Secret,
) (bool, error) {
	ready, err := r.isDeploymentReady(ctx, namespace, deploymentName)
	if err != nil {
		return false, fmt.Errorf("check %s deployment: %w", deploymentName, err)
	}
	if ready {
		log.Info(fmt.Sprintf("%s deployment is already ready", deploymentName), "namespace", namespace)
		return false, nil
	}

	// Check for timeout before creating
	timedOut, _ := r.checkOperationTimeout(log, p)
	if timedOut {
		_, err := r.handleOperationTimeout(ctx, log, p, operationName)
		return false, err
	}

	log.Info(fmt.Sprintf("creating %s deployment", deploymentName), "namespace", namespace)
	err = createFn(ctx, ns, labels, secret, *p)
	if err != nil {
		return false, fmt.Errorf("create %s deployment: %w", deploymentName, err)
	}
	return true, nil
}

// ensureStatefulSetReady checks if a StatefulSet is ready, creates it if not, and handles timeouts.
func (r *PipelineReconciler) ensureStatefulSetReady(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	namespace,
	statefulSetName,
	operationName string,
	createFn func(context.Context, v1.Namespace, map[string]string, v1.Secret, etlv1alpha1.Pipeline) error,
	ns v1.Namespace,
	labels map[string]string,
	secret v1.Secret,
) (bool, error) {
	ready, err := r.isStatefulSetReady(ctx, namespace, statefulSetName)
	if err != nil {
		return false, fmt.Errorf("check %s statefulset: %w", statefulSetName, err)
	}
	if ready {
		log.Info(fmt.Sprintf("%s statefulset is already ready", statefulSetName), "namespace", namespace)
		return false, nil
	}

	timedOut, _ := r.checkOperationTimeout(log, p)
	if timedOut {
		_, err := r.handleOperationTimeout(ctx, log, p, operationName)
		return false, err
	}

	log.Info(fmt.Sprintf("creating %s statefulset", statefulSetName), "namespace", namespace)
	err = createFn(ctx, ns, labels, secret, *p)
	if err != nil {
		return false, fmt.Errorf("create %s statefulset: %w", statefulSetName, err)
	}
	return true, nil
}

// ensureAllDeploymentsReady ensures all required components are ready: Sink StatefulSet, Join deployment (if enabled), Dedup StatefulSets, Ingestor StatefulSets.
func (r *PipelineReconciler) ensureAllDeploymentsReady(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	ns v1.Namespace,
	labels map[string]string,
	secret v1.Secret,
	operationName string,
) (ctrl.Result, error) {
	namespace := r.getTargetNamespace(*p)

	// Step 1: Ensure Sink StatefulSet is ready
	requeue, err := r.ensureStatefulSetReady(ctx, log, p, namespace, r.getResourceName(*p, constants.SinkComponent), operationName, r.createSink, ns, labels, secret)
	if err != nil {
		return ctrl.Result{}, err
	}
	if requeue {
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}

	// Step 2: Ensure Join deployment is ready (if enabled)
	if p.Spec.Join.Enabled {
		requeue, err := r.ensureDeploymentReady(ctx, log, p, namespace, r.getResourceName(*p, constants.JoinComponent), operationName, r.createJoin, ns, labels, secret)
		if err != nil {
			return ctrl.Result{}, err
		}
		if requeue {
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		}
	}

	// Step 3: Ensure Dedup StatefulSets are ready
	result, err := r.ensureDedupStatefulSetsReady(ctx, log, *p, ns, labels, secret, operationName)
	if err != nil || result.Requeue {
		return result, err
	}

	// Step 4: Ensure Ingestor StatefulSets are ready
	for i := range p.Spec.Ingestor.Streams {
		statefulSetName := r.getResourceName(*p, fmt.Sprintf("%s-%d", constants.IngestorComponent, i))
		ready, err := r.isStatefulSetReady(ctx, namespace, statefulSetName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor statefulset %s: %w", statefulSetName, err)
		}
		if !ready {
			timedOut, _ := r.checkOperationTimeout(log, p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, p, operationName)
			}
			log.Info("creating ingestor statefulset", "statefulset", statefulSetName, "namespace", namespace)
			err = r.createIngestors(ctx, log, ns, labels, secret, *p)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("create ingestor statefulset %s: %w", statefulSetName, err)
			}
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		}
		log.Info("ingestor statefulset is already ready", "statefulset", statefulSetName, "namespace", namespace)
	}

	return ctrl.Result{}, nil
}
