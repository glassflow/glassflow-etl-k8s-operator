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
	"fmt"
	"maps"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/utils"
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
	ComponentNATSAddr string
	// NATS stream configurations
	NATSMaxStreamAge   string
	NATSMaxStreamBytes string
	// Component image configurations
	IngestorImage string
	JoinImage     string
	SinkImage     string
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
	// Component affinity configurations
	IngestorAffinity string
	JoinAffinity     string
	SinkAffinity     string
	// Observability configurations
	ObservabilityLogsEnabled    string
	ObservabilityMetricsEnabled string
	ObservabilityOTelEndpoint   string
	IngestorLogLevel            string
	JoinLogLevel                string
	SinkLogLevel                string
	IngestorImageTag            string
	JoinImageTag                string
	SinkImageTag                string
	// Pipelines namespace configuration
	PipelinesNamespaceAuto bool
	PipelinesNamespaceName string
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
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;delete
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
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusRunning) {
		log.Info("pipeline already running", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}

	// Check for timeout before proceeding
	timedOut, elapsed := r.checkOperationTimeout(ctx, log, &p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, &p, constants.OperationCreate)
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", pipelineID, "elapsed", elapsed)
	}

	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusCreated) {
		log.Info("pipeline is already being created", "pipeline_id", p.Spec.ID)
		// Continue with the creation process
	} else {
		// Transition to Creation status first
		p.Status = etlv1alpha1.PipelineStatus(nats.PipelineStatusCreated)

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

	secretName := r.getResourceName(p, p.Spec.ID)
	secret, err := r.createSecret(ctx, types.NamespacedName{Namespace: ns.GetName(), Name: secretName}, labels, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("create secret for pipeline config %s: %w", p.Spec.ID, err)
	}

	namespace := r.getTargetNamespace(p)

	// Step 1: Create Sink deployment
	ready, err := r.isDeploymentReady(ctx, namespace, r.getResourceName(p, "sink"))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !ready {
		// Check for timeout before requeuing
		timedOut, _ := r.checkOperationTimeout(ctx, log, &p)
		if timedOut {
			return r.handleOperationTimeout(ctx, log, &p, "create")
		}

		log.Info("creating sink deployment", "namespace", namespace)
		err = r.createSink(ctx, ns, labels, secret, p)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("create sink deployment: %w", err)
		}
		// Requeue to wait for deployment to be ready
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	} else {
		log.Info("sink deployment is ready", "namespace", namespace)
	}

	// Step 2: Create Join deployment (if enabled)
	if p.Spec.Join.Enabled {
		ready, err := r.isDeploymentReady(ctx, namespace, r.getResourceName(p, "join"))
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !ready {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(ctx, log, &p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, &p, "create")
			}

			log.Info("creating join deployment", "namespace", namespace)
			err := r.createJoin(ctx, ns, labels, secret, p)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("create join deployment: %w", err)
			}
			// Requeue to wait for deployment to be ready
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("join deployment is already created", "namespace", namespace)
		}
	}

	// Step 3: Create Ingestor deployments
	for i := range p.Spec.Ingestor.Streams {
		deploymentName := r.getResourceName(p, fmt.Sprintf("ingestor-%d", i))
		ready, err := r.isDeploymentReady(ctx, namespace, deploymentName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor deployment %s: %w", deploymentName, err)
		}
		if !ready {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(ctx, log, &p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, &p, "create")
			}

			log.Info("creating ingestor deployment", "deployment", deploymentName, "namespace", namespace)
			err = r.createIngestors(ctx, log, ns, labels, secret, p)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("create ingestor deployment %s: %w", deploymentName, err)
			}
			// Requeue to wait for deployment to be ready
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("ingestor deployment is already created", "namespace", namespace)
		}
	}

	// All deployments are ready, update status to Running
	err = r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusRunning)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to running: %w", err)
	}

	// Remove create annotation and clear operation start time
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineCreateAnnotation)
		r.clearOperationStartTime(&p)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(nats.PipelineStatusRunning)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove create annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "create", "success", pipelineID)
	})

	log.Info("pipeline creation completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileTerminate(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline termination", "pipeline_id", pipelineID)

	// Check if pipeline is already stopped
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusStopped) {
		log.Info("pipeline already stopped", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}

	// Stop all pipeline components
	result, err := r.terminatePipelineComponents(ctx, log, p)
	if err != nil || result.Requeue {
		return result, err
	}

	// Update pipeline status to "Stopped"
	err = r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusStopped)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to stopped: %w", err)
	}

	// Remove terminate annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineTerminateAnnotation)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(nats.PipelineStatusStopped)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove terminate annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "terminate", "success", pipelineID)
	})

	log.Info("pipeline termination completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileDelete(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline deletion", "pipeline_id", pipelineID)

	// Check if pipeline is stopped
	if p.Status != etlv1alpha1.PipelineStatus(nats.PipelineStatusStopped) {
		log.Info("pipeline is not stopped but attempting to delete", "pipeline_id", p.Spec.ID)
	}

	// Delete namespace for this pipeline
	err := r.deleteNamespace(ctx, log, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("delete pipeline namespace: %w", err)
	}

	// Clean up NATS streams
	err = r.cleanupNATSPipelineResources(ctx, log, p)
	if err != nil {
		log.Info("failed to cleanup NATS resources during termination", "pipeline_id", p.Spec.ID)
		// Don't return error here as namespace is already deleted
		// Just log and continue
	}

	// Clean up pipeline configuration from NATS KV store
	if r.NATSClient != nil {
		err = r.NATSClient.DeletePipeline(ctx, p.Spec.ID)
		if err != nil {
			log.Info("failed to delete pipeline configuration from NATS KV store", "pipeline_id", p.Spec.ID)
			// Don't return error here - we're in force cleanup mode
		} else {
			log.Info("successfully deleted pipeline configuration from NATS KV store", "pipeline_id", p.Spec.ID)
		}
	}

	// Remove terminate annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineDeleteAnnotation)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(nats.PipelineStatusStopped)
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

	log.Info("pipeline deletion completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileHelmUninstall(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID
	log.Info("reconciling pipeline helm uninstall - FORCING cleanup regardless of current status", "pipeline_id", pipelineID, "current_status", p.Status)

	// FORCE cleanup regardless of current status - this is helm uninstall!
	log.Info("HELM UNINSTALL: Forcing immediate cleanup of pipeline", "pipeline_id", pipelineID)

	// Check if pipeline is already stopped
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusStopped) {
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

	// Force delete namespace for this pipeline (this will delete all deployments)
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

	// Clean up pipeline configuration from NATS KV store
	if r.NATSClient != nil {
		err = r.NATSClient.DeletePipeline(ctx, pipelineID)
		if err != nil {
			log.Info("failed to delete pipeline configuration from NATS KV store", "pipeline_id", pipelineID)
			// Don't return error here - we're in force cleanup mode
		} else {
			log.Info("successfully deleted pipeline configuration from NATS KV store", "pipeline_id", pipelineID)
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

	log.Info("pipeline helm uninstall completed successfully - FORCE CLEANUP", "pipeline", p.Name, "pipeline_id", pipelineID)
	return ctrl.Result{}, nil
}

// checkConsumerPendingMessages checks if a specific consumer has pending messages
func (r *PipelineReconciler) checkConsumerPendingMessages(ctx context.Context, streamName, consumerName string) error {
	hasPending, pending, unack, err := r.NATSClient.CheckConsumerPendingMessages(ctx, streamName, consumerName)
	if err != nil {
		return fmt.Errorf("check consumer %s: %w", consumerName, err)
	}
	if hasPending {
		return fmt.Errorf("consumer %s has %d pending and %d unacknowledged messages", consumerName, pending, unack)
	}

	return nil
}

// checkJoinPendingMessages checks if join consumers have pending messages
func (r *PipelineReconciler) checkJoinPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline) error {
	if !p.Spec.Join.Enabled {
		return nil // No join, nothing to check
	}

	// Get consumer names and stream names directly from spec
	leftConsumerName := p.Spec.Join.NATSLeftConsumerName
	rightConsumerName := p.Spec.Join.NATSRightConsumerName
	leftStreamName := p.Spec.Ingestor.Streams[0].OutputStream
	rightStreamName := p.Spec.Ingestor.Streams[1].OutputStream

	// Check left stream
	err := r.checkConsumerPendingMessages(ctx, leftStreamName, leftConsumerName)
	if err != nil {
		return fmt.Errorf("left join consumer: %w", err)
	}

	// Check right stream
	err = r.checkConsumerPendingMessages(ctx, rightStreamName, rightConsumerName)
	if err != nil {
		return fmt.Errorf("right join consumer: %w", err)
	}

	return nil
}

// checkSinkPendingMessages checks if sink consumer has pending messages
func (r *PipelineReconciler) checkSinkPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline) error {
	// Get consumer name and stream name directly from spec
	sinkConsumerName := p.Spec.Sink.NATSConsumerName

	// Get stream name based on whether join is enabled
	var sinkStreamName string
	if p.Spec.Join.Enabled {
		sinkStreamName = p.Spec.Join.OutputStream
	} else {
		sinkStreamName = p.Spec.Ingestor.Streams[0].OutputStream
	}

	// Check sink stream
	err := r.checkConsumerPendingMessages(ctx, sinkStreamName, sinkConsumerName)
	if err != nil {
		return fmt.Errorf("sink consumer: %w", err)
	}

	return nil
}

// stopPipelineComponents stops all pipeline components in the correct order with pending message checks
func (r *PipelineReconciler) stopPipelineComponents(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline) (ctrl.Result, error) {
	namespace := r.getTargetNamespace(*p)

	// Step 1: Stop Ingestor deployments
	for i := range p.Spec.Ingestor.Streams {
		deploymentName := r.getResourceName(*p, fmt.Sprintf("ingestor-%d", i))
		deleted, err := r.isDeploymentAbsent(ctx, namespace, deploymentName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor deployment %s: %w", deploymentName, err)
		}
		if !deleted {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(ctx, log, p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, p, constants.OperationStop)
			}

			log.Info("deleting ingestor deployment", "deployment", deploymentName, "namespace", namespace)
			var deployment appsv1.Deployment
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: deploymentName}, &deployment)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("get ingestor deployment %s: %w", deploymentName, err)
				}
			} else {
				err = r.deleteDeployment(ctx, &deployment)
				if err != nil {
					return ctrl.Result{}, fmt.Errorf("delete ingestor deployment %s: %w", deploymentName, err)
				}
			}
			// Requeue to wait for deployment to be fully deleted
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("ingestor deployment is already deleted", "deployment", deploymentName, "namespace", namespace)
		}
	}

	// Step 2: Check join and stop Join deployment (if enabled)
	if p.Spec.Join.Enabled {
		// Check for timeout before checking pending messages
		timedOut, _ := r.checkOperationTimeout(ctx, log, p)
		if timedOut {
			return r.handleOperationTimeout(ctx, log, p, constants.OperationStop)
		}

		// Check for pending messages first
		err := r.checkJoinPendingMessages(ctx, *p)
		if err != nil {
			log.Info("join has pending messages, requeuing operation",
				"pipeline_id", p.Spec.ID,
				"error", err.Error())
			return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
		}

		deleted, err := r.isDeploymentAbsent(ctx, namespace, r.getResourceName(*p, "join"))
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !deleted {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(ctx, log, p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, p, constants.OperationStop)
			}

			log.Info("deleting join deployment", "namespace", namespace)
			var deployment appsv1.Deployment
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: r.getResourceName(*p, "join")}, &deployment)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("get join deployment: %w", err)
				}
			} else {
				err = r.deleteDeployment(ctx, &deployment)
				if err != nil {
					return ctrl.Result{}, fmt.Errorf("delete join deployment: %w", err)
				}
			}
			// Requeue to wait for deployment to be fully deleted
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("join deployment is already deleted", "namespace", namespace)
		}
	}

	// Step 3: Check sink and stop Sink deployment
	// Check for timeout before checking pending messages
	timedOut, _ := r.checkOperationTimeout(ctx, log, p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, p, "stop")
	}

	// Check for pending messages first
	err := r.checkSinkPendingMessages(ctx, *p)
	if err != nil {
		log.Info("sink has pending messages, requeuing operation",
			"pipeline_id", p.Spec.ID,
			"error", err.Error())
		return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
	}

	deleted, err := r.isDeploymentAbsent(ctx, namespace, r.getResourceName(*p, "sink"))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !deleted {
		// Check for timeout before requeuing
		timedOut, _ := r.checkOperationTimeout(ctx, log, p)
		if timedOut {
			return r.handleOperationTimeout(ctx, log, p, constants.OperationStop)
		}

		log.Info("deleting sink deployment", "namespace", namespace)
		var deployment appsv1.Deployment
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: r.getResourceName(*p, "sink")}, &deployment)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("get sink deployment: %w", err)
			}
		} else {
			err = r.deleteDeployment(ctx, &deployment)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete sink deployment: %w", err)
			}
		}
		// Requeue to wait for deployment to be fully deleted
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	} else {
		log.Info("sink deployment is already deleted", "namespace", namespace)
	}

	// All deployments are deleted
	return ctrl.Result{}, nil
}

// terminatePipelineComponents terminates all pipeline components immediately
func (r *PipelineReconciler) terminatePipelineComponents(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	namespace := r.getTargetNamespace(p)

	// Step 1: Stop Ingestor deployments
	for i := range p.Spec.Ingestor.Streams {
		deploymentName := r.getResourceName(p, fmt.Sprintf("ingestor-%d", i))
		deleted, err := r.isDeploymentAbsent(ctx, namespace, deploymentName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor deployment %s: %w", deploymentName, err)
		}
		if !deleted {
			log.Info("deleting ingestor deployment", "deployment", deploymentName, "namespace", namespace)
			var deployment appsv1.Deployment
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: deploymentName}, &deployment)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("get ingestor deployment %s: %w", deploymentName, err)
				}
			} else {
				err = r.deleteDeployment(ctx, &deployment)
				if err != nil {
					return ctrl.Result{}, fmt.Errorf("delete ingestor deployment %s: %w", deploymentName, err)
				}
			}
			// Requeue to wait for deployment to be fully deleted
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("ingestor deployment is already deleted", "deployment", deploymentName, "namespace", namespace)
		}
	}

	// Step 2: Check join and stop Join deployment (if enabled)
	if p.Spec.Join.Enabled {
		deleted, err := r.isDeploymentAbsent(ctx, namespace, r.getResourceName(p, "join"))
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !deleted {
			log.Info("deleting join deployment", "namespace", namespace)
			var deployment appsv1.Deployment
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p, "join")}, &deployment)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("get join deployment: %w", err)
				}
			} else {
				err = r.deleteDeployment(ctx, &deployment)
				if err != nil {
					return ctrl.Result{}, fmt.Errorf("delete join deployment: %w", err)
				}
			}
			// Requeue to wait for deployment to be fully deleted
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("join deployment is already deleted", "namespace", namespace)
		}
	}

	// Step 3: Check sink and stop Sink deployment
	deleted, err := r.isDeploymentAbsent(ctx, namespace, r.getResourceName(p, "sink"))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !deleted {
		log.Info("deleting sink deployment", "namespace", namespace)
		var deployment appsv1.Deployment
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p, "sink")}, &deployment)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("get sink deployment: %w", err)
			}
		} else {
			err = r.deleteDeployment(ctx, &deployment)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete sink deployment: %w", err)
			}
		}
		// Requeue to wait for deployment to be fully deleted
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	} else {
		log.Info("sink deployment is already deleted", "namespace", namespace)
	}

	// All deployments are deleted
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileResume(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline resume", "pipeline_id", pipelineID)

	namespace := r.getTargetNamespace(p)

	// Check if pipeline is already running or resuming
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusRunning) {
		log.Info("pipeline already running", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}

	// Check for timeout before proceeding
	timedOut, elapsed := r.checkOperationTimeout(ctx, log, &p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, &p, constants.OperationResume)
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", pipelineID, "elapsed", elapsed)
	}

	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusResuming) {
		log.Info("pipeline is already being resumed", "pipeline_id", p.Spec.ID)
		// Continue with the resume process
	} else {
		// Transition to Resuming status first
		err := r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusResuming)
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
	secretName := types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p, p.Spec.ID)}
	var secret v1.Secret
	err = r.Get(ctx, secretName, &secret)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get secret %s: %w", secretName, err)
	}

	labels := preparePipelineLabels(p)

	// Step 1: Create Sink deployment
	ready, err := r.isDeploymentReady(ctx, namespace, r.getResourceName(p, "sink"))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !ready {
		// Check for timeout before requeuing
		timedOut, _ := r.checkOperationTimeout(ctx, log, &p)
		if timedOut {
			return r.handleOperationTimeout(ctx, log, &p, constants.OperationResume)
		}

		log.Info("creating sink deployment", "namespace", namespace)
		err = r.createSink(ctx, ns, labels, secret, p)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("create sink deployment: %w", err)
		}
		// Requeue to wait for deployment to be ready
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	} else {
		log.Info("sink deployment is already created", "namespace", namespace)
	}

	// Step 2: Create Join deployment (if enabled)
	if p.Spec.Join.Enabled {
		ready, err := r.isDeploymentReady(ctx, namespace, r.getResourceName(p, "join"))
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !ready {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(ctx, log, &p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, &p, constants.OperationResume)
			}

			log.Info("creating join deployment", "namespace", namespace)
			err = r.createJoin(ctx, ns, labels, secret, p)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("create join deployment: %w", err)
			}
			// Requeue to wait for deployment to be ready
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("join deployment is already created", "namespace", namespace)
		}
	}

	// Step 3: Create Ingestor deployments
	for i := range p.Spec.Ingestor.Streams {
		deploymentName := r.getResourceName(p, fmt.Sprintf("ingestor-%d", i))
		ready, err := r.isDeploymentReady(ctx, namespace, deploymentName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor deployment %s: %w", deploymentName, err)
		}
		if !ready {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(ctx, log, &p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, &p, constants.OperationResume)
			}

			log.Info("creating ingestor deployment", "deployment", deploymentName, "namespace", namespace)
			err = r.createIngestors(ctx, log, ns, labels, secret, p)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("create ingestor deployment %s: %w", deploymentName, err)
			}
			// Requeue to wait for deployment to be ready
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("ingestor deployment is already created", "namespace", namespace)
		}
	}

	// All deployments are ready, update status to Running
	err = r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusRunning)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to running: %w", err)
	}

	// Remove resume annotation and clear operation start time
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineResumeAnnotation)
		r.clearOperationStartTime(&p)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(nats.PipelineStatusRunning)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove resume annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "resume", "success", pipelineID)
	})

	log.Info("pipeline resume completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileStop(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline stop", "pipeline_id", pipelineID)

	// Check if pipeline is already stopped
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusStopped) {
		log.Info("pipeline already stopped", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}

	// Check for timeout before proceeding
	timedOut, elapsed := r.checkOperationTimeout(ctx, log, &p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, &p, constants.OperationStop)
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", pipelineID, "elapsed", elapsed)
	}

	// Check if pipeline is already stopping
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusStopping) {
		log.Info("pipeline is already being stopped", "pipeline_id", p.Spec.ID)
		// Continue with the stop process
	} else {
		// Transition to Stopping status first
		err := r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusStopping)
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
	err = r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusStopped)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to stopped: %w", err)
	}

	// Remove stop annotation and clear operation start time
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineStopAnnotation)
		r.clearOperationStartTime(&p)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(nats.PipelineStatusStopped)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove stop annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "stop", "success", pipelineID)
	})

	log.Info("pipeline stop completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileEdit(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	pipelineID := p.Spec.ID

	log.Info("reconciling pipeline edit", "pipeline_id", pipelineID)

	namespace := r.getTargetNamespace(p)

	// Check for timeout before proceeding
	timedOut, elapsed := r.checkOperationTimeout(ctx, log, &p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, &p, constants.OperationEdit)
	}
	if elapsed > 0 {
		log.Info("operation in progress", "pipeline_id", pipelineID, "elapsed", elapsed)
	}

	// Update the pipeline config secret with new config
	labels := preparePipelineLabels(p)
	secretName := types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p, p.Spec.ID)}
	_, err := r.updateSecret(ctx, secretName, labels, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update secret for edit: %w", err)
	}

	// Transition status to Resuming
	err = r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusResuming)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to resuming: %w", err)
	}

	// Set operation start time when transitioning to Resuming status
	err = r.setOperationStartTime(ctx, &p)
	if err != nil {
		log.Error(err, "failed to set operation start time", "pipeline_id", pipelineID)
		// Continue anyway - this is not critical
	}

	// Get namespace and secret for deployment creation
	var ns v1.Namespace
	err = r.Get(ctx, types.NamespacedName{Name: namespace}, &ns)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get namespace %s: %w", namespace, err)
	}

	var secret v1.Secret
	err = r.Get(ctx, secretName, &secret)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get secret %s: %w", secretName, err)
	}

	// Step 1: Create Sink deployment
	ready, err := r.isDeploymentReady(ctx, namespace, r.getResourceName(p, "sink"))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !ready {
		// Check for timeout before requeuing
		timedOut, _ := r.checkOperationTimeout(ctx, log, &p)
		if timedOut {
			return r.handleOperationTimeout(ctx, log, &p, constants.OperationEdit)
		}

		log.Info("creating sink deployment", "namespace", namespace)
		err = r.createSink(ctx, ns, labels, secret, p)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("create sink deployment: %w", err)
		}
		// Requeue to wait for deployment to be ready
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}

	// Step 2: Create Join deployment (if enabled)
	if p.Spec.Join.Enabled {
		ready, err := r.isDeploymentReady(ctx, namespace, r.getResourceName(p, "join"))
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !ready {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(ctx, log, &p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, &p, constants.OperationEdit)
			}

			log.Info("creating join deployment", "namespace", namespace)
			err = r.createJoin(ctx, ns, labels, secret, p)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("create join deployment: %w", err)
			}
			// Requeue to wait for deployment to be ready
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		}
	}

	// Step 3: Create Ingestor deployments
	for i := range p.Spec.Ingestor.Streams {
		deploymentName := r.getResourceName(p, fmt.Sprintf("ingestor-%d", i))
		ready, err := r.isDeploymentReady(ctx, namespace, deploymentName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor deployment %s: %w", deploymentName, err)
		}
		if !ready {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(ctx, log, &p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, &p, constants.OperationEdit)
			}

			log.Info("creating ingestor deployment", "deployment", deploymentName, "namespace", namespace)
			err = r.createIngestors(ctx, log, ns, labels, secret, p)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("create ingestor deployment %s: %w", deploymentName, err)
			}
			// Requeue to wait for deployment to be ready
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		}
	}

	// All deployments are ready, update status to Running
	err = r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusRunning)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to running: %w", err)
	}

	// Remove edit annotation and clear operation start time
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineEditAnnotation)
		r.clearOperationStartTime(&p)
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(nats.PipelineStatusRunning)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove edit annotation", "pipeline_id", p.Spec.ID)
		}
	}

	// Record success metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, "edit", "success", pipelineID)
	})

	log.Info("pipeline edit completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

// -------------------------------------------------------------------------------------------------------------------

// recordReconcileError records error metrics for reconcile operations
func (r *PipelineReconciler) recordReconcileError(ctx context.Context, operation, pipelineID string, err error) {
	if r.Meter != nil {
		r.Meter.RecordReconcileOperation(ctx, operation, "failure", pipelineID)
		r.Meter.RecordReconcileError(ctx, operation, err.Error(), pipelineID)
	}
}

// recordMetricsIfEnabled is a helper function to safely record metrics only if the meter is available
func (r *PipelineReconciler) recordMetricsIfEnabled(fn func(*observability.Meter)) {
	if r.Meter != nil {
		fn(r.Meter)
	}
}

// checkOperationTimeout checks if an operation has exceeded the timeout duration
// Returns true if timed out, false otherwise, and the elapsed duration
func (r *PipelineReconciler) checkOperationTimeout(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline) (bool, time.Duration) {
	annotations := p.GetAnnotations()
	if annotations == nil {
		return false, 0
	}

	startTimeStr, exists := annotations[constants.PipelineOperationStartTimeAnnotation]
	if !exists {
		return false, 0
	}

	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		log.Error(err, "failed to parse operation start time", "pipeline_id", p.Spec.ID, "start_time", startTimeStr)
		// If we can't parse the time, clear it and continue
		r.clearOperationStartTime(p)
		return false, 0
	}

	elapsed := time.Since(startTime)
	if elapsed > constants.ReconcileTimeout {
		log.Info("operation timed out", "pipeline_id", p.Spec.ID, "elapsed", elapsed, "timeout", constants.ReconcileTimeout)
		return true, elapsed
	}

	return false, elapsed
}

// setOperationStartTime sets the operation start time annotation if not already set
func (r *PipelineReconciler) setOperationStartTime(ctx context.Context, p *etlv1alpha1.Pipeline) error {
	annotations := p.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	// Only set if not already set (to preserve the original start time)
	if _, exists := annotations[constants.PipelineOperationStartTimeAnnotation]; !exists {
		annotations[constants.PipelineOperationStartTimeAnnotation] = time.Now().UTC().Format(time.RFC3339)
		p.SetAnnotations(annotations)
		return r.Update(ctx, p)
	}

	return nil
}

// clearOperationStartTime clears the operation start time annotation
func (r *PipelineReconciler) clearOperationStartTime(p *etlv1alpha1.Pipeline) {
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, constants.PipelineOperationStartTimeAnnotation)
		p.SetAnnotations(annotations)
		// Note: We don't update here, caller should handle the update
	}
}

// handleOperationTimeout handles a timed-out operation by updating status to Failed and clearing annotations
func (r *PipelineReconciler) handleOperationTimeout(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline, operation string) (ctrl.Result, error) {
	pipelineID := p.Spec.ID
	log.Error(fmt.Errorf("operation timed out after %v", constants.ReconcileTimeout), "operation timed out", "pipeline_id", pipelineID, "operation", operation)

	// Update status to Failed
	err := r.updatePipelineStatus(ctx, log, p, nats.PipelineStatusFailed)
	if err != nil {
		log.Error(err, "failed to update pipeline status to Failed", "pipeline_id", pipelineID)
		// Continue anyway to clear annotations
	}

	// Clear operation start time
	r.clearOperationStartTime(p)

	// Clear the operation annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		switch operation {
		case constants.OperationCreate:
			delete(annotations, constants.PipelineCreateAnnotation)
		case constants.OperationResume:
			delete(annotations, constants.PipelineResumeAnnotation)
		case constants.OperationStop:
			delete(annotations, constants.PipelineStopAnnotation)
		case constants.OperationEdit:
			delete(annotations, constants.PipelineEditAnnotation)
		}
		p.SetAnnotations(annotations)
		p.Status = etlv1alpha1.PipelineStatus(nats.PipelineStatusFailed)
		err = r.Update(ctx, p)
		if err != nil {
			log.Error(err, "failed to clear operation annotation after timeout", "pipeline_id", pipelineID)
		}
	}

	// Record timeout metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordReconcileOperation(ctx, operation, "timeout", pipelineID)
	})

	return ctrl.Result{}, nil // Don't requeue - operation has timed out
}

func (r *PipelineReconciler) createNamespace(ctx context.Context, p etlv1alpha1.Pipeline) (zero v1.Namespace, _ error) {
	targetNamespace := r.getTargetNamespace(p)

	// If auto=false, we don't create namespaces, just return the target namespace
	if !r.PipelinesNamespaceAuto {
		var ns v1.Namespace
		err := r.Get(ctx, types.NamespacedName{Name: targetNamespace}, &ns)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return zero, fmt.Errorf("target namespace %s does not exist and auto=false", targetNamespace)
			}
			return zero, fmt.Errorf("get namespace %s: %w", targetNamespace, err)
		}
		return ns, nil
	}

	// Auto=true: create per-pipeline namespace
	var ns v1.Namespace
	err := r.Get(ctx, types.NamespacedName{Name: targetNamespace}, &ns)
	if err == nil {
		return ns, nil
	}

	if !apierrors.IsNotFound(err) {
		return zero, fmt.Errorf("get namespace %s: %w", targetNamespace, err)
	}

	ns = v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: targetNamespace,
			Annotations: map[string]string{
				"etl.glassflow.io/managed-by": "glassflow-operator",
				"etl.glassflow.io/mode":       "auto",
			},
		},
	}

	err = r.Create(ctx, &ns, &client.CreateOptions{})
	if err != nil {
		return zero, fmt.Errorf("create namespace %s: %w", ns.GetName(), err)
	}

	return ns, nil
}

// getTargetNamespace determines the target namespace for a pipeline based on configuration
func (r *PipelineReconciler) getTargetNamespace(p etlv1alpha1.Pipeline) string {
	if r.PipelinesNamespaceAuto {
		return "pipeline-" + p.Spec.ID
	}
	return r.PipelinesNamespaceName
}

// isOperatorManagedNamespace checks if a namespace was created by the operator
func (r *PipelineReconciler) isOperatorManagedNamespace(ns v1.Namespace) bool {
	// Check if namespace has the operator management annotation
	if managedBy, exists := ns.Annotations["etl.glassflow.io/managed-by"]; exists {
		return managedBy == "glassflow-operator"
	}
	// Fallback: check if namespace follows the pipeline-<id> pattern
	return strings.HasPrefix(ns.Name, "pipeline-")
}

// getResourceName generates a unique resource name for the given pipeline
func (r *PipelineReconciler) getResourceName(p etlv1alpha1.Pipeline, baseName string) string {
	if r.PipelinesNamespaceAuto {
		return baseName
	}
	return fmt.Sprintf("gf-pipeline-%s-%s", p.Spec.ID, baseName)
}

func (r *PipelineReconciler) createDeployment(ctx context.Context, deployment *appsv1.Deployment) error {
	err := r.Create(ctx, deployment, &client.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}

		return fmt.Errorf("create deployment: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) createSecret(ctx context.Context, namespacedName types.NamespacedName, labels map[string]string, p etlv1alpha1.Pipeline) (zero v1.Secret, _ error) {
	s := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
			Labels:    labels,
		},
		Immutable: ptrBool(true),
		StringData: map[string]string{
			"pipeline.json": p.Spec.Config,
		},
		Type: v1.SecretTypeOpaque,
	}
	err := r.Create(ctx, &s, &client.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return s, nil
		}
		return zero, fmt.Errorf("create secret %s: %w", namespacedName, err)
	}

	return s, nil
}

func (r *PipelineReconciler) updateSecret(ctx context.Context, namespacedName types.NamespacedName, labels map[string]string, p etlv1alpha1.Pipeline) (zero v1.Secret, _ error) {
	// First, try to get the existing secret
	var existingSecret v1.Secret
	err := r.Get(ctx, namespacedName, &existingSecret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Secret doesn't exist, create it
			return r.createSecret(ctx, namespacedName, labels, p)
		}
		return zero, fmt.Errorf("get existing secret %s: %w", namespacedName, err)
	}

	// Delete the existing secret since it's immutable and cannot be updated
	// We need to recreate it with the new configuration
	err = r.Delete(ctx, &existingSecret)
	if err != nil {
		return zero, fmt.Errorf("delete existing secret %s: %w", namespacedName, err)
	}

	// Create a new secret with the updated config
	return r.createSecret(ctx, namespacedName, labels, p)
}

func (r *PipelineReconciler) deleteNamespace(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	targetNamespace := r.getTargetNamespace(p)
	log.Info("deleting pipeline namespace", "pipeline", p.Name, "pipeline_id", p.Spec.ID, "namespace", targetNamespace)

	// If auto=false, we don't delete namespaces, just clean up resources
	if !r.PipelinesNamespaceAuto {
		log.Info("auto=false, skipping namespace deletion", "namespace", targetNamespace)
		return nil
	}

	namespaceName := types.NamespacedName{Name: targetNamespace}

	var namespace v1.Namespace
	err := r.Get(ctx, namespaceName, &namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("namespace already deleted", "namespace", namespaceName.Name)
			return nil
		}
		return fmt.Errorf("get namespace %s: %w", namespaceName.Name, err)
	}

	// Safety check: only delete operator-managed namespaces
	if !r.isOperatorManagedNamespace(namespace) {
		log.Info("namespace not managed by operator, skipping deletion", "namespace", namespaceName.Name)
		return nil
	}

	err = r.Delete(ctx, &namespace, &client.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete namespace %s: %w", namespaceName.Name, err)
	}

	log.Info("namespace deleted successfully", "namespace", namespaceName.Name)
	return nil
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) createIngestors(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	ing := p.Spec.Ingestor

	for i, t := range ing.Streams {
		resourceRef := r.getResourceName(p, fmt.Sprintf("ingestor-%d", i))

		ingestorLabels := r.getKafkaIngestorLabels(t.TopicName)
		maps.Copy(ingestorLabels, labels)

		container := newComponentContainerBuilder().
			withName(resourceRef).
			withImage(r.IngestorImage).
			withVolumeMount(v1.VolumeMount{
				Name:      "config",
				ReadOnly:  true,
				MountPath: "/config",
			}).
			withEnv([]v1.EnvVar{
				{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
				{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
				{Name: "GLASSFLOW_INGESTOR_TOPIC", Value: t.TopicName},
				{Name: "GLASSFLOW_LOG_LEVEL", Value: r.IngestorLogLevel},
				{Name: "GLASSFLOW_NATS_MAX_STREAM_AGE", Value: r.NATSMaxStreamAge},
				{Name: "GLASSFLOW_NATS_MAX_STREAM_BYTES", Value: utils.ConvertBytesToString(r.NATSMaxStreamBytes)},

				{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.ObservabilityLogsEnabled},
				{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.ObservabilityMetricsEnabled},
				{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.ObservabilityOTelEndpoint},
				{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: "ingestor"},
				{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.IngestorImageTag},
				{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
				{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
				{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				}},
			}).
			withResources(r.IngestorCPURequest, r.IngestorCPULimit, r.IngestorMemoryRequest, r.IngestorMemoryLimit).
			build()

		deployment := newComponentDeploymentBuilder().
			withNamespace(ns).
			withResourceName(resourceRef).
			withLabels(ingestorLabels).
			withVolume(v1.Volume{
				Name: "config",
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName:  secret.Name,
						DefaultMode: ptrInt32(0o600),
					},
				},
			}).
			withReplicas(t.Replicas).
			withContainer(*container).
			withAffinity(r.IngestorAffinity).
			build()

		err := r.createDeployment(ctx, deployment)
		if err != nil {
			return fmt.Errorf("create ingestor deployment: %w", err)
		}
	}

	return nil
}

func (r *PipelineReconciler) createJoin(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	resourceRef := r.getResourceName(p, "join")

	joinLabels := r.getJoinLabels()

	maps.Copy(joinLabels, labels)

	container := newComponentContainerBuilder().
		withName(resourceRef).
		withImage(r.JoinImage).
		withVolumeMount(v1.VolumeMount{
			Name:      "config",
			ReadOnly:  true,
			MountPath: "/config",
		}).
		withEnv([]v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
			{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
			{Name: "GLASSFLOW_LOG_LEVEL", Value: r.JoinLogLevel},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_AGE", Value: r.NATSMaxStreamAge},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_BYTES", Value: utils.ConvertBytesToString(r.NATSMaxStreamBytes)},

			{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.ObservabilityLogsEnabled},
			{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.ObservabilityMetricsEnabled},
			{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.ObservabilityOTelEndpoint},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: "join"},
			{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.JoinImageTag},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
			{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
			{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			}},
		}).
		withResources(r.JoinCPURequest, r.JoinCPULimit, r.JoinMemoryRequest, r.JoinMemoryLimit).
		build()

	deployment := newComponentDeploymentBuilder().
		withNamespace(ns).
		withResourceName(resourceRef).
		withLabels(joinLabels).
		withVolume(v1.Volume{
			Name: "config",
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName:  secret.Name,
					DefaultMode: ptrInt32(0o600),
				},
			},
		}).
		withContainer(*container).
		withAffinity(r.JoinAffinity).
		build()

	err := r.createDeployment(ctx, deployment)
	if err != nil {
		return fmt.Errorf("create join deployment: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) createSink(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	resourceRef := r.getResourceName(p, "sink")

	sinkLabels := r.getSinkLabels()
	maps.Copy(sinkLabels, labels)

	container := newComponentContainerBuilder().
		withName(resourceRef).
		withImage(r.SinkImage).
		withVolumeMount(v1.VolumeMount{
			Name:      "config",
			ReadOnly:  true,
			MountPath: "/config",
		}).
		withEnv([]v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
			{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
			{Name: "GLASSFLOW_LOG_LEVEL", Value: r.SinkLogLevel},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_AGE", Value: r.NATSMaxStreamAge},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_BYTES", Value: utils.ConvertBytesToString(r.NATSMaxStreamBytes)},

			{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.ObservabilityLogsEnabled},
			{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.ObservabilityMetricsEnabled},
			{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.ObservabilityOTelEndpoint},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: "sink"},
			{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.SinkImageTag},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
			{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
			{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			}},
		}).
		withResources(r.SinkCPURequest, r.SinkCPULimit, r.SinkMemoryRequest, r.SinkMemoryLimit).
		build()

	deployment := newComponentDeploymentBuilder().
		withNamespace(ns).
		withResourceName(resourceRef).
		withLabels(sinkLabels).
		withVolume(v1.Volume{
			Name: "config",
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName:  secret.Name,
					DefaultMode: ptrInt32(0o600),
				},
			},
		}).
		withContainer(*container).
		withAffinity(r.SinkAffinity).
		build()

	err := r.createDeployment(ctx, deployment)
	if err != nil {
		return fmt.Errorf("create sink deployment: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) createNATSStreams(ctx context.Context, p etlv1alpha1.Pipeline) error {
	// create DLQ
	err := r.NATSClient.CreateOrUpdateStream(ctx, p.Spec.DLQ, 0)
	if err != nil {
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "create_stream", "failure", p.Spec.ID)
		})
		return fmt.Errorf("create stream %s: %w", p.Spec.DLQ, err)
	}
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordNATSOperation(ctx, "create_dlq_stream", "success", p.Spec.ID)
	})

	// create source streams
	for _, s := range p.Spec.Ingestor.Streams {
		err := r.NATSClient.CreateOrUpdateStream(ctx, s.OutputStream, s.DedupWindow)
		if err != nil {
			return fmt.Errorf("create stream %s: %w", s.OutputStream, err)
		}
	}

	// create join stream
	if p.Spec.Join.Enabled {
		err := r.NATSClient.CreateOrUpdateStream(ctx, p.Spec.Join.OutputStream, 0)
		if err != nil {
			return fmt.Errorf("create stream %s: %w", p.Spec.Join.OutputStream, err)
		}

		// create join KV stores for each source stream
		// The join KV store names are the same as the stream names
		for _, stream := range p.Spec.Ingestor.Streams {
			// Use LeftBufferTTL for the first stream, RightBufferTTL for the second
			var ttl time.Duration
			if stream.OutputStream == p.Spec.Ingestor.Streams[0].OutputStream {
				ttl = p.Spec.Join.LeftBufferTTL
			} else {
				ttl = p.Spec.Join.RightBufferTTL
			}

			err := r.NATSClient.CreateOrUpdateJoinKeyValueStore(ctx, stream.OutputStream, ttl)
			if err != nil {
				return fmt.Errorf("create join KV store %s: %w", stream.OutputStream, err)
			}
		}
	}

	return nil
}

func (r *PipelineReconciler) getKafkaIngestorLabels(topic string) map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": "ingestor",
		"etl.glassflow.io/topic":     topic,
	}

	return labels
}

func (r *PipelineReconciler) getJoinLabels() map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": "join",
	}

	return labels
}

func (r *PipelineReconciler) getSinkLabels() map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": "sink",
	}

	return labels
}

func preparePipelineLabels(p etlv1alpha1.Pipeline) map[string]string {
	return map[string]string{"etl.glassflow.io/glassflow-etl-k8s-operator-id": p.Spec.ID}
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) cleanupNATSPipelineResources(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("cleaning up NATS streams", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping stream cleanup")
		return fmt.Errorf("NATS client not available, skipping stream cleanup")
	}

	// delete the DLQ stream
	if p.Spec.DLQ != "" {
		log.Info("deleting NATS DLQ stream", "stream", p.Spec.DLQ)
		err := r.deleteNATSStream(ctx, log, p.Spec.DLQ)
		if err != nil {
			log.Error(err, "failed to cleanup NATS DLQ stream", "pipeline", p.Spec.DLQ)
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_dlq_stream", "failure", p.Spec.ID)
			})
			return fmt.Errorf("failed to cleanup NATS DLQ stream: %w", err)
		}

		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "delete_dlq_stream", "success", p.Spec.ID)
		})
		log.Info("NATS DLQ stream deleted successfully", "stream", p.Spec.DLQ)
	}

	// delete Ingestor Streams
	for _, stream := range p.Spec.Ingestor.Streams {
		if stream.OutputStream != "" {
			log.Info("deleting NATS ingestor output stream", "stream", stream.OutputStream)
			err := r.deleteNATSStream(ctx, log, stream.OutputStream)
			if err != nil {
				r.recordMetricsIfEnabled(func(m *observability.Meter) {
					m.RecordNATSOperation(ctx, "delete_ingestor_stream", "failure", p.Spec.ID)
				})
				log.Error(err, "failed to cleanup NATS Ingestor Output Stream", "stream", stream)
				return fmt.Errorf("cleanup NATS Ingestor Output Stream: %w", err)
			}
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_ingestor_stream", "success", p.Spec.ID)
			})
			log.Info("NATS ingestor output stream deleted successfully", "stream", stream.OutputStream)
		}
	}

	// delete Join Streams and key value stores
	if p.Spec.Join.Enabled {
		if p.Spec.Join.OutputStream != "" {
			log.Info("deleting NATS join output stream", "stream", p.Spec.Join.OutputStream)
			err := r.deleteNATSStream(ctx, log, p.Spec.Join.OutputStream)
			if err != nil {
				r.recordMetricsIfEnabled(func(m *observability.Meter) {
					m.RecordNATSOperation(ctx, "delete_join_stream", "failure", p.Spec.ID)
				})
				log.Error(err, "failed to cleanup join output stream", "stream", p.Spec.Join.OutputStream)
				return fmt.Errorf("cleanup join output stream: %w", err)
			}
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_join_stream", "success", p.Spec.ID)
			})
			log.Info("NATS join output stream deleted successfully", "stream", p.Spec.Join.OutputStream)
		}

		err := r.cleanupNATSPipelineJoinKeyValueStore(ctx, log, p)
		if err != nil {
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_join_kv", "failure", p.Spec.ID)
			})
			log.Error(err, "failed to cleanup NATS join KV store", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
			return fmt.Errorf("failed cleanup NATS join KV store: %w", err)
		}
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "delete_join_kv", "success", p.Spec.ID)
		})
	}

	return nil
}

func (r *PipelineReconciler) cleanupNATSPipelineJoinKeyValueStore(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("cleaning up NATS join key value store", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping key value store cleanup")
		return fmt.Errorf("NATS client not available, skipping NATS cleanup")
	}

	// since join key value names are same as stream names in CH-ETL
	for _, stream := range p.Spec.Ingestor.Streams {
		err := r.NATSClient.JetStream().DeleteKeyValue(ctx, stream.OutputStream)
		if err != nil {
			log.Error(err, "failed to delete join key-value store", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
			return fmt.Errorf("failed to delete NATS KV Store: %w", err)
		}
	}

	log.Info("NATS join key value store deleted successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return nil
}

func (r *PipelineReconciler) deleteNATSStream(ctx context.Context, log logr.Logger, streamName string) error {

	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping key value store cleanup")
		return fmt.Errorf("NATS client not available, skipping NATS cleanup")
	}

	err := r.NATSClient.JetStream().DeleteStream(ctx, streamName)
	if err != nil {
		log.Error(err, "failed to delete NATS output stream", "stream", streamName)
		return fmt.Errorf("failed to delete NATS output stream: %w", err)
	}

	return nil
}

// isDeploymentAbsent checks if a deployment is fully deleted
func (r *PipelineReconciler) isDeploymentAbsent(ctx context.Context, namespace, name string) (bool, error) {
	var deployment appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &deployment)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("get deployment %s: %w", name, err)
	}
	return false, nil
}

// isDeploymentReady checks if a deployment is ready
func (r *PipelineReconciler) isDeploymentReady(ctx context.Context, namespace, name string) (bool, error) {
	var deployment appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &deployment)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get deployment %s: %w", name, err)
	}

	// Check if deployment is ready
	if deployment.Status.ReadyReplicas == deployment.Status.Replicas && deployment.Status.Replicas > 0 {
		return true, nil
	}
	return false, nil
}

// deleteDeployment safely deletes a deployment, handling NotFound errors gracefully
func (r *PipelineReconciler) deleteDeployment(ctx context.Context, deployment *appsv1.Deployment) error {
	err := r.Delete(ctx, deployment, &client.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // Already deleted, that's fine
		}
		return fmt.Errorf("delete deployment: %w", err)
	}
	return nil
}

// updatePipelineStatus updates both NATS KV and CRD status (validation handled by backend API)
func (r *PipelineReconciler) updatePipelineStatus(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline, newStatus nats.PipelineStatus) error {
	// Status validation is now handled by the backend API, so we trust the status update

	// Update NATS KV store status
	if r.NATSClient != nil {
		err := r.NATSClient.UpdatePipelineStatus(ctx, p.Spec.ID, newStatus)
		if err != nil {
			log.Info("failed to update pipeline status in NATS KV store", "pipeline_id", p.Spec.ID, "status", newStatus)
			// Don't fail the reconciliation if NATS update fails, just log the error
		} else {
			log.Info("successfully updated pipeline status in NATS KV store", "pipeline_id", p.Spec.ID, "status", newStatus)
		}
	}

	// Update CRD status
	oldStatus := string(p.Status)
	p.Status = etlv1alpha1.PipelineStatus(newStatus)
	err := r.Status().Update(ctx, p, &client.SubResourceUpdateOptions{})
	if err != nil {
		return fmt.Errorf("update pipeline CRD status: %w", err)
	}

	// Record status transition metrics
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordStatusTransition(ctx, oldStatus, string(newStatus), p.Spec.ID)
	})

	log.Info("pipeline status updated successfully", "pipeline_id", p.Spec.ID, "to", newStatus)
	return nil
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) addFinalizer(ctx context.Context, p *etlv1alpha1.Pipeline) error {
	if !containsFinalizer(p.Finalizers, constants.PipelineFinalizerName) {
		p.Finalizers = append(p.Finalizers, constants.PipelineFinalizerName)
		err := r.Update(ctx, p)
		if err != nil {
			return fmt.Errorf("add finalizer: %w", err)
		}
	}
	return nil
}

func (r *PipelineReconciler) removeFinalizer(ctx context.Context, p *etlv1alpha1.Pipeline) error {
	if containsFinalizer(p.Finalizers, constants.PipelineFinalizerName) {
		// Remove the finalizer from the slice
		finalizers := make([]string, 0, len(p.Finalizers)-1)
		for _, f := range p.Finalizers {
			if f != constants.PipelineFinalizerName {
				finalizers = append(finalizers, f)
			}
		}
		p.Finalizers = finalizers

		err := r.Update(ctx, p)
		if err != nil {
			return fmt.Errorf("remove finalizer: %w", err)
		}
	}
	return nil
}

func containsFinalizer(finalizers []string, finalizer string) bool {
	for _, f := range finalizers {
		if f == finalizer {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------------------------------------

func ptrInt32(i int32) *int32 {
	return &i
}

func ptrInt64(i int64) *int64 {
	return &i
}

func ptrBool(v bool) *bool {
	return &v
}
