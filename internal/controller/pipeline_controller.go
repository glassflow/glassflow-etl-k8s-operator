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
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/status"
)

// -------------------------------------------------------------------------------------------------------------------

const (
	// PipelineFinalizerName is the name of the finalizer added to Pipeline resources
	PipelineFinalizerName       = "pipeline.etl.glassflow.io/finalizer"
	PipelineCreateAnnotation    = "pipeline.etl.glassflow.io/create"
	PipelinePauseAnnotation     = "pipeline.etl.glassflow.io/pause"
	PipelineResumeAnnotation    = "pipeline.etl.glassflow.io/resume"
	PipelineStopAnnotation      = "pipeline.etl.glassflow.io/stop"
	PipelineTerminateAnnotation = "pipeline.etl.glassflow.io/terminate"
)

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
		if !reflect.DeepEqual(oldAnnotations, newAnnotations) {
			return true
		}

		// Don't trigger on status-only changes
		return false
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
	NATSClient        *nats.NATSClient
	ComponentNATSAddr string
	// Component image configurations
	IngestorImage string
	JoinImage     string
	SinkImage     string
}

// -------------------------------------------------------------------------------------------------------------------

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etlv1alpha1.Pipeline{}).
		Named("pipeline").
		WithEventFilter(pipelineOperationPredicate).
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
		var operation string
		if _, hasCreate := annotations[PipelineCreateAnnotation]; hasCreate {
			operation = "create"
		} else if _, hasPause := annotations[PipelinePauseAnnotation]; hasPause {
			operation = "pause"
		} else if _, hasResume := annotations[PipelineResumeAnnotation]; hasResume {
			operation = "resume"
		} else if _, hasStop := annotations[PipelineStopAnnotation]; hasStop {
			operation = "stop"
		} else if _, hasTerminate := annotations[PipelineTerminateAnnotation]; hasTerminate {
			operation = "terminate"
		}

		// Execute the appropriate operation
		switch operation {
		case "create":
			return r.createPipeline(ctx, p)
		case "pause":
			return r.reconcilePause(ctx, log, p)
		case "resume":
			return r.reconcileResume(ctx, log, p)
		case "stop":
			return r.reconcileStop(ctx, log, p)
		case "terminate":
			return r.reconcileTerminate(ctx, log, p)
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
	log.Info("reconciling pipeline creation", "pipeline_id", p.Spec.ID)

	// Check if pipeline is already running
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusRunning) {
		log.Info("pipeline already running", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
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
			return ctrl.Result{}, fmt.Errorf("update pipeline CRD status: %w", err)
		}
	}

	ns, err := r.createNamespace(ctx, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("setup namespace: %w", err)
	}

	err = r.createNATSStreams(ctx, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("setup streams: %w", err)
	}

	labels := preparePipelineLabels(p)

	secret, err := r.createSecret(ctx, types.NamespacedName{Namespace: ns.GetName(), Name: p.Spec.ID}, labels, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("create secret for pipeline config %s: %w", p.Spec.ID, err)
	}

	namespace := "pipeline-" + p.Spec.ID

	// Step 1: Create Sink deployment
	ready, err := r.isDeploymentReady(ctx, namespace, "sink")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !ready {
		log.Info("creating sink deployment", "namespace", namespace)
		err = r.createSink(ctx, ns, labels, secret)
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
		ready, err := r.isDeploymentReady(ctx, namespace, "join")
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !ready {
			log.Info("creating join deployment", "namespace", namespace)
			err := r.createJoin(ctx, ns, labels, secret)
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
		deploymentName := fmt.Sprintf("ingestor-%d", i)
		ready, err := r.isDeploymentReady(ctx, namespace, deploymentName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor deployment %s: %w", deploymentName, err)
		}
		if !ready {
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

	// Remove create annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, PipelineCreateAnnotation)
		p.SetAnnotations(annotations)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove create annotation", "pipeline_id", p.Spec.ID)
		}
	}

	log.Info("pipeline creation completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileTerminate(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	log.Info("reconciling pipeline termination", "pipeline_id", p.Spec.ID)

	// Check if pipeline is already terminated
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusTerminated) {
		log.Info("pipeline already terminated", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}

	// Set status to Terminating first
	err := r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusTerminating)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to terminating: %w", err)
	}

	// Delete namespace for this pipeline
	err = r.deleteNamespace(ctx, log, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("delete pipeline namespace: %w", err)
	}

	// Clean up NATS streams but keep the pipeline configuration
	err = r.cleanupNATSPipelineStreams(ctx, log, p)
	if err != nil {
		log.Info("failed to cleanup NATS resources during termination", "pipeline_id", p.Spec.ID)
		// Don't return error here as namespace is already deleted
		// Just log and continue
	}

	// Update pipeline status to "Terminated"
	err = r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusTerminated)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to terminated: %w", err)
	}

	// Remove terminate annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, PipelineTerminateAnnotation)
		p.SetAnnotations(annotations)
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

	log.Info("pipeline termination completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcilePause(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	log.Info("reconciling pipeline pause", "pipeline_id", p.Spec.ID)

	namespace := "pipeline-" + p.Spec.ID

	// Check if pipeline is already paused or pausing
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusPaused) {
		log.Info("pipeline already paused", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
	}
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusPausing) {
		log.Info("pipeline is already being paused", "pipeline_id", p.Spec.ID)
		// Continue with the pause process
	} else {
		// Transition to Pausing status first
		err := r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusPausing)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("update pipeline status to pausing: %w", err)
		}
		// Requeue to continue with the pause process
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}

	// Step 1: Stop Ingestor deployments
	for i := range p.Spec.Ingestor.Streams {
		deploymentName := fmt.Sprintf("ingestor-%d", i)
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
				err = r.Delete(ctx, &deployment, &client.DeleteOptions{})
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

	// Step 2: Stop Join deployment (if enabled)
	if p.Spec.Join.Enabled {
		deleted, err := r.isDeploymentAbsent(ctx, namespace, "join")
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !deleted {
			log.Info("deleting join deployment", "namespace", namespace)
			var deployment appsv1.Deployment
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "join"}, &deployment)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("get join deployment: %w", err)
				}
			} else {
				err = r.Delete(ctx, &deployment, &client.DeleteOptions{})
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

	// Step 3: Stop Sink deployment
	deleted, err := r.isDeploymentAbsent(ctx, namespace, "sink")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !deleted {
		log.Info("deleting sink deployment", "namespace", namespace)
		var deployment appsv1.Deployment
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "sink"}, &deployment)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("get sink deployment: %w", err)
			}
		} else {
			err = r.Delete(ctx, &deployment, &client.DeleteOptions{})
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete sink deployment: %w", err)
			}
		}
		// Requeue to wait for deployment to be fully deleted
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	} else {
		log.Info("sink deployment is already deleted", "namespace", namespace)
	}

	// All deployments are deleted, update status to Paused
	err = r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusPaused)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to paused: %w", err)
	}

	// Remove pause annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, PipelinePauseAnnotation)
		p.SetAnnotations(annotations)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove pause annotation", "pipeline_id", p.Spec.ID)
		}
	}

	log.Info("pipeline pause completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileResume(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	log.Info("reconciling pipeline resume", "pipeline_id", p.Spec.ID)

	namespace := "pipeline-" + p.Spec.ID

	// Check if pipeline is already running or resuming
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusRunning) {
		log.Info("pipeline already running", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
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
	secretName := types.NamespacedName{Namespace: namespace, Name: p.Spec.ID}
	var secret v1.Secret
	err = r.Get(ctx, secretName, &secret)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get secret %s: %w", secretName, err)
	}

	labels := preparePipelineLabels(p)

	// Step 1: Create Sink deployment
	ready, err := r.isDeploymentReady(ctx, namespace, "sink")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !ready {
		log.Info("creating sink deployment", "namespace", namespace)
		err = r.createSink(ctx, ns, labels, secret)
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
		ready, err := r.isDeploymentReady(ctx, namespace, "join")
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !ready {
			log.Info("creating join deployment", "namespace", namespace)
			err = r.createJoin(ctx, ns, labels, secret)
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
		deploymentName := fmt.Sprintf("ingestor-%d", i)
		ready, err := r.isDeploymentReady(ctx, namespace, deploymentName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor deployment %s: %w", deploymentName, err)
		}
		if !ready {
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

	// Remove resume annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, PipelineResumeAnnotation)
		p.SetAnnotations(annotations)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove resume annotation", "pipeline_id", p.Spec.ID)
		}
	}

	log.Info("pipeline resume completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileStop(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) (ctrl.Result, error) {
	log.Info("reconciling pipeline stop", "pipeline_id", p.Spec.ID)

	namespace := "pipeline-" + p.Spec.ID

	// Check if pipeline is already stopped
	if p.Status == etlv1alpha1.PipelineStatus(nats.PipelineStatusStopped) {
		log.Info("pipeline already stopped", "pipeline_id", p.Spec.ID)
		return ctrl.Result{}, nil
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
		// Requeue to continue with the stop process
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}

	// Step 1: Stop Ingestor deployments
	for i := range p.Spec.Ingestor.Streams {
		deploymentName := fmt.Sprintf("ingestor-%d", i)
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
				err = r.Delete(ctx, &deployment, &client.DeleteOptions{})
				if err != nil {
					return ctrl.Result{}, fmt.Errorf("delete ingestor deployment %s: %w", deploymentName, err)
				}
			}
			// Requeue to wait for deployment to be fully deleted
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("ingestor deployment is already deleted", "namespace", namespace)
		}
	}

	// Step 2: Stop Join deployment (if enabled)
	if p.Spec.Join.Enabled {
		deleted, err := r.isDeploymentAbsent(ctx, namespace, "join")
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !deleted {
			log.Info("deleting join deployment", "namespace", namespace)
			var deployment appsv1.Deployment
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "join"}, &deployment)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("get join deployment: %w", err)
				}
			} else {
				err = r.Delete(ctx, &deployment, &client.DeleteOptions{})
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

	// Step 3: Stop Sink deployment
	deleted, err := r.isDeploymentAbsent(ctx, namespace, "sink")
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !deleted {
		log.Info("deleting sink deployment", "namespace", namespace)
		var deployment appsv1.Deployment
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "sink"}, &deployment)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("get sink deployment: %w", err)
			}
		} else {
			err = r.Delete(ctx, &deployment, &client.DeleteOptions{})
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete sink deployment: %w", err)
			}
		}
		// Requeue to wait for deployment to be fully deleted
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	} else {
		log.Info("sink deployment is already deleted", "namespace", namespace)
	}

	// All deployments are deleted, clean up NATS resources
	err = r.cleanupNATSPipelineStreams(ctx, log, p)
	if err != nil {
		log.Error(err, "failed to cleanup NATS resources during stop", "pipeline_id", p.Spec.ID)
		// Don't return error here as deployments are already deleted
		// Just log the error and continue
	}

	// Delete namespace for this pipeline
	err = r.deleteNamespace(ctx, log, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("delete pipeline namespace: %w", err)
	}

	// Update status to Stopped
	err = r.updatePipelineStatus(ctx, log, &p, nats.PipelineStatusStopped)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update pipeline status to stopped: %w", err)
	}

	// Remove stop annotation
	annotations := p.GetAnnotations()
	if annotations != nil {
		delete(annotations, PipelineStopAnnotation)
		p.SetAnnotations(annotations)
		err = r.Update(ctx, &p)
		if err != nil {
			log.Error(err, "failed to remove stop annotation", "pipeline_id", p.Spec.ID)
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

	log.Info("pipeline stop completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return ctrl.Result{}, nil
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) createNamespace(ctx context.Context, p etlv1alpha1.Pipeline) (zero v1.Namespace, _ error) {
	var ns v1.Namespace

	id := "pipeline-" + p.Spec.ID

	err := r.Get(ctx, types.NamespacedName{Name: id}, &ns)
	if err == nil {
		return ns, nil
	}

	if !apierrors.IsNotFound(err) {
		return zero, fmt.Errorf("get namespace %s: %w", id, err)
	}

	ns = v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: p.GetNamespace(),
		},
	}

	err = r.Create(ctx, &ns, &client.CreateOptions{})
	if err != nil {
		return zero, fmt.Errorf("create namespace %s: %w", ns.GetName(), err)
	}

	return ns, nil
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

func (r *PipelineReconciler) deleteNamespace(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("deleting pipeline namespace", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	namespaceName := types.NamespacedName{Name: "pipeline-" + p.Spec.ID}

	var namespace v1.Namespace
	err := r.Get(ctx, namespaceName, &namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("namespace already deleted", "namespace", namespaceName.Name)
			return nil
		}
		return fmt.Errorf("get namespace %s: %w", namespaceName.Name, err)
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
	// TODO: incase of multiple ingestors, ensure type and relevant images
	ing := p.Spec.Ingestor

	for i, t := range ing.Streams {
		resourceRef := fmt.Sprintf("ingestor-%d", i)

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
			}).
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
			build()

		err := r.createDeployment(ctx, deployment)
		if err != nil {
			return fmt.Errorf("create ingestor deployment: %w", err)
		}
	}

	return nil
}

func (r *PipelineReconciler) createJoin(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret) error {
	resourceRef := "join"

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
		}).
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
		build()

	fmt.Println(len(deployment.Spec.Template.Spec.Containers))

	err := r.createDeployment(ctx, deployment)
	if err != nil {
		return fmt.Errorf("create join deployment: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) createSink(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret) error {
	resourceRef := "sink"

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
		}).
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
		return fmt.Errorf("create stream %s: %w", p.Spec.DLQ, err)
	}

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

func (r *PipelineReconciler) cleanupNATSPipelineStreams(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
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
			return fmt.Errorf("failed to cleanup NATS DLQ stream: %w", err)
		}
		log.Info("NATS DLQ stream deleted successfully", "stream", p.Spec.DLQ)
	}

	// delete Ingestor Streams
	for _, stream := range p.Spec.Ingestor.Streams {
		if stream.OutputStream != "" {
			log.Info("deleting NATS ingestor output stream", "stream", stream.OutputStream)
			err := r.deleteNATSStream(ctx, log, stream.OutputStream)
			if err != nil {
				log.Error(err, "failed to cleanup NATS Ingestor Output Stream", "stream", stream)
				return fmt.Errorf("cleanup NATS Ingestor Output Stream: %w", err)
			}
			log.Info("NATS ingestor output stream deleted successfully", "stream", stream.OutputStream)
		}
	}

	// delete Join Streams and key value stores
	if p.Spec.Join.Enabled {
		if p.Spec.Join.OutputStream != "" {
			log.Info("deleting NATS join output stream", "stream", p.Spec.Join.OutputStream)
			err := r.deleteNATSStream(ctx, log, p.Spec.Join.OutputStream)
			if err != nil {
				log.Error(err, "failed to cleanup join output stream", "stream", p.Spec.Join.OutputStream)
				return fmt.Errorf("cleanup join output stream: %w", err)
			}
			log.Info("NATS join output stream deleted successfully", "stream", p.Spec.Join.OutputStream)
		}

		err := r.cleanupNATSPipelineJoinKeyValueStore(ctx, log, p)
		if err != nil {
			log.Error(err, "failed to cleanup NATS join KV store", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
			return fmt.Errorf("failed cleanup NATS join KV store: %w", err)
		}
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

// updatePipelineStatus updates both NATS KV and CRD status with validation
func (r *PipelineReconciler) updatePipelineStatus(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline, newStatus nats.PipelineStatus) error {
	// Validate status transition
	currentStatus := nats.PipelineStatus(p.Status)
	err := status.ValidatePipelineStatusTransition(currentStatus, newStatus)
	if err != nil {
		log.Error(err, "invalid status transition", "pipeline_id", p.Spec.ID, "from", currentStatus, "to", newStatus)
		return fmt.Errorf("invalid status transition from %s to %s: %w", currentStatus, newStatus, err)
	}

	// Update NATS KV store status
	if r.NATSClient != nil {
		err := r.NATSClient.UpdatePipelineStatus(ctx, p.Spec.ID, newStatus)
		if err != nil {
			log.Error(err, "failed to update pipeline status in NATS KV store", "pipeline_id", p.Spec.ID, "status", newStatus)
			// Don't fail the reconciliation if NATS update fails, just log the error
		} else {
			log.Info("successfully updated pipeline status in NATS KV store", "pipeline_id", p.Spec.ID, "status", newStatus)
		}
	}

	// Update CRD status
	p.Status = etlv1alpha1.PipelineStatus(newStatus)
	err = r.Status().Update(ctx, p, &client.SubResourceUpdateOptions{})
	if err != nil {
		return fmt.Errorf("update pipeline CRD status: %w", err)
	}

	log.Info("pipeline status updated successfully", "pipeline_id", p.Spec.ID, "from", currentStatus, "to", newStatus)
	return nil
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) addFinalizer(ctx context.Context, p *etlv1alpha1.Pipeline) error {
	if !containsFinalizer(p.Finalizers, PipelineFinalizerName) {
		p.Finalizers = append(p.Finalizers, PipelineFinalizerName)
		err := r.Update(ctx, p)
		if err != nil {
			return fmt.Errorf("add finalizer: %w", err)
		}
	}
	return nil
}

func (r *PipelineReconciler) removeFinalizer(ctx context.Context, p *etlv1alpha1.Pipeline) error {
	if containsFinalizer(p.Finalizers, PipelineFinalizerName) {
		// Remove the finalizer from the slice
		finalizers := make([]string, 0, len(p.Finalizers)-1)
		for _, f := range p.Finalizers {
			if f != PipelineFinalizerName {
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
