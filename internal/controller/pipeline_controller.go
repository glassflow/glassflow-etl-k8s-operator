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

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
)

// -------------------------------------------------------------------------------------------------------------------

const (
	// PipelineFinalizerName is the name of the finalizer added to Pipeline resources
	PipelineFinalizerName          = "pipeline.etl.glassflow.io/finalizer"
	PipelineDeletionTypeAnnotation = "pipeline.etl.glassflow.io/deletion-type"
)

const (
	PipelineDeletionTypeShutdown  = "shutdown"
	PipelineDeletionTypeTerminate = "terminate"
)

// -------------------------------------------------------------------------------------------------------------------

// PipelineReconciler reconciles a Pipeline object
type PipelineReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	NATSClient        *nats.NATSClient
	ComponentNATSAddr string
}

// -------------------------------------------------------------------------------------------------------------------

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etlv1alpha1.Pipeline{}).
		Named("pipeline").
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

	// pipeline deletion flow
	if !p.DeletionTimestamp.IsZero() {
		return r.deletePipeline(ctx, err, p, log)
	}

	// pipeline creation flow
	if !containsFinalizer(p.Finalizers, PipelineFinalizerName) {
		return r.createPipeline(ctx, p, log)
	}

	return ctrl.Result{}, nil
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) createPipeline(ctx context.Context, p etlv1alpha1.Pipeline, log logr.Logger) (ctrl.Result, error) {
	err := r.addFinalizer(ctx, &p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
	}

	err = r.reconcileCreate(ctx, log, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("create pipeline: %w", err)
	}

	// Return to trigger another reconciling
	return ctrl.Result{Requeue: true}, nil
}

func (r *PipelineReconciler) deletePipeline(ctx context.Context, err error, p etlv1alpha1.Pipeline, log logr.Logger) (ctrl.Result, error) {
	err = r.removeFinalizer(ctx, &p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}

	if deletionType, exists := p.GetAnnotations()[PipelineDeletionTypeAnnotation]; exists {
		if deletionType == PipelineDeletionTypeTerminate {
			err = r.reconcileTerminate(ctx, log, p)
			if err != nil {
				log.Error(err, "unable to terminate Pipeline")
				return ctrl.Result{}, err
			}
		} else if deletionType == PipelineDeletionTypeShutdown {
			err = r.reconcileShutdown(ctx, log, p)
			if err != nil {
				log.Error(err, "unable to shutdown Pipeline")
				return ctrl.Result{}, err
			}
		}
	}

	return ctrl.Result{}, nil
}

// -------------------------------------------------------------------------------------------------------------------

func (r *PipelineReconciler) reconcileCreate(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	ns, err := r.createNamespace(ctx, p)
	if err != nil {
		return fmt.Errorf("setup namespace: %w", err)
	}

	err = r.createNATSStreams(ctx, p)
	if err != nil {
		return fmt.Errorf("setup streams: %w", err)
	}

	labels := preparePipelineLabels(p)

	secret, err := r.createSecret(ctx, types.NamespacedName{Namespace: ns.GetName(), Name: p.Spec.ID}, labels, p)
	if err != nil {
		return fmt.Errorf("create secret for pipeline config %s: %w", p.Spec.ID, err)
	}

	err = r.createIngestors(ctx, log, ns, labels, secret, p)
	if err != nil {
		p.Status.IngestorOperatorStatus = etlv1alpha1.ComponentStatusStopped
		return fmt.Errorf("setup ingestors: %w", err)
	}
	p.Status.IngestorOperatorStatus = etlv1alpha1.ComponentStatusStarted

	if p.Spec.Join.Enabled {
		err := r.createJoin(ctx, ns, labels, secret)
		if err != nil {
			p.Status.JoinOperatorStatus = etlv1alpha1.ComponentStatusStopped
			return fmt.Errorf("setup join: %w", err)
		}
		p.Status.JoinOperatorStatus = etlv1alpha1.ComponentStatusStarted
	} else {
		p.Status.JoinOperatorStatus = etlv1alpha1.ComponentStatusNone
	}

	err = r.createSink(ctx, ns, labels, secret)
	if err != nil {
		p.Status.SinkOperatorStatus = etlv1alpha1.ComponentStatusStopped
		return fmt.Errorf("setup sink: %w", err)
	}
	p.Status.SinkOperatorStatus = etlv1alpha1.ComponentStatusStarted

	err = r.Status().Update(ctx, &p, &client.SubResourceUpdateOptions{})
	if err != nil {
		return fmt.Errorf("update pipeline status: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) reconcileShutdown(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("reconciling pipeline deletion", "pipeline_id", p.Spec.ID)
	// Delete ingestor deployments
	// Wait for NATS stream for join to be empty (i.e. 0 messages) if enabled
	// Delete join deployment
	// Wait for NATS stream for sink to be empty (i.e. 0 messages) if enabled
	// Delete sink deployment
	// Delete namespace
	// Delete natsStreams for this pipeline
	return nil
}

func (r *PipelineReconciler) reconcileTerminate(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("reconciling pipeline termination", "pipeline_id", p.Spec.ID)

	err := r.cleanupNATSPipelineData(ctx, log, p)
	if err != nil {
		return fmt.Errorf("cleanup NATS streams: %w", err)
	}

	// Delete namespace for this pipeline
	err = r.deleteNamespace(ctx, log, p)
	if err != nil {
		return fmt.Errorf("delete pipeline namespace: %w", err)
	}

	log.Info("pipeline termination completed successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return nil
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

func (r *PipelineReconciler) deleteDeployments(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("deleting pipeline deployments", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	// List all deployments in the pipeline namespace
	var deployments appsv1.DeploymentList
	labels := preparePipelineLabels(p)

	err := r.List(ctx, &deployments, client.InNamespace("pipeline-"+p.Spec.ID), client.MatchingLabels(labels))
	if err != nil {
		return fmt.Errorf("list deployments: %w", err)
	}

	// Delete each deployment
	for _, deployment := range deployments.Items {
		log.Info("deleting deployment", "deployment", deployment.Name, "namespace", deployment.Namespace)
		err := r.Delete(ctx, &deployment, &client.DeleteOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("deployment already deleted", "deployment", deployment.Name)
				continue
			}
			return fmt.Errorf("delete deployment %s: %w", deployment.Name, err)
		}
		log.Info("deployment deleted successfully", "deployment", deployment.Name)
	}

	return nil
}

func (r *PipelineReconciler) deleteSecret(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("deleting pipeline secret", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	secretName := types.NamespacedName{
		Namespace: "pipeline-" + p.Spec.ID,
		Name:      p.Spec.ID,
	}

	var secret v1.Secret
	err := r.Get(ctx, secretName, &secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("secret already deleted", "secret", secretName.String())
			return nil
		}
		return fmt.Errorf("get secret %s: %w", secretName.String(), err)
	}

	err = r.Delete(ctx, &secret, &client.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete secret %s: %w", secretName.String(), err)
	}

	log.Info("secret deleted successfully", "secret", secretName.String())
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
			withImage("ghcr.io/glassflow/glassflow-etl-ingestor:glassflow-cloud").
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
		withImage("ghcr.io/glassflow/glassflow-etl-join:glassflow-cloud").
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
		withImage("ghcr.io/glassflow/glassflow-etl-sink:glassflow-cloud").
		withVolumeMount(v1.VolumeMount{
			Name:      "config",
			ReadOnly:  true,
			MountPath: "/config",
		}).
		withEnv([]v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
			{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
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
	}

	return nil
}

func (r *PipelineReconciler) getKafkaIngestorLabels(topic string) map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": "ingestor",
		"elt.glassflow.io/topic":     topic,
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

func (r *PipelineReconciler) cleanupNATSPipelineData(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("cleaning up NATS data", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	// Delete all streams associated with the pipeline
	err := r.cleanupNATSStreams(ctx, log, p)
	if err != nil {
		return fmt.Errorf("cleanup NATS streams: %w", err)
	}

	// Delete the key value store associated with the pipeline
	err = r.cleanupNATSPipelineKeyValueStore(ctx, log, p)
	if err != nil {
		return fmt.Errorf("cleanup NATS key value store: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) cleanupNATSStreams(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("cleaning up NATS streams", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping stream cleanup")
		return nil
	}

	// Get all streams for this pipeline
	js := r.NATSClient.JetStream()
	streams := js.ListStreams(ctx)

	// Find and delete streams associated with this pipeline
	for stream := range streams.Info() {
		streamName := stream.Config.Name

		// Check if this stream is associated with our pipeline
		// Streams are typically named with the pipeline ID or topic names
		if isPipelineStream(streamName, p) {
			log.Info("deleting NATS stream", "stream", streamName)
			err := js.DeleteStream(ctx, streamName)
			if err != nil {
				log.Error(err, "failed to delete NATS stream", "stream", streamName)
				// Continue with other streams even if one fails
				continue
			}
			log.Info("NATS stream deleted successfully", "stream", streamName)
		}
	}

	return nil
}

func (r *PipelineReconciler) cleanupNATSPipelineKeyValueStore(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("cleaning up NATS key value store", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping key value store cleanup")
		return nil
	}

	kv, err := r.NATSClient.JetStream().KeyValue(ctx, "glassflow-pipelines")
	if err != nil {
		log.Error(err, "failed to list glassflow pipelines key-value store", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
		return nil
	}

	err = kv.Delete(ctx, p.Spec.ID)
	if err != nil {
		return fmt.Errorf("check if key exists: %w", err)
	}

	log.Info("NATS key value store deleted successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
	return nil
}

func isPipelineStream(streamName string, p etlv1alpha1.Pipeline) bool {
	// Check if stream name contains pipeline ID
	if streamName == p.Spec.ID {
		return true
	}

	// Check if stream name matches any of the pipeline's topic streams
	for _, stream := range p.Spec.Ingestor.Streams {
		if streamName == stream.OutputStream {
			return true
		}
	}

	// Check if stream name matches join stream
	if p.Spec.Join.Enabled && streamName == p.Spec.Join.OutputStream {
		return true
	}

	// Check if stream name matches DLQ stream
	if streamName == p.Spec.DLQ {
		return true
	}

	return false
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
