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

	etlv1alpha1 "glassflow.io/pipeline/api/v1alpha1"
	"glassflow.io/pipeline/internal/nats"
)

// PipelineReconciler reconciles a Pipeline object
type PipelineReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	NATSClient        *nats.NATSClient
	ComponentNATSAddr string
}

// +kubebuilder:rbac:groups=etl.glassflow.io,resources=pipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=etl.glassflow.io,resources=pipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=etl.glassflow.io,resources=pipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;list;watch;create;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Pipeline object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
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

	if !p.DeletionTimestamp.IsZero() {
		// TODO: sync delete logic
		err := r.reconcileDeletion(ctx, log, p)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile deletion: %w", err)
		}

		return ctrl.Result{}, nil
	}

	err = r.reconcileCreation(ctx, log, p)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("create pipeline: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileCreation(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	ns, err := r.setupNamespace(ctx, p)
	if err != nil {
		return fmt.Errorf("setup namespace: %w", err)
	}

	err = r.setupNATSStreams(ctx, p)
	if err != nil {
		return fmt.Errorf("setup streams: %w", err)
	}

	labels := r.preparePipelineLabels(p)

	secret, err := r.getOrCreateSecret(ctx, types.NamespacedName{Namespace: ns.GetName(), Name: p.Spec.ID}, labels, p)
	if err != nil {
		return fmt.Errorf("create secret for pipeline config %s: %w", p.Spec.ID, err)
	}

	err = r.setupIngestors(ctx, log, ns, labels, secret, p)
	if err != nil {
		p.Status.IngestorOperatorStatus = etlv1alpha1.ComponentStatusStopped
		return fmt.Errorf("setup ingestors: %w", err)
	}
	p.Status.IngestorOperatorStatus = etlv1alpha1.ComponentStatusStarted

	if p.Spec.Join.Enabled {
		err := r.setupJoin(ctx, ns, labels, secret)
		if err != nil {
			p.Status.JoinOperatorStatus = etlv1alpha1.ComponentStatusStopped
			return fmt.Errorf("setup join: %w", err)
		}
		p.Status.JoinOperatorStatus = etlv1alpha1.ComponentStatusStarted
	} else {
		p.Status.JoinOperatorStatus = etlv1alpha1.ComponentStatusNone
	}

	err = r.setupSink(ctx, ns, labels, secret)
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

func (r *PipelineReconciler) setupNamespace(ctx context.Context, p etlv1alpha1.Pipeline) (zero v1.Namespace, _ error) {
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

func (r *PipelineReconciler) setupNATSStreams(ctx context.Context, p etlv1alpha1.Pipeline) error {
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

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etlv1alpha1.Pipeline{}).
		Named("pipeline").
		Complete(r)
}

//nolint:unparam // will reuse the deployment in future
func (r *PipelineReconciler) createDeploymentIfNotExists(ctx context.Context, deployment *appsv1.Deployment) error {
	err := r.Create(ctx, deployment, &client.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}

		return fmt.Errorf("create deployment: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) getOrCreateSecret(ctx context.Context, namespacedName types.NamespacedName, labels map[string]string, p etlv1alpha1.Pipeline) (zero v1.Secret, _ error) {
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

func (r *PipelineReconciler) reconcileDeletion(_ context.Context, _ logr.Logger, _ etlv1alpha1.Pipeline) error {
	// TODO: reconcile deletion
	// Add finalizer to the namespace or delete it in the end?
	// Delete ingestor deployments
	// Wait for NATS stream for join to be empty (i.e. 0 messages) if enabled
	// Delete join deployment
	// Wait for NATS stream for sink to be empty (i.e. 0 messages) if enabled
	// Delete sink deployment
	// Delete namespace - this should delete secret too
	return nil
}

func (*PipelineReconciler) preparePipelineLabels(p etlv1alpha1.Pipeline) map[string]string {
	return map[string]string{"etl.glassflow.io/pipeline-id": p.Spec.ID}
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

func (r *PipelineReconciler) setupIngestors(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
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

		err := r.createDeploymentIfNotExists(ctx, deployment)
		if err != nil {
			return fmt.Errorf("create ingestor deployment: %w", err)
		}
	}

	return nil
}

func (r *PipelineReconciler) setupJoin(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret) error {
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

	err := r.createDeploymentIfNotExists(ctx, deployment)
	if err != nil {
		return fmt.Errorf("create join deployment: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) setupSink(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret) error {
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

	err := r.createDeploymentIfNotExists(ctx, deployment)
	if err != nil {
		return fmt.Errorf("create sink deployment: %w", err)
	}

	return nil
}

func ptrInt32(i int32) *int32 {
	return &i
}

func ptrInt64(i int64) *int64 {
	return &i
}

func ptrBool(v bool) *bool {
	return &v
}
