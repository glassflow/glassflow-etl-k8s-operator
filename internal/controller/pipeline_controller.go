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
	"strconv"
	"strings"

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
		fmt.Println("DELETION TIMESTAMP")
		fmt.Println(p.GetDeletionTimestamp())

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

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etlv1alpha1.Pipeline{}).
		Named("pipeline").
		Complete(r)
}

func ptrInt32(i int32) *int32 {
	return &i
}

//nolint:unparam // will reuse the deployment in future
func (r *PipelineReconciler) getOrCreateDeployment(ctx context.Context, deployment *appsv1.Deployment) (*appsv1.Deployment, error) {
	err := r.Create(ctx, deployment, &client.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return deployment, nil
		}

		return nil, fmt.Errorf("create deployment: %w", err)
	}

	return deployment, nil
}

func (r *PipelineReconciler) getOrCreateSecret(ctx context.Context, namespacedName types.NamespacedName, labels, secret map[string]string) (zero v1.Secret, _ error) {
	s := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
			Labels:    labels,
		},
		StringData: secret,
		Type:       v1.SecretTypeOpaque,
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

const (
	defaultStreamSubject = "input"
	DLQSuffix            = "DLQ"
	DLQSubject           = "failed"
)

type streamsConfig struct {
	Sources map[string]string
	Join    string
	Sink    string
}

func (r *PipelineReconciler) setupNATSStreams(ctx context.Context, id string, join etlv1alpha1.JoinOperatorConfig, ingestorTopics []etlv1alpha1.KafkaTopicsConfig) (zero streamsConfig, _ error) {
	var streamCfg streamsConfig
	var sourceStreamsMap = make(map[string]string, len(ingestorTopics))

	// create DLQ
	DLQStream := fmt.Sprintf("%s-%s", id, DLQSuffix)
	err := r.NATSClient.CreateOrUpdateStream(ctx, DLQStream, fmt.Sprintf("%s.%s", DLQStream, DLQSubject), 0)
	if err != nil {
		return zero, fmt.Errorf("create stream %s: %w", DLQStream, err)
	}

	// create join stream
	if join.Enabled {
		stream := fmt.Sprintf("%s-%s", id, "join")
		err := r.NATSClient.CreateOrUpdateStream(ctx, stream, fmt.Sprintf("%s.%s", stream, defaultStreamSubject), 0)
		if err != nil {
			return zero, fmt.Errorf("create stream %s: %w", stream, err)
		}

		streamCfg.Join = stream
		streamCfg.Sink = stream
	}

	// create source streams
	for _, t := range ingestorTopics {
		stream := fmt.Sprintf("%s-%s", id, t.Name)
		err := r.NATSClient.CreateOrUpdateStream(ctx, stream, fmt.Sprintf("%s.%s", stream, defaultStreamSubject), t.Deduplication.Window)
		if err != nil {
			return zero, fmt.Errorf("create stream %s: %w", stream, err)
		}

		sourceStreamsMap[t.Name] = stream
		streamCfg.Sink = stream
	}
	streamCfg.Sources = sourceStreamsMap

	return streamCfg, nil
}

func (r *PipelineReconciler) reconcileDeletion(_ context.Context, _ logr.Logger, _ etlv1alpha1.Pipeline) error {
	// TODO: reconcile deletion
	// Add finalizer to the namespace or delete it in the end?
	// Delete ingestor deployments and secrets
	// Wait for NATS stream for join to be empty (i.e. 0 messages) if enabled
	// Delete join deployment and secret
	// Wait for NATS stream for sink to be empty (i.e. 0 messages) if enabled
	// Delete sink deployment and secret
	return nil
}

func (r *PipelineReconciler) reconcileCreation(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	ns, err := r.setupNamespace(ctx, p)
	if err != nil {
		return fmt.Errorf("setup namespace: %w", err)
	}

	// TODO: use these nats streams as evn vars for join / sink
	// TODO: may be pass the whole pipeline?
	_, err = r.setupNATSStreams(ctx, p.Spec.ID, p.Spec.Join, p.Spec.Ingestor.KafkaTopics)
	if err != nil {
		return fmt.Errorf("setup streams: %w", err)
	}

	labels := r.preparePipelineLabels(p)

	err = r.setupIngestors(ctx, log, ns, labels, p)
	if err != nil {
		p.Status.IngestorOperatorStatus = etlv1alpha1.ComponentStatusStopped
		return fmt.Errorf("setup ingestors: %w", err)
	}
	p.Status.IngestorOperatorStatus = etlv1alpha1.ComponentStatusStarted

	if p.Spec.Join.Enabled {
		err := r.setupJoin(ctx, log, ns, labels, p)
		if err != nil {
			p.Status.JoinOperatorStatus = etlv1alpha1.ComponentStatusStopped
			return fmt.Errorf("setup join: %w", err)
		}
		p.Status.JoinOperatorStatus = etlv1alpha1.ComponentStatusStarted
	} else {
		p.Status.JoinOperatorStatus = etlv1alpha1.ComponentStatusNone
	}

	err = r.setupSink(ctx, log, ns, labels, p)
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

func (r *PipelineReconciler) setupIngestors(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, p etlv1alpha1.Pipeline) error {
	ing := p.Spec.Ingestor

	for i, t := range ing.KafkaTopics {
		resourceRef := fmt.Sprintf("ingestor-%d", i)

		kafkaCfg := ing.KafkaConnectionParams

		secret, err := r.getOrCreateSecret(ctx, types.NamespacedName{Namespace: ns.GetName(), Name: resourceRef}, labels, map[string]string{
			"brokers":               strings.Join(kafkaCfg.Brokers, ","),
			"skip_auth":             strconv.FormatBool(kafkaCfg.SkipAuth),
			"protocol":              kafkaCfg.SASLProtocol,
			"mechanism":             kafkaCfg.SASLMechanism,
			"username":              kafkaCfg.SASLUsername,
			"password":              kafkaCfg.SASLPassword,
			"root_ca":               kafkaCfg.TLSRoot,
			"tls_enabled":           strconv.FormatBool(kafkaCfg.SASLTLSEnable),
			"topic":                 t.Name,
			"initial_offset":        t.ConsumerGroupInitialOffset,
			"deduplication_enabled": strconv.FormatBool(t.Deduplication.Enabled),
			"deduplication_key":     t.Deduplication.ID,
			"deduplication_type":    t.Deduplication.Type,
			"deduplication_window":  t.Deduplication.Window.String(),
		})
		if err != nil {
			return fmt.Errorf("create secret for topic %s: %w", t.Name, err)
		}

		maps.Copy(labels, r.getKafkaIngestorLabels(t.Name))

		container := newComponentContainerBuilder().
			withName(resourceRef).
			withImage("ghcr.io/glassflow/glassflow-etl-ingestor:glassflow-cloud").
			withEnv([]v1.EnvVar{
				{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
			}).
			withSecret(secret).
			build()

		deployment := newComponentDeploymentBuilder().
			withNamespace(ns).
			withResourceName(resourceRef).
			withLabels(labels).
			withContainer(*container).
			build()

		_, err = r.getOrCreateDeployment(ctx, deployment)
		if err != nil {
			return fmt.Errorf("create ingestor deployment: %w", err)
		}
	}

	return nil
}

func (r *PipelineReconciler) setupJoin(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, _ etlv1alpha1.Pipeline) error {
	resourceRef := "join"

	// TODO: fill secret values for join and may be use a configmap for this one?
	secret, err := r.getOrCreateSecret(ctx, types.NamespacedName{Namespace: ns.GetName(), Name: resourceRef}, labels, map[string]string{})
	if err != nil {
		return fmt.Errorf("create secret for join: %w", err)
	}

	maps.Copy(r.getJoinLabels(), labels)

	container := newComponentContainerBuilder().
		withName(resourceRef).
		withImage("ghcr.io/glassflow/glassflow-etl-join:glassflow-cloud").
		withEnv([]v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
		}).
		withSecret(secret).
		build()

	deployment := newComponentDeploymentBuilder().
		withNamespace(ns).
		withResourceName(resourceRef).
		withLabels(labels).
		withContainer(*container).
		build()

	_, err = r.getOrCreateDeployment(ctx, deployment)
	if err != nil {
		return fmt.Errorf("create join deployment: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) setupSink(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, _ etlv1alpha1.Pipeline) error {
	resourceRef := "sink"

	// TODO: fill secret values for join
	secret, err := r.getOrCreateSecret(ctx, types.NamespacedName{Namespace: ns.GetName(), Name: resourceRef}, labels, map[string]string{})
	if err != nil {
		return fmt.Errorf("create secret for sink: %w", err)
	}

	maps.Copy(r.getSinkLabels(), labels)

	container := newComponentContainerBuilder().
		withName(resourceRef).
		withImage("ghcr.io/glassflow/glassflow-etl-sink:glassflow-cloud").
		withEnv([]v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
		}).
		withSecret(secret).
		build()

	deployment := newComponentDeploymentBuilder().
		withNamespace(ns).
		withResourceName(resourceRef).
		withLabels(labels).
		withContainer(*container).
		build()

	_, err = r.getOrCreateDeployment(ctx, deployment)
	if err != nil {
		return fmt.Errorf("create sink deployment: %w", err)
	}

	return nil
}
