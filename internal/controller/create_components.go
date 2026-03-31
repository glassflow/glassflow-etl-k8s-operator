package controller

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/errs"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/pipelinegraph"
)

const (
	componentDeleteRequeueDelay = time.Second
	pendingMessagesRequeueDelay = 10 * time.Second
)

// stopPipelineComponents stops all pipeline components in the correct order with pending message checks.
func (r *PipelineReconciler) stopPipelineComponents(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline) (ctrl.Result, error) {
	return r.reconcilePipelineTeardown(ctx, log, p, true)
}

// terminatePipelineComponents terminates all pipeline components immediately.
func (r *PipelineReconciler) terminatePipelineComponents(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline) (ctrl.Result, error) {
	return r.reconcilePipelineTeardown(ctx, log, p, false)
}

func (r *PipelineReconciler) createPipelineComponents(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	ns v1.Namespace,
	labels map[string]string,
	secret v1.Secret,
) (ctrl.Result, error) {
	namespace := r.getTargetNamespace(*p)

	// Step 1: Ensure Sink StatefulSet is ready
	requeue, err := r.ensureStatefulSetReady(ctx, log, p, namespace, r.getStatefulSetResourceName(*p, constants.SinkComponent), r.createSink, ns, labels, secret)
	if err != nil {
		return ctrl.Result{}, err
	}
	if requeue {
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}

	// Step 2: Ensure Join StatefulSet is ready (if enabled)
	if p.Spec.Join.Enabled {
		requeue, err = r.ensureStatefulSetReady(ctx, log, p, namespace, r.getStatefulSetResourceName(*p, constants.JoinComponent), r.createJoin, ns, labels, secret)
		if err != nil {
			return ctrl.Result{}, err
		}
		if requeue {
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		}
	}

	// Step 3: Ensure Dedup StatefulSets are ready
	result, err := r.ensureDedupStatefulSetsReady(ctx, log, p, ns, labels, secret)
	if err != nil || result.Requeue {
		return result, err
	}

	// Step 4: Ensure Ingestor StatefulSets are ready (Kafka only — OTLP has no ingestor)
	if p.Spec.IsOTLPSource() {
		return result, nil
	}

	for i := range p.Spec.Source.Streams {
		statefulSetName := r.getStatefulSetResourceName(*p, fmt.Sprintf("%s-%d", constants.IngestorComponent, i))
		ready, err := r.isStatefulSetReady(ctx, namespace, statefulSetName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor statefulset %s: %w", statefulSetName, err)
		}
		if !ready {
			timedOut, _ := r.checkOperationTimeout(log, p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, p)
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

// createIngestors creates ingestor deployments for the pipeline
func (r *PipelineReconciler) createIngestors(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	ing := p.Spec.Source
	graph, err := pipelinegraph.NewFromPipelineSpec(p.Spec)
	if err != nil {
		return fmt.Errorf("build pipeline graph for ingestors: %w", err)
	}

	for i, t := range ing.Streams {
		resourceRef := r.getStatefulSetResourceName(p, fmt.Sprintf("%s-%d", constants.IngestorComponent, i))

		ingestorLabels := r.getKafkaIngestorLabels(t.TopicName)
		maps.Copy(ingestorLabels, labels)

		cpuReq, cpuLim, memReq, memLim := r.Config.ResourceDefaults.Ingestor.CPURequest, r.Config.ResourceDefaults.Ingestor.CPULimit, r.Config.ResourceDefaults.Ingestor.MemoryRequest, r.Config.ResourceDefaults.Ingestor.MemoryLimit
		ingestorReplicas := constants.DefaultMinReplicas
		if p.Spec.Resources != nil && p.Spec.Resources.Ingestor != nil {
			ingRes := p.Spec.Resources.Ingestor
			var comp *etlv1alpha1.ComponentResources
			if p.Spec.Join.Enabled {
				if i == 0 {
					comp = ingRes.Left
				} else {
					comp = ingRes.Right
				}
			} else {
				comp = ingRes.Base
			}
			if comp != nil {
				if comp.Requests != nil {
					cpuReq, memReq = comp.Requests.CPU.String(), comp.Requests.Memory.String()
				}
				if comp.Limits != nil {
					cpuLim, memLim = comp.Limits.CPU.String(), comp.Limits.Memory.String()
				}
				if comp.Replicas != nil {
					ingestorReplicas = int(*comp.Replicas)
				}
			}
		}

		subjectCountEnvVars := ingestorNATSSubjectCountEnvVars(t, ingestorReplicas)
		if t.Deduplication != nil && t.Deduplication.Enabled {
			replicasForSubjects := ingestorReplicas
			if replicasForSubjects <= 0 {
				replicasForSubjects = 1
			}
			subjectCountEnvVars = []v1.EnvVar{{Name: "NATS_SUBJECT_COUNT", Value: fmt.Sprintf("%d", replicasForSubjects)}}
		}

		outputBinding, err := graph.GetOutput(pipelinegraph.IngestorNodeID(p.Spec, i))
		if err != nil {
			return fmt.Errorf("resolve ingestor output for stream %d: %w", i, err)
		}

		err = r.createHeadlessService(ctx, ns.GetName(), resourceRef, ingestorLabels)
		if err != nil {
			return fmt.Errorf("create ingestor headless service %s: %w", resourceRef, err)
		}

		containerBuilder := newComponentContainerBuilder().
			withName(resourceRef).
			withImage(r.Config.Images.Ingestor).
			withImagePullPolicy(r.Config.PullPolicies.Ingestor).
			withVolumeMount(v1.VolumeMount{
				Name:      "config",
				ReadOnly:  true,
				MountPath: "/config",
			}).
			withEnv(append(append(append(append([]v1.EnvVar{
				{Name: "GLASSFLOW_NATS_SERVER", Value: r.Config.NATS.ComponentAddr},
				{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
				{Name: "GLASSFLOW_INGESTOR_TOPIC", Value: t.TopicName},
				{Name: "NATS_SUBJECT_PREFIX", Value: outputBinding.SubjectPrefix},
				{Name: "GLASSFLOW_LOG_LEVEL", Value: r.Config.Observability.LogLevels.Ingestor},

				{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.Config.Observability.LogsEnabled},
				{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.Config.Observability.MetricsEnabled},
				{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.Config.Observability.OTelEndpoint},
				{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: constants.IngestorComponent},
				{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.Config.Observability.ImageTags.Ingestor},
				{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
				{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
				{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				}},
			}, subjectCountEnvVars...), r.getStatefulSetPodIdentityEnvVars()...), r.getComponentDatabaseEnvVars()...), r.getUsageStatsEnvVars()...)).
			withResources(cpuReq, cpuLim, memReq, memLim)
		if mount, ok := r.getComponentEncryptionVolumeMount(); ok {
			containerBuilder = containerBuilder.withVolumeMount(mount)
		}
		container := containerBuilder.build()

		stsBuilder := newComponentStatefulSetBuilder().
			withNamespace(ns).
			withResourceName(resourceRef).
			withServiceName(resourceRef).
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
			withReplicas(ingestorReplicas).
			withContainer(*container).
			withAffinity(r.Config.Affinity.Ingestor)
		if vol, ok := r.getComponentEncryptionVolume(); ok {
			stsBuilder = stsBuilder.withVolume(vol)
		}
		statefulSet := stsBuilder.build()

		err = r.createStatefulSet(ctx, statefulSet)
		if err != nil {
			return fmt.Errorf("create ingestor statefulset %s: %w", resourceRef, err)
		}
	}

	return nil
}

// createJoin creates a join StatefulSet (and headless Service) for the pipeline.
func (r *PipelineReconciler) createJoin(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	if len(p.Spec.Source.Streams) < 2 {
		return fmt.Errorf("join requires at least 2 source streams")
	}

	graph, err := pipelinegraph.NewFromPipelineSpec(p.Spec)
	if err != nil {
		return fmt.Errorf("build pipeline graph for join: %w", err)
	}

	resourceRef := r.getStatefulSetResourceName(p, constants.JoinComponent)
	namespace := ns.GetName()

	joinLabels := r.getJoinLabels()

	maps.Copy(joinLabels, labels)

	cpuReq, cpuLim, memReq, memLim := r.Config.ResourceDefaults.Join.CPURequest, r.Config.ResourceDefaults.Join.CPULimit, r.Config.ResourceDefaults.Join.MemoryRequest, r.Config.ResourceDefaults.Join.MemoryLimit
	joinReplicas := constants.DefaultMinReplicas
	if p.Spec.Resources != nil && p.Spec.Resources.Join != nil {
		comp := p.Spec.Resources.Join
		if comp.Requests != nil {
			cpuReq, memReq = comp.Requests.CPU.String(), comp.Requests.Memory.String()
		}
		if comp.Limits != nil {
			cpuLim, memLim = comp.Limits.CPU.String(), comp.Limits.Memory.String()
		}
		if comp.Replicas != nil {
			joinReplicas = int(*comp.Replicas)
		}
	}

	joinInputs, err := graph.GetJoinInput(pipelinegraph.JoinNodeID())
	if err != nil {
		return fmt.Errorf("resolve join input: %w", err)
	}
	if len(joinInputs.Left.Streams) == 0 {
		return fmt.Errorf("resolve join input: left input has no streams")
	}
	if len(joinInputs.Right.Streams) == 0 {
		return fmt.Errorf("resolve join input: right input has no streams")
	}
	joinOutput, err := graph.GetOutput(pipelinegraph.JoinNodeID())
	if err != nil {
		return fmt.Errorf("resolve join output: %w", err)
	}

	err = r.createHeadlessService(ctx, namespace, resourceRef, joinLabels)
	if err != nil {
		return fmt.Errorf("create join headless service: %w", err)
	}

	joinContainerBuilder := newComponentContainerBuilder().
		withName(resourceRef).
		withImage(r.Config.Images.Join).
		withImagePullPolicy(r.Config.PullPolicies.Join).
		withVolumeMount(v1.VolumeMount{
			Name:      "config",
			ReadOnly:  true,
			MountPath: "/config",
		}).
		withEnv(append(append(append([]v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.Config.NATS.ComponentAddr},
			{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
			{Name: "NATS_LEFT_INPUT_STREAM_PREFIX", Value: joinInputs.Left.Streams[0].Name},
			{Name: "NATS_RIGHT_INPUT_STREAM_PREFIX", Value: joinInputs.Right.Streams[0].Name},
			{Name: "NATS_SUBJECT_PREFIX", Value: joinOutput.SubjectPrefix},
			{Name: "GLASSFLOW_LOG_LEVEL", Value: r.Config.Observability.LogLevels.Join},

			{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.Config.Observability.LogsEnabled},
			{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.Config.Observability.MetricsEnabled},
			{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.Config.Observability.OTelEndpoint},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: constants.JoinComponent},
			{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.Config.Observability.ImageTags.Join},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
			{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
			{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			}},
		}, r.getStatefulSetPodIdentityEnvVars()...), r.getComponentDatabaseEnvVars()...), r.getUsageStatsEnvVars()...)).
		withResources(cpuReq, cpuLim, memReq, memLim)
	if mount, ok := r.getComponentEncryptionVolumeMount(); ok {
		joinContainerBuilder = joinContainerBuilder.withVolumeMount(mount)
	}
	joinContainer := joinContainerBuilder.build()

	stsBuilder := newComponentStatefulSetBuilder().
		withNamespace(ns).
		withResourceName(resourceRef).
		withServiceName(resourceRef).
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
		withContainer(*joinContainer).
		withReplicas(joinReplicas).
		withAffinity(r.Config.Affinity.Join)
	if vol, ok := r.getComponentEncryptionVolume(); ok {
		stsBuilder = stsBuilder.withVolume(vol)
	}
	statefulSet := stsBuilder.build()

	err = r.createStatefulSet(ctx, statefulSet)
	if err != nil {
		return fmt.Errorf("create join statefulset: %w", err)
	}

	return nil
}

// createSink creates a sink StatefulSet (and headless Service) for the pipeline.
func (r *PipelineReconciler) createSink(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	graph, err := pipelinegraph.NewFromPipelineSpec(p.Spec)
	if err != nil {
		return fmt.Errorf("build pipeline graph for sink: %w", err)
	}

	resourceRef := r.getStatefulSetResourceName(p, constants.SinkComponent)
	namespace := ns.GetName()

	sinkLabels := r.getSinkLabels()
	maps.Copy(sinkLabels, labels)

	cpuReq, cpuLim, memReq, memLim := r.Config.ResourceDefaults.Sink.CPURequest, r.Config.ResourceDefaults.Sink.CPULimit, r.Config.ResourceDefaults.Sink.MemoryRequest, r.Config.ResourceDefaults.Sink.MemoryLimit
	sinkReplicas := constants.DefaultMinReplicas
	if p.Spec.Resources != nil && p.Spec.Resources.Sink != nil {
		comp := p.Spec.Resources.Sink
		if comp.Requests != nil {
			cpuReq, memReq = comp.Requests.CPU.String(), comp.Requests.Memory.String()
		}
		if comp.Limits != nil {
			cpuLim, memLim = comp.Limits.CPU.String(), comp.Limits.Memory.String()
		}
		if comp.Replicas != nil {
			sinkReplicas = int(*comp.Replicas)
		}
	}

	sinkInput, err := graph.GetInput(pipelinegraph.SinkNodeID())
	if err != nil {
		return fmt.Errorf("resolve sink input: %w", err)
	}

	err = r.createHeadlessService(ctx, namespace, resourceRef, sinkLabels)
	if err != nil {
		return fmt.Errorf("create sink headless service: %w", err)
	}

	sinkContainerBuilder := newComponentContainerBuilder().
		withName(resourceRef).
		withImage(r.Config.Images.Sink).
		withImagePullPolicy(r.Config.PullPolicies.Sink).
		withVolumeMount(v1.VolumeMount{
			Name:      "config",
			ReadOnly:  true,
			MountPath: "/config",
		}).
		withEnv(append(append(append([]v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.Config.NATS.ComponentAddr},
			{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
			{Name: "NATS_INPUT_STREAM_PREFIX", Value: sinkInput.StreamPrefix},
			{Name: "GLASSFLOW_LOG_LEVEL", Value: r.Config.Observability.LogLevels.Sink},

			{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.Config.Observability.LogsEnabled},
			{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.Config.Observability.MetricsEnabled},
			{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.Config.Observability.OTelEndpoint},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: constants.SinkComponent},
			{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.Config.Observability.ImageTags.Sink},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
			{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
			{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			}},
		}, r.getStatefulSetPodIdentityEnvVars()...), r.getComponentDatabaseEnvVars()...), r.getUsageStatsEnvVars()...)).
		withResources(cpuReq, cpuLim, memReq, memLim)
	if mount, ok := r.getComponentEncryptionVolumeMount(); ok {
		sinkContainerBuilder = sinkContainerBuilder.withVolumeMount(mount)
	}
	sinkContainer := sinkContainerBuilder.build()

	stsBuilder := newComponentStatefulSetBuilder().
		withNamespace(ns).
		withResourceName(resourceRef).
		withServiceName(resourceRef).
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
		withContainer(*sinkContainer).
		withReplicas(sinkReplicas).
		withAffinity(r.Config.Affinity.Sink)
	if vol, ok := r.getComponentEncryptionVolume(); ok {
		stsBuilder = stsBuilder.withVolume(vol)
	}
	statefulSet := stsBuilder.build()

	err = r.createStatefulSet(ctx, statefulSet)
	if err != nil {
		return fmt.Errorf("create sink statefulset: %w", err)
	}

	return nil
}

// createDedups creates dedup StatefulSets for the pipeline.
// For Kafka: one per stream with dedup enabled. For OTLP: one (index 0) when IsDedupEnabled.
func (r *PipelineReconciler) createDedups(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	indices := dedupStreamIndices(p)
	if len(indices) == 0 {
		return nil
	}

	graph, err := pipelinegraph.NewFromPipelineSpec(p.Spec)
	if err != nil {
		return fmt.Errorf("build pipeline graph for dedups: %w", err)
	}

	for _, i := range indices {
		// Resolve storage and Kafka-specific params for this instance.
		storageSize := r.Config.DedupStorage.Size
		storageClass := r.Config.DedupStorage.StorageCls
		var topicName string
		var extraEnv []v1.EnvVar

		if !p.Spec.IsOTLPSource() {
			stream := p.Spec.Source.Streams[i]
			topicName = stream.TopicName
			if stream.Deduplication.StorageSize != "" {
				storageSize = stream.Deduplication.StorageSize
			}
			if stream.Deduplication.StorageClass != "" {
				storageClass = stream.Deduplication.StorageClass
			}
			extraEnv = []v1.EnvVar{
				{Name: "GLASSFLOW_DEDUP_TOPIC", Value: topicName},
				{Name: "GLASSFLOW_SOURCE_INDEX", Value: fmt.Sprintf("%d", i)},
			}
		}

		// Resources.Dedup.Storage overrides both Kafka and OTLP.
		if p.Spec.Resources != nil && p.Spec.Resources.Dedup != nil {
			if comp := p.Spec.Resources.Dedup; comp.Storage != nil && !comp.Storage.Size.IsZero() {
				storageSize = comp.Storage.Size.String()
			}
		}

		if err := r.createSingleDedup(ctx, ns, labels, secret, p, graph, i, topicName, storageSize, storageClass, extraEnv); err != nil {
			return fmt.Errorf("create dedup-%d statefulset: %w", i, err)
		}
	}

	return nil
}

// createSingleDedup builds and creates one dedup StatefulSet.
// topicName and extraEnv carry Kafka-specific values; both are empty/nil for OTLP.
func (r *PipelineReconciler) createSingleDedup(
	ctx context.Context,
	ns v1.Namespace,
	labels map[string]string,
	secret v1.Secret,
	p etlv1alpha1.Pipeline,
	graph *pipelinegraph.Graph,
	index int,
	topicName string,
	storageSize string,
	storageClass string,
	extraEnv []v1.EnvVar,
) error {
	resourceRef := r.getStatefulSetResourceName(p, fmt.Sprintf("dedup-%d", index))

	dedupLabels := r.getDedupLabels(topicName)
	maps.Copy(dedupLabels, labels)

	replicas := constants.DefaultMinReplicas
	cpuReq, cpuLim, memReq, memLim := r.Config.ResourceDefaults.Dedup.CPURequest, r.Config.ResourceDefaults.Dedup.CPULimit, r.Config.ResourceDefaults.Dedup.MemoryRequest, r.Config.ResourceDefaults.Dedup.MemoryLimit
	if p.Spec.Resources != nil && p.Spec.Resources.Dedup != nil {
		comp := p.Spec.Resources.Dedup
		if comp.Requests != nil {
			cpuReq, memReq = comp.Requests.CPU.String(), comp.Requests.Memory.String()
		}
		if comp.Limits != nil {
			cpuLim, memLim = comp.Limits.CPU.String(), comp.Limits.Memory.String()
		}
		if comp.Replicas != nil {
			replicas = int(*comp.Replicas)
		}
	}

	dedupInput, err := graph.GetInput(pipelinegraph.DedupNodeID(p.Spec, index))
	if err != nil {
		return fmt.Errorf("resolve dedup input for index %d: %w", index, err)
	}
	dedupOutput, err := graph.GetOutput(pipelinegraph.DedupNodeID(p.Spec, index))
	if err != nil {
		return fmt.Errorf("resolve dedup output for index %d: %w", index, err)
	}

	if err = r.createHeadlessService(ctx, ns.GetName(), resourceRef, dedupLabels); err != nil {
		return fmt.Errorf("create dedup headless service %s: %w", resourceRef, err)
	}

	dedupEnv := make([]v1.EnvVar, 0, 12+len(extraEnv)+2)
	dedupEnv = append(dedupEnv,
		v1.EnvVar{Name: "GLASSFLOW_NATS_SERVER", Value: r.Config.NATS.ComponentAddr},
		v1.EnvVar{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
		v1.EnvVar{Name: "GLASSFLOW_BADGER_PATH", Value: "/data/badger"},
		v1.EnvVar{Name: "GLASSFLOW_LOG_LEVEL", Value: r.Config.Observability.LogLevels.Dedup},
		v1.EnvVar{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.Config.Observability.LogsEnabled},
		v1.EnvVar{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.Config.Observability.MetricsEnabled},
		v1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.Config.Observability.OTelEndpoint},
		v1.EnvVar{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: constants.DedupComponent},
		v1.EnvVar{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.Config.Observability.ImageTags.Dedup},
		v1.EnvVar{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
		v1.EnvVar{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
		v1.EnvVar{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
	)
	dedupEnv = append(dedupEnv, extraEnv...)
	dedupEnv = append(dedupEnv,
		v1.EnvVar{Name: "NATS_INPUT_STREAM_PREFIX", Value: dedupInput.StreamPrefix},
		v1.EnvVar{Name: "NATS_SUBJECT_PREFIX", Value: dedupOutput.SubjectPrefix},
	)

	containerBuilder := newComponentContainerBuilder().
		withName(resourceRef).
		withImage(r.Config.Images.Dedup).
		withImagePullPolicy(r.Config.PullPolicies.Dedup).
		withVolumeMount(v1.VolumeMount{Name: "config", ReadOnly: true, MountPath: "/config"}).
		withVolumeMount(v1.VolumeMount{Name: "data", MountPath: "/data/badger"}).
		withEnv(append(append(append(dedupEnv, r.getStatefulSetPodIdentityEnvVars()...), r.getComponentDatabaseEnvVars()...), r.getUsageStatsEnvVars()...)).
		withResources(cpuReq, cpuLim, memReq, memLim)
	if mount, ok := r.getComponentEncryptionVolumeMount(); ok {
		containerBuilder = containerBuilder.withVolumeMount(mount)
	}
	container := containerBuilder.build()

	stsBuilder := newComponentStatefulSetBuilder().
		withNamespace(ns).
		withResourceName(resourceRef).
		withServiceName(resourceRef).
		withLabels(dedupLabels).
		withVolume(v1.Volume{
			Name: "config",
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{SecretName: secret.Name, DefaultMode: ptrInt32(0o600)},
			},
		}).
		withReplicas(replicas).
		withContainer(*container).
		withAffinity(r.Config.Affinity.Dedup)

	if p.Spec.Transform.IsDedupEnabled {
		storageSizeQuantity, err := resource.ParseQuantity(storageSize)
		if err != nil {
			return fmt.Errorf("parse storage size for dedup-%d: %w", index, err)
		}
		pvcTemplate := v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data"},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				Resources:   v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: storageSizeQuantity}},
			},
		}
		if storageClass != "" {
			pvcTemplate.Spec.StorageClassName = &storageClass
		}
		stsBuilder = stsBuilder.withVolumeClaimTemplate(pvcTemplate)
	} else {
		stsBuilder = stsBuilder.withVolume(v1.Volume{
			Name:         "data",
			VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}},
		})
	}

	if vol, ok := r.getComponentEncryptionVolume(); ok {
		stsBuilder = stsBuilder.withVolume(vol)
	}

	return r.createStatefulSet(ctx, stsBuilder.build())
}

// dedupStreamIndices returns the stream indices for which a dedup StatefulSet exists.
// For OTLP, returns [0] when dedup is enabled. For Kafka, returns the indices of
// streams that have deduplication enabled.
func dedupStreamIndices(p etlv1alpha1.Pipeline) []int {
	if p.Spec.IsOTLPSource() {
		if p.Spec.Transform.IsDedupEnabled {
			return []int{0}
		}
		return nil
	}
	var out []int
	for i, s := range p.Spec.Source.Streams {
		if s.Deduplication != nil && s.Deduplication.Enabled {
			out = append(out, i)
		}
	}
	return out
}

func isDedupEnabled(p etlv1alpha1.Pipeline) bool {
	return len(dedupStreamIndices(p)) > 0
}

func (r *PipelineReconciler) areDedupStatefulSetsReady(
	ctx context.Context,
	p etlv1alpha1.Pipeline,
	namespace string,
) (bool, error) {
	for _, i := range dedupStreamIndices(p) {
		dedupName := r.getStatefulSetResourceName(p, fmt.Sprintf("%s-%d", constants.DedupComponent, i))
		ready, err := r.isStatefulSetReady(ctx, namespace, dedupName)
		if err != nil {
			return false, fmt.Errorf("check dedup-%d statefulset: %w", i, err)
		}
		if !ready {
			return false, nil
		}
	}

	return true, nil
}

// ensureDedupStatefulSetsReady ensures all dedup StatefulSets are ready for a pipeline.
func (r *PipelineReconciler) ensureDedupStatefulSetsReady(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	ns v1.Namespace,
	labels map[string]string,
	secret v1.Secret,
) (ctrl.Result, error) {
	if !isDedupEnabled(*p) {
		return ctrl.Result{}, nil
	}

	namespace := r.getTargetNamespace(*p)
	allReady, err := r.areDedupStatefulSetsReady(ctx, *p, namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if allReady {
		log.Info("dedup statefulsets are ready", "namespace", namespace)
		return ctrl.Result{}, nil
	}

	timedOut, _ := r.checkOperationTimeout(log, p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, p)
	}

	log.Info("creating dedup statefulsets", "namespace", namespace)
	if err = r.createDedups(ctx, log, ns, labels, secret, *p); err != nil {
		return ctrl.Result{}, fmt.Errorf("create dedup statefulsets: %w", err)
	}

	log.Info("waiting for dedup statefulsets to be ready", "namespace", namespace)
	return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
}
func (r *PipelineReconciler) ensureStatefulSetDeleted(
	ctx context.Context,
	log logr.Logger,
	namespace,
	componentType,
	statefulSetName string,
) (ctrl.Result, error) {
	absent, err := r.isStatefulSetAbsent(ctx, namespace, statefulSetName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check %s statefulset %s: %w", componentType, statefulSetName, err)
	}
	if absent {
		log.Info(componentType+" statefulset is already deleted", "statefulset", statefulSetName, "namespace", namespace)
		return ctrl.Result{}, nil
	}

	log.Info("deleting "+componentType+" statefulset", "statefulset", statefulSetName, "namespace", namespace)
	if err = r.deleteStatefulSetByName(ctx, namespace, statefulSetName); err != nil {
		return ctrl.Result{}, fmt.Errorf("delete %s statefulset %s: %w", componentType, statefulSetName, err)
	}

	return ctrl.Result{Requeue: true, RequeueAfter: componentDeleteRequeueDelay}, nil
}

func (r *PipelineReconciler) reconcilePipelineTeardown(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	checkPendingMessages bool,
) (ctrl.Result, error) {
	namespace := r.getTargetNamespace(*p)

	stages := []func(context.Context, logr.Logger, *etlv1alpha1.Pipeline, string, bool) (ctrl.Result, error){
		r.reconcileIngestorTeardown,
		r.reconcileDedupTeardown,
		r.reconcileJoinTeardown,
		r.reconcileSinkTeardown,
	}

	for _, stage := range stages {
		result, err := stage(ctx, log, p, namespace, checkPendingMessages)
		if err != nil || result.Requeue {
			return result, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileIngestorTeardown(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	namespace string,
	_ bool,
) (ctrl.Result, error) {
	if p.Spec.IsOTLPSource() {
		return ctrl.Result{}, nil
	}

	for i := range p.Spec.Source.Streams {
		deploymentName := r.getStatefulSetResourceName(*p, fmt.Sprintf("%s-%d", constants.IngestorComponent, i))
		result, err := r.ensureStatefulSetDeleted(ctx, log, namespace, "ingestor", deploymentName)
		if err != nil || result.Requeue {
			return result, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileDedupTeardown(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	namespace string,
	checkPendingMessages bool,
) (ctrl.Result, error) {
	for _, i := range dedupStreamIndices(*p) {
		if checkPendingMessages {
			if err := r.checkDedupPendingMessages(ctx, *p, i); err != nil {
				if errs.IsConsumerPendingMessagesError(err) {
					log.Info("dedup has pending messages, requeuing", "pipeline_id", p.Spec.ID, "stream_index", i)
					return ctrl.Result{Requeue: true, RequeueAfter: pendingMessagesRequeueDelay}, nil
				}
				return ctrl.Result{}, fmt.Errorf("check dedup pending messages for index %d: %w", i, err)
			}
		}

		statefulSetName := r.getStatefulSetResourceName(*p, fmt.Sprintf("%s-%d", constants.DedupComponent, i))
		result, err := r.ensureStatefulSetDeleted(ctx, log, namespace, "dedup", statefulSetName)
		if err != nil || result.Requeue {
			return result, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *PipelineReconciler) reconcileJoinTeardown(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	namespace string,
	checkPendingMessages bool,
) (ctrl.Result, error) {
	if !p.Spec.Join.Enabled {
		return ctrl.Result{}, nil
	}

	if checkPendingMessages {
		err := r.checkJoinPendingMessages(ctx, *p)
		if err != nil {
			if errs.IsConsumerPendingMessagesError(err) {
				log.Info("join has pending messages, requeuing operation", "pipeline_id", p.Spec.ID, "error", err.Error())
				return ctrl.Result{Requeue: true, RequeueAfter: pendingMessagesRequeueDelay}, nil
			}
			return ctrl.Result{}, fmt.Errorf("check join pending messages: %w", err)
		}
	}

	statefulSetName := r.getStatefulSetResourceName(*p, constants.JoinComponent)
	return r.ensureStatefulSetDeleted(ctx, log, namespace, constants.JoinComponent, statefulSetName)
}

func (r *PipelineReconciler) reconcileSinkTeardown(
	ctx context.Context,
	log logr.Logger,
	p *etlv1alpha1.Pipeline,
	namespace string,
	checkPendingMessages bool,
) (ctrl.Result, error) {
	if checkPendingMessages {
		err := r.checkSinkPendingMessages(ctx, *p)
		if err != nil {
			if errs.IsConsumerPendingMessagesError(err) {
				log.Info("sink has pending messages, requeuing operation", "pipeline_id", p.Spec.ID, "error", err.Error())
				return ctrl.Result{Requeue: true, RequeueAfter: pendingMessagesRequeueDelay}, nil
			}
			return ctrl.Result{}, fmt.Errorf("check sink pending messages: %w", err)
		}
	}

	deploymentName := r.getStatefulSetResourceName(*p, constants.SinkComponent)
	return r.ensureStatefulSetDeleted(ctx, log, namespace, constants.SinkComponent, deploymentName)
}
