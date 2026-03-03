package controller

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/errs"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/utils"
)

// stopPipelineComponents stops all pipeline components in the correct order with pending message checks
// nolint:gocyclo
func (r *PipelineReconciler) stopPipelineComponents(ctx context.Context, log logr.Logger, p *etlv1alpha1.Pipeline) (ctrl.Result, error) {
	namespace := r.getTargetNamespace(*p)

	// Step 1: Stop Ingestor deployments
	for i := range p.Spec.Ingestor.Streams {
		deploymentName := r.getResourceName(*p, fmt.Sprintf("%s-%d", constants.IngestorComponent, i))
		deleted, err := r.isDeploymentAbsent(ctx, namespace, deploymentName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check ingestor deployment %s: %w", deploymentName, err)
		}
		if !deleted {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(log, p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, p)
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

	// Step 2: Stop Dedup StatefulSets
	for i, stream := range p.Spec.Ingestor.Streams {
		if stream.Deduplication == nil || !stream.Deduplication.Enabled {
			continue
		}

		dedupName := r.getResourceName(*p, fmt.Sprintf("dedup-%d", i))

		// Check for timeout
		timedOut, _ := r.checkOperationTimeout(log, p)
		if timedOut {
			return r.handleOperationTimeout(ctx, log, p)
		}

		// Check for pending messages first
		err := r.checkDedupPendingMessages(ctx, *p, i)
		if err != nil {
			if errs.IsConsumerPendingMessagesError(err) {
				log.Info("dedup has pending messages, requeuing", "pipeline_id", p.Spec.ID, "stream_index", i, "error", err.Error())
				return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
			}
			return ctrl.Result{}, fmt.Errorf("check dedup pending messages for stream %d: %w", i, err)
		}

		deleted, err := r.isStatefulSetAbsent(ctx, namespace, dedupName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check dedup statefulset %s: %w", dedupName, err)
		}
		if !deleted {
			log.Info("deleting dedup statefulset", "statefulset", dedupName, "namespace", namespace)
			var sts appsv1.StatefulSet
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dedupName}, &sts)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("get dedup statefulset %s: %w", dedupName, err)
				}
			} else {
				err = r.deleteStatefulSet(ctx, &sts)
				if err != nil {
					return ctrl.Result{}, fmt.Errorf("delete dedup statefulset %s: %w", dedupName, err)
				}
			}
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("dedup statefulset is already deleted", "statefulset", dedupName, "namespace", namespace)
		}
	}

	// Step 3: Check join and stop Join deployment (if enabled)
	if p.Spec.Join.Enabled {
		// Check for timeout before checking pending messages
		timedOut, _ := r.checkOperationTimeout(log, p)
		if timedOut {
			return r.handleOperationTimeout(ctx, log, p)
		}

		// Check for pending messages first
		err := r.checkJoinPendingMessages(ctx, *p)
		if err != nil {
			if errs.IsConsumerPendingMessagesError(err) {
				log.Info("join has pending messages, requeuing operation",
					"pipeline_id", p.Spec.ID,
					"error", err.Error())
				return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
			}
			return ctrl.Result{}, fmt.Errorf("check join pending messages: %w", err)
		}

		deleted, err := r.isDeploymentAbsent(ctx, namespace, r.getResourceName(*p, constants.JoinComponent))
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !deleted {
			// Check for timeout before requeuing
			timedOut, _ := r.checkOperationTimeout(log, p)
			if timedOut {
				return r.handleOperationTimeout(ctx, log, p)
			}

			log.Info("deleting join deployment", "namespace", namespace)
			var deployment appsv1.Deployment
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: r.getResourceName(*p, constants.JoinComponent)}, &deployment)
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
	timedOut, _ := r.checkOperationTimeout(log, p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, p)
	}

	// Check for pending messages first
	err := r.checkSinkPendingMessages(ctx, *p)
	if err != nil {
		if errs.IsConsumerPendingMessagesError(err) {
			log.Info("sink has pending messages, requeuing operation",
				"pipeline_id", p.Spec.ID,
				"error", err.Error())
			return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, fmt.Errorf("check sink pending messages: %w", err)
	}

	deleted, err := r.isDeploymentAbsent(ctx, namespace, r.getResourceName(*p, constants.SinkComponent))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !deleted {
		// Check for timeout before requeuing
		timedOut, _ := r.checkOperationTimeout(log, p)
		if timedOut {
			return r.handleOperationTimeout(ctx, log, p)
		}

		log.Info("deleting sink deployment", "namespace", namespace)
		var deployment appsv1.Deployment
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: r.getResourceName(*p, constants.SinkComponent)}, &deployment)
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
		deploymentName := r.getResourceName(p, fmt.Sprintf("%s-%d", constants.IngestorComponent, i))
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

	// Step 2: Stop Dedup StatefulSets (no pending message checks in terminate)
	for i, stream := range p.Spec.Ingestor.Streams {
		if stream.Deduplication == nil || !stream.Deduplication.Enabled {
			continue
		}

		dedupName := r.getResourceName(p, fmt.Sprintf("dedup-%d", i))
		deleted, err := r.isStatefulSetAbsent(ctx, namespace, dedupName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check dedup statefulset %s: %w", dedupName, err)
		}
		if !deleted {
			log.Info("deleting dedup statefulset", "statefulset", dedupName, "namespace", namespace)
			var sts appsv1.StatefulSet
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dedupName}, &sts)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("get dedup statefulset %s: %w", dedupName, err)
				}
			} else {
				err = r.deleteStatefulSet(ctx, &sts)
				if err != nil {
					return ctrl.Result{}, fmt.Errorf("delete dedup statefulset %s: %w", dedupName, err)
				}
			}
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
		} else {
			log.Info("dedup statefulset is already deleted", "statefulset", dedupName, "namespace", namespace)
		}
	}

	// Step 3: Check join and stop Join deployment (if enabled)
	if p.Spec.Join.Enabled {
		deleted, err := r.isDeploymentAbsent(ctx, namespace, r.getResourceName(p, constants.JoinComponent))
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check join deployment: %w", err)
		}
		if !deleted {
			log.Info("deleting join deployment", "namespace", namespace)
			var deployment appsv1.Deployment
			err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p, constants.JoinComponent)}, &deployment)
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
	deleted, err := r.isDeploymentAbsent(ctx, namespace, r.getResourceName(p, constants.SinkComponent))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check sink deployment: %w", err)
	}
	if !deleted {
		log.Info("deleting sink deployment", "namespace", namespace)
		var deployment appsv1.Deployment
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: r.getResourceName(p, constants.SinkComponent)}, &deployment)
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

// createIngestors creates ingestor deployments for the pipeline
func (r *PipelineReconciler) createIngestors(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	ing := p.Spec.Ingestor

	natsMaxAge, natsMaxBytes := r.resolveNatsStreamEnvVars(p)

	for i, t := range ing.Streams {
		resourceRef := r.getResourceName(p, fmt.Sprintf("%s-%d", constants.IngestorComponent, i))

		ingestorLabels := r.getKafkaIngestorLabels(t.TopicName)
		maps.Copy(ingestorLabels, labels)

		cpuReq, cpuLim, memReq, memLim := r.IngestorCPURequest, r.IngestorCPULimit, r.IngestorMemoryRequest, r.IngestorMemoryLimit
		ingestorReplicas := t.Replicas
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

		err := r.createHeadlessService(ctx, namespace, resourceRef, ingestorLabels)
		if err != nil {
			return fmt.Errorf("create ingestor headless service %s: %w", resourceRef, err)
		}

		containerBuilder := newComponentContainerBuilder().
			withName(resourceRef).
			withImage(r.IngestorImage).
			withImagePullPolicy(r.IngestorPullPolicy).
			withVolumeMount(v1.VolumeMount{
				Name:      "config",
				ReadOnly:  true,
				MountPath: "/config",
			}).
			withEnv(append(append(append(append([]v1.EnvVar{
				{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
				{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
				{Name: "GLASSFLOW_INGESTOR_TOPIC", Value: t.TopicName},
				{Name: "NATS_SUBJECT_PREFIX", Value: getIngestorOutputSubjectPrefix(p.Spec.ID, t.TopicName)},
				{Name: "GLASSFLOW_LOG_LEVEL", Value: r.IngestorLogLevel},
				{Name: "GLASSFLOW_NATS_MAX_STREAM_AGE", Value: natsMaxAge},
				{Name: "GLASSFLOW_NATS_MAX_STREAM_BYTES", Value: natsMaxBytes},

				{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.ObservabilityLogsEnabled},
				{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.ObservabilityMetricsEnabled},
				{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.ObservabilityOTelEndpoint},
				{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: constants.IngestorComponent},
				{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.IngestorImageTag},
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
			withAffinity(r.IngestorAffinity)
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

// createJoin creates a join deployment for the pipeline
func (r *PipelineReconciler) createJoin(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	resourceRef := r.getResourceName(p, constants.JoinComponent)

	joinLabels := r.getJoinLabels()

	maps.Copy(joinLabels, labels)
	cpuReq, cpuLim, memReq, memLim := r.JoinCPURequest, r.JoinCPULimit, r.JoinMemoryRequest, r.JoinMemoryLimit
	if p.Spec.Resources != nil && p.Spec.Resources.Join != nil {
		comp := p.Spec.Resources.Join
		if comp.Requests != nil {
			cpuReq, memReq = comp.Requests.CPU.String(), comp.Requests.Memory.String()
		}
		if comp.Limits != nil {
			cpuLim, memLim = comp.Limits.CPU.String(), comp.Limits.Memory.String()
		}
	}
	natsMaxAge, natsMaxBytes := r.resolveNatsStreamEnvVars(p)

	joinContainerBuilder := newComponentContainerBuilder().
		withName(resourceRef).
		withImage(r.JoinImage).
		withImagePullPolicy(r.JoinPullPolicy).
		withVolumeMount(v1.VolumeMount{
			Name:      "config",
			ReadOnly:  true,
			MountPath: "/config",
		}).
		withEnv(append(append([]v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
			{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
			{Name: "GLASSFLOW_LOG_LEVEL", Value: r.JoinLogLevel},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_AGE", Value: natsMaxAge},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_BYTES", Value: natsMaxBytes},

			{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.ObservabilityLogsEnabled},
			{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.ObservabilityMetricsEnabled},
			{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.ObservabilityOTelEndpoint},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: constants.JoinComponent},
			{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.JoinImageTag},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
			{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
			{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			}},
		}, r.getComponentDatabaseEnvVars()...), r.getUsageStatsEnvVars()...)).
		withResources(cpuReq, cpuLim, memReq, memLim)
	if mount, ok := r.getComponentEncryptionVolumeMount(); ok {
		joinContainerBuilder = joinContainerBuilder.withVolumeMount(mount)
	}
	joinContainer := joinContainerBuilder.build()

	joinDepBuilder := newComponentDeploymentBuilder().
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
		withContainer(*joinContainer).
		withAffinity(r.JoinAffinity)
	if p.Spec.Resources != nil && p.Spec.Resources.Join != nil && p.Spec.Resources.Join.Replicas != nil {
		joinDepBuilder = joinDepBuilder.withReplicas(int(*p.Spec.Resources.Join.Replicas))
	}
	if vol, ok := r.getComponentEncryptionVolume(); ok {
		joinDepBuilder = joinDepBuilder.withVolume(vol)
	}
	deployment := joinDepBuilder.build()

	err := r.createDeployment(ctx, deployment)
	if err != nil {
		return fmt.Errorf("create join deployment: %w", err)
	}

	return nil
}

// createSink creates a sink StatefulSet (and headless Service) for the pipeline.
func (r *PipelineReconciler) createSink(ctx context.Context, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	resourceRef := r.getResourceName(p, constants.SinkComponent)
	namespace := ns.GetName()

	sinkLabels := r.getSinkLabels()
	maps.Copy(sinkLabels, labels)
	cpuReq, cpuLim, memReq, memLim := r.SinkCPURequest, r.SinkCPULimit, r.SinkMemoryRequest, r.SinkMemoryLimit
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
	natsMaxAge, natsMaxBytes := r.resolveNatsStreamEnvVars(p)

	err := r.createHeadlessService(ctx, namespace, resourceRef, sinkLabels)
	if err != nil {
		return fmt.Errorf("create sink headless service: %w", err)
	}

	sinkContainerBuilder := newComponentContainerBuilder().
		withName(resourceRef).
		withImage(r.SinkImage).
		withImagePullPolicy(r.SinkPullPolicy).
		withVolumeMount(v1.VolumeMount{
			Name:      "config",
			ReadOnly:  true,
			MountPath: "/config",
		}).
		withEnv(append(append(append([]v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
			{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
			{Name: "NATS_INPUT_STREAM_PREFIX", Value: getSinkInputStreamPrefix(p.Spec.ID)},
			{Name: "GLASSFLOW_LOG_LEVEL", Value: r.SinkLogLevel},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_AGE", Value: natsMaxAge},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_BYTES", Value: natsMaxBytes},

			{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.ObservabilityLogsEnabled},
			{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.ObservabilityMetricsEnabled},
			{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.ObservabilityOTelEndpoint},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: constants.SinkComponent},
			{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.SinkImageTag},
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
		withAffinity(r.SinkAffinity)
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

// createDedups creates dedup StatefulSets for the pipeline (one per ingestor stream with dedup enabled)
func (r *PipelineReconciler) createDedups(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	ing := p.Spec.Ingestor
	natsMaxAge, natsMaxBytes := r.resolveNatsStreamEnvVars(p)

	for i, stream := range ing.Streams {
		// Skip if dedup not enabled for this stream
		if stream.Deduplication == nil || !stream.Deduplication.Enabled {
			continue
		}

		resourceRef := r.getResourceName(p, fmt.Sprintf("dedup-%d", i))
		serviceName := resourceRef

		dedupLabels := r.getDedupLabels(stream.TopicName)
		maps.Copy(dedupLabels, labels)

		replicas := constants.DefaultMinReplicas
		cpuReq, cpuLim, memReq, memLim := r.DedupCPURequest, r.DedupCPULimit, r.DedupMemoryRequest, r.DedupMemoryLimit

		err := r.createHeadlessService(ctx, ns.GetName(), serviceName, dedupLabels)
		if err != nil {
			return fmt.Errorf("create dedup headless service %s: %w", serviceName, err)
		}

		// Determine storage size (use pipeline config, fallback to Helm default)
		storageSize := r.DedupDefaultStorageSize
		if stream.Deduplication.StorageSize != "" {
			storageSize = stream.Deduplication.StorageSize
		}
		if p.Spec.Resources != nil && p.Spec.Resources.Dedup != nil {
			comp := p.Spec.Resources.Dedup
			if comp.Requests != nil {
				cpuReq, memReq = comp.Requests.CPU.String(), comp.Requests.Memory.String()
			}
			if comp.Limits != nil {
				cpuLim, memLim = comp.Limits.CPU.String(), comp.Limits.Memory.String()
			}
			if comp.Storage != nil && !comp.Storage.Size.IsZero() {
				storageSize = comp.Storage.Size.String()
			}
			if comp.Replicas != nil {
				replicas = int(*comp.Replicas)
			}
		}

		// Determine storage class (use pipeline config, fallback to Helm default)
		storageClass := r.DedupDefaultStorageClass
		if stream.Deduplication.StorageClass != "" {
			storageClass = stream.Deduplication.StorageClass
		}

		dedupEnvBase := []v1.EnvVar{
			{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
			{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
			{Name: "GLASSFLOW_DEDUP_TOPIC", Value: stream.TopicName},
			{Name: "GLASSFLOW_SOURCE_INDEX", Value: fmt.Sprintf("%d", i)},
			{Name: "GLASSFLOW_BADGER_PATH", Value: "/data/badger"},
			{Name: "GLASSFLOW_LOG_LEVEL", Value: r.DedupLogLevel},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_AGE", Value: natsMaxAge},
			{Name: "GLASSFLOW_NATS_MAX_STREAM_BYTES", Value: natsMaxBytes},

			{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.ObservabilityLogsEnabled},
			{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.ObservabilityMetricsEnabled},
			{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.ObservabilityOTelEndpoint},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: constants.DedupComponent},
			{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.DedupImageTag},
			{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
			{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
			{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			}},
		}
		if p.Spec.Join.Enabled {
			dedupEnvBase = append(dedupEnvBase,
				v1.EnvVar{Name: "GLASSFLOW_INPUT_STREAM", Value: getIngestorOutputSubjectPrefix(p.Spec.ID, stream.TopicName)},
				v1.EnvVar{Name: "GLASSFLOW_OUTPUT_STREAM", Value: getDedupOutputSubjectPrefix(p.Spec.ID, stream.TopicName)},
			)
		} else {
			dedupEnvBase = append(dedupEnvBase,
				v1.EnvVar{Name: "NATS_INPUT_STREAM_PREFIX", Value: getDedupInputStreamPrefix(p.Spec.ID, stream.TopicName)},
				v1.EnvVar{Name: "NATS_SUBJECT_PREFIX", Value: getDedupOutputSubjectPrefix(p.Spec.ID, stream.TopicName)},
			)
		}
		dedupContainerBuilder := newComponentContainerBuilder().
			withName(resourceRef).
			withImage(r.DedupImage).
			withImagePullPolicy(r.DedupPullPolicy).
			withVolumeMount(v1.VolumeMount{
				Name:      "config",
				ReadOnly:  true,
				MountPath: "/config",
			}).
			withVolumeMount(v1.VolumeMount{
				Name:      "data",
				MountPath: "/data/badger",
			}).
			withEnv(append(append(append(dedupEnvBase, r.getStatefulSetPodIdentityEnvVars()...), r.getComponentDatabaseEnvVars()...), r.getUsageStatsEnvVars()...)).
			withResources(cpuReq, cpuLim, memReq, memLim)
		if mount, ok := r.getComponentEncryptionVolumeMount(); ok {
			dedupContainerBuilder = dedupContainerBuilder.withVolumeMount(mount)
		}
		container := dedupContainerBuilder.build()

		// Parse storage size
		storageSizeQuantity, err := resource.ParseQuantity(storageSize)
		if err != nil {
			return fmt.Errorf("parse storage size for dedup-%d: %w", i, err)
		}

		// Create PVC template
		pvcTemplate := v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data"},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceStorage: storageSizeQuantity},
				},
			},
		}

		if storageClass != "" {
			pvcTemplate.Spec.StorageClassName = &storageClass
		}

		stsBuilder := newComponentStatefulSetBuilder().
			withNamespace(ns).
			withResourceName(resourceRef).
			withServiceName(serviceName).
			withLabels(dedupLabels).
			withVolume(v1.Volume{
				Name: "config",
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName:  secret.Name,
						DefaultMode: ptrInt32(0o600),
					},
				},
			}).
			withReplicas(replicas).
			withContainer(*container).
			withAffinity(r.DedupAffinity).
			withVolumeClaimTemplate(pvcTemplate)
		if vol, ok := r.getComponentEncryptionVolume(); ok {
			stsBuilder = stsBuilder.withVolume(vol)
		}
		statefulSet := stsBuilder.build()

		err = r.createStatefulSet(ctx, statefulSet)
		if err != nil {
			return fmt.Errorf("create dedup-%d statefulset: %w", i, err)
		}

	}

	return nil
}

// resolveNatsStreamEnvVars returns NATS stream env var values, applying per-pipeline CRD overrides
// on top of the operator-level defaults.
func (r *PipelineReconciler) resolveNatsStreamEnvVars(p etlv1alpha1.Pipeline) (natsMaxAge, natsMaxBytes string) {
	natsMaxAge = r.NATSMaxStreamAge
	natsMaxBytes = utils.ConvertBytesToString(r.NATSMaxStreamBytes)
	if p.Spec.Resources == nil || p.Spec.Resources.Nats == nil || p.Spec.Resources.Nats.Stream == nil {
		return
	}
	stream := p.Spec.Resources.Nats.Stream
	if stream.MaxAge.Duration != 0 {
		natsMaxAge = stream.MaxAge.Duration.String()
	}
	if !stream.MaxBytes.IsZero() {
		natsMaxBytes = fmt.Sprintf("%d", stream.MaxBytes.Value())
	}
	return
}
