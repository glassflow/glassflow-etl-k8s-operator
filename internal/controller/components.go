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
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/utils"
)

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
			timedOut, _ := r.checkOperationTimeout(log, p)
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

	// Step 2: Stop Dedup StatefulSets
	for i, stream := range p.Spec.Ingestor.Streams {
		if stream.Deduplication == nil || !stream.Deduplication.Enabled {
			continue
		}

		dedupName := r.getResourceName(*p, fmt.Sprintf("dedup-%d", i))

		// Check for timeout
		timedOut, _ := r.checkOperationTimeout(log, p)
		if timedOut {
			return r.handleOperationTimeout(ctx, log, p, constants.OperationStop)
		}

		// Check for pending messages first
		err := r.checkDedupPendingMessages(ctx, *p, i)
		if err != nil {
			log.Info("dedup has pending messages, requeuing", "dedup", dedupName, "error", err.Error())
			return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
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
			timedOut, _ := r.checkOperationTimeout(log, p)
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
	timedOut, _ := r.checkOperationTimeout(log, p)
	if timedOut {
		return r.handleOperationTimeout(ctx, log, p, constants.OperationStop)
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
		timedOut, _ := r.checkOperationTimeout(log, p)
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

// createIngestors creates ingestor deployments for the pipeline
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

// createJoin creates a join deployment for the pipeline
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

// createSink creates a sink deployment for the pipeline
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

// createDedups creates dedup StatefulSets for the pipeline (one per ingestor stream with dedup enabled)
func (r *PipelineReconciler) createDedups(ctx context.Context, _ logr.Logger, ns v1.Namespace, labels map[string]string, secret v1.Secret, p etlv1alpha1.Pipeline) error {
	ing := p.Spec.Ingestor

	for i, stream := range ing.Streams {
		// Skip if dedup not enabled for this stream
		if stream.Deduplication == nil || !stream.Deduplication.Enabled {
			continue
		}

		resourceRef := r.getResourceName(p, fmt.Sprintf("dedup-%d", i))
		serviceName := resourceRef

		dedupLabels := r.getDedupLabels(stream.TopicName)
		maps.Copy(dedupLabels, labels)

		// Determine replicas (default to 1)
		replicas := 1
		if stream.Deduplication.Replicas > 0 {
			replicas = stream.Deduplication.Replicas
		}

		// Determine storage size (default to 10Gi)
		storageSize := "10Gi"
		if stream.Deduplication.StorageSize != "" {
			storageSize = stream.Deduplication.StorageSize
		}

		container := newComponentContainerBuilder().
			withName(resourceRef).
			withImage(r.DedupImage).
			withVolumeMount(v1.VolumeMount{
				Name:      "config",
				ReadOnly:  true,
				MountPath: "/config",
			}).
			withVolumeMount(v1.VolumeMount{
				Name:      "data",
				MountPath: "/data/badger",
			}).
			withEnv([]v1.EnvVar{
				{Name: "GLASSFLOW_NATS_SERVER", Value: r.ComponentNATSAddr},
				{Name: "GLASSFLOW_PIPELINE_CONFIG", Value: "/config/pipeline.json"},
				{Name: "GLASSFLOW_SOURCE_INDEX", Value: fmt.Sprintf("%d", i)},
				{Name: "GLASSFLOW_INPUT_STREAM", Value: stream.OutputStream},
				{Name: "GLASSFLOW_OUTPUT_STREAM", Value: stream.Deduplication.OutputStream},
				{Name: "GLASSFLOW_BADGER_PATH", Value: "/data/badger"},
				{Name: "GLASSFLOW_LOG_LEVEL", Value: r.DedupLogLevel},
				{Name: "GLASSFLOW_NATS_MAX_STREAM_AGE", Value: r.NATSMaxStreamAge},
				{Name: "GLASSFLOW_NATS_MAX_STREAM_BYTES", Value: utils.ConvertBytesToString(r.NATSMaxStreamBytes)},

				{Name: "GLASSFLOW_OTEL_LOGS_ENABLED", Value: r.ObservabilityLogsEnabled},
				{Name: "GLASSFLOW_OTEL_METRICS_ENABLED", Value: r.ObservabilityMetricsEnabled},
				{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: r.ObservabilityOTelEndpoint},
				{Name: "GLASSFLOW_OTEL_SERVICE_NAME", Value: "dedup"},
				{Name: "GLASSFLOW_OTEL_SERVICE_VERSION", Value: r.DedupImageTag},
				{Name: "GLASSFLOW_OTEL_SERVICE_NAMESPACE", Value: r.getTargetNamespace(p)},
				{Name: "GLASSFLOW_OTEL_PIPELINE_ID", Value: p.Spec.ID},
				{Name: "GLASSFLOW_OTEL_SERVICE_INSTANCE_ID", ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				}},
			}).
			withResources(r.DedupCPURequest, r.DedupCPULimit, r.DedupMemoryRequest, r.DedupMemoryLimit).
			build()

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

		if stream.Deduplication.StorageClass != "" {
			pvcTemplate.Spec.StorageClassName = &stream.Deduplication.StorageClass
		}

		// Build StatefulSet
		statefulSet := newComponentStatefulSetBuilder().
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
			withVolumeClaimTemplate(pvcTemplate).
			build()

		err = r.createStatefulSet(ctx, statefulSet)
		if err != nil {
			return fmt.Errorf("create dedup-%d statefulset: %w", i, err)
		}

		// Create headless service for StatefulSet
		err = r.createDedupService(ctx, ns, resourceRef, dedupLabels)
		if err != nil {
			return fmt.Errorf("create dedup-%d service: %w", i, err)
		}
	}

	return nil
}

// createDedupService creates a headless service for dedup StatefulSet
func (r *PipelineReconciler) createDedupService(ctx context.Context, ns v1.Namespace, name string, labels map[string]string) error {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns.Name,
			Labels:    labels,
		},
		Spec: v1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Ports: []v1.ServicePort{
				{Name: "metrics", Port: 8080, Protocol: v1.ProtocolTCP},
			},
		},
	}

	err := r.Create(ctx, service)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create service: %w", err)
	}
	return nil
}
