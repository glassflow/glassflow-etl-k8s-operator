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
	"fmt"
	"strconv"
	"strings"

	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	v1 "k8s.io/api/core/v1"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
)

// getKafkaIngestorLabels returns labels for Kafka ingestor components
func (r *PipelineReconciler) getKafkaIngestorLabels(topic string) map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": constants.IngestorComponent,
		"etl.glassflow.io/topic":     topic,
	}

	return labels
}

// getJoinLabels returns labels for join components
func (r *PipelineReconciler) getJoinLabels() map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": constants.JoinComponent,
	}

	return labels
}

// getSinkLabels returns labels for sink components
func (r *PipelineReconciler) getSinkLabels() map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": constants.SinkComponent,
	}

	return labels
}

// getDedupLabels returns labels for dedup components
func (r *PipelineReconciler) getDedupLabels(topic string) map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": constants.DedupComponent,
		"etl.glassflow.io/topic":     topic,
	}

	return labels
}

// getEffectiveOutputStream returns the effective output stream name for a source stream,
// accounting for deduplication. If dedup is enabled and has an output stream configured,
// returns the dedup output stream. Otherwise returns the stream's standard output stream.
func getEffectiveOutputStream(stream etlv1alpha1.SourceStream) string {
	if stream.Deduplication != nil &&
		stream.Deduplication.Enabled &&
		stream.Deduplication.OutputStream != "" {
		return stream.Deduplication.OutputStream
	}
	return stream.OutputStream
}

// getSinkReplicaCount returns the sink replica count from the pipeline spec. Defaults to 2 when unset or 0; minimum 2 when set.
func getSinkReplicaCount(p etlv1alpha1.Pipeline) int {
	if p.Spec.Sink.Replicas >= 2 {
		return p.Spec.Sink.Replicas
	}
	return 2
}

// getDedupReplicaCount returns the dedup replica count for a stream. Defaults to 3 when dedup is enabled and Replicas is unset/0; minimum 3 when set. Returns 1 when dedup is disabled (unused).
func getDedupReplicaCount(stream etlv1alpha1.SourceStream) int {
	if stream.Deduplication == nil || !stream.Deduplication.Enabled {
		return 1
	}
	if stream.Deduplication.Replicas >= 3 {
		return stream.Deduplication.Replicas
	}
	return 3
}

// useDedupNStreamPath returns true when the pipeline uses the M→D→N dedup path: single topic, no join, dedup enabled for that topic.
// In this path we create N sink streams (dedup output subjects) and D dedup input streams (ingestor subjects), not s.OutputStream / s.Deduplication.OutputStream.
func useDedupNStreamPath(p etlv1alpha1.Pipeline) bool {
	if p.Spec.Join.Enabled || len(p.Spec.Ingestor.Streams) != 1 {
		return false
	}
	s := &p.Spec.Ingestor.Streams[0]
	return s.Deduplication != nil && s.Deduplication.Enabled
}

// ingestorNATSSubjectCountEnvVars returns NATS_SUBJECT_COUNT=M (ingestor replica count) when dedup is enabled for the stream.
func ingestorNATSSubjectCountEnvVars(stream etlv1alpha1.SourceStream) []v1.EnvVar {
	if stream.Deduplication == nil || !stream.Deduplication.Enabled {
		return nil
	}
	M := stream.Replicas
	if M <= 0 {
		M = 1
	}
	return []v1.EnvVar{{Name: "NATS_SUBJECT_COUNT", Value: strconv.Itoa(M)}}
}

// preparePipelineLabels returns labels for pipeline resources
func preparePipelineLabels(p etlv1alpha1.Pipeline) map[string]string {
	return map[string]string{"etl.glassflow.io/glassflow-etl-k8s-operator-id": p.Spec.ID}
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

// ptrInt32 returns a pointer to an int32
func ptrInt32(i int32) *int32 {
	return &i
}

// ptrInt64 returns a pointer to an int64
func ptrInt64(i int64) *int64 {
	return &i
}

// ptrBool returns a pointer to a bool
func ptrBool(v bool) *bool {
	return &v
}

// getUsageStatsEnvVars returns environment variables for usage stats configuration
// Values are passed directly (not from secrets)
func (r *PipelineReconciler) getUsageStatsEnvVars() []v1.EnvVar {
	if !r.UsageStatsEnabled {
		return []v1.EnvVar{}
	}

	envVars := []v1.EnvVar{
		{Name: "GLASSFLOW_USAGE_STATS_ENABLED", Value: "true"},
	}

	if r.UsageStatsEndpoint != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_USAGE_STATS_ENDPOINT",
			Value: r.UsageStatsEndpoint,
		})
	}

	if r.UsageStatsUsername != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_USAGE_STATS_USERNAME",
			Value: r.UsageStatsUsername,
		})
	}

	if r.UsageStatsPassword != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_USAGE_STATS_PASSWORD",
			Value: r.UsageStatsPassword,
		})
	}

	if r.UsageStatsInstallationID != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_USAGE_STATS_INSTALLATION_ID",
			Value: r.UsageStatsInstallationID,
		})
	}

	return envVars
}

// getStatefulSetPodIdentityEnvVars returns env vars that inject the Kubernetes pod index by reference
// (downward API). StatefulSet adds the label apps.kubernetes.io/pod-index to each pod; this exposes
// that ordinal (0, 1, 2, ...) as GLASSFLOW_POD_INDEX.
func (r *PipelineReconciler) getStatefulSetPodIdentityEnvVars() []v1.EnvVar {
	return []v1.EnvVar{
		{
			Name: "GLASSFLOW_POD_INDEX",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.labels['apps.kubernetes.io/pod-index']",
				},
			},
		},
	}
}

// getComponentDatabaseEnvVars returns GLASSFLOW_DATABASE_URL from the component secret (same as API).
func (r *PipelineReconciler) getComponentDatabaseEnvVars() []v1.EnvVar {
	if r.DatabaseURL == "" {
		return nil
	}
	return []v1.EnvVar{
		{
			Name: "GLASSFLOW_DATABASE_URL",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{Name: constants.ComponentDatabaseSecretName},
					Key:                  constants.ComponentDatabaseSecretKey,
				},
			},
		},
	}
}

// getComponentEncryptionVolume returns the encryption secret volume (same as API) when encryption is enabled.
func (r *PipelineReconciler) getComponentEncryptionVolume() (v1.Volume, bool) {
	if !r.EncryptionEnabled {
		return v1.Volume{}, false
	}
	return v1.Volume{
		Name: "encryption-key",
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				SecretName:  constants.ComponentEncryptionSecretName,
				DefaultMode: ptrInt32(0o444),
				Items:       []v1.KeyToPath{{Key: constants.ComponentEncryptionSecretKey, Path: "encryption-key"}},
			},
		},
	}, true
}

// getComponentEncryptionVolumeMount returns the encryption volume mount at /etc/glassflow/secrets (same as API).
func (r *PipelineReconciler) getComponentEncryptionVolumeMount() (v1.VolumeMount, bool) {
	if !r.EncryptionEnabled {
		return v1.VolumeMount{}, false
	}
	return v1.VolumeMount{
		Name:      "encryption-key",
		ReadOnly:  true,
		MountPath: "/etc/glassflow/secrets",
	}, true
}
