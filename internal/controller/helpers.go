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

const (
	pipelineScopedResourcePrefix       = "gf-"
	maxK8sResourceNameLength           = 63
	maxStatefulSetNameLengthForRevHash = 52 // controller-revision-hash label value is "<sts-name>-<10 chars>"
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

// ingestorNATSSubjectCountEnvVars returns NATS_SUBJECT_COUNT=ingestorReplicas when dedup is enabled for the stream.
func ingestorNATSSubjectCountEnvVars(stream etlv1alpha1.SourceStream, ingestorReplicas int) []v1.EnvVar {
	if stream.Deduplication == nil || !stream.Deduplication.Enabled {
		return nil
	}
	if ingestorReplicas <= 0 {
		ingestorReplicas = constants.DefaultMinReplicas
	}
	return []v1.EnvVar{{Name: "NATS_SUBJECT_COUNT", Value: strconv.Itoa(ingestorReplicas)}}
}

// preparePipelineLabels returns labels for pipeline resources
func preparePipelineLabels(p etlv1alpha1.Pipeline) map[string]string {
	return map[string]string{"etl.glassflow.io/glassflow-etl-k8s-operator-id": p.Spec.ID}
}

// getTargetNamespace determines the target namespace for a pipeline based on configuration
func (r *PipelineReconciler) getTargetNamespace(p etlv1alpha1.Pipeline) string {
	if r.Config.Namespaces.Auto {
		return "pipeline-" + p.Spec.ID
	}
	return r.Config.Namespaces.Name
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
func (r *PipelineReconciler) getResourceName(p etlv1alpha1.Pipeline) string {
	return r.getPipelineScopedResourceName(p, constants.SecretSuffix, maxK8sResourceNameLength)
}

// getStatefulSetResourceName generates a StatefulSet resource name that keeps
// controller-revision-hash label values within Kubernetes' 63-char limit.
func (r *PipelineReconciler) getStatefulSetResourceName(p etlv1alpha1.Pipeline, baseName string) string {
	return r.getPipelineScopedResourceName(p, baseName, maxStatefulSetNameLengthForRevHash)
}

func (r *PipelineReconciler) getPipelineScopedResourceName(p etlv1alpha1.Pipeline, baseName string, maxLen int) string {
	if r.Config.Namespaces.Auto {
		return baseName
	}

	// Format: gf-<pipeline-id>-<baseName>
	reserved := len(pipelineScopedResourcePrefix) + 1 + len(baseName)
	availableForPipelineID := maxLen - reserved
	pipelineID := p.Spec.ID
	if availableForPipelineID < 1 {
		availableForPipelineID = 1
	}
	if len(pipelineID) > availableForPipelineID {
		pipelineID = pipelineID[:availableForPipelineID]
	}

	return fmt.Sprintf("%s%s-%s", pipelineScopedResourcePrefix, pipelineID, baseName)
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
	if !r.Config.UsageStats.Enabled {
		return []v1.EnvVar{}
	}

	envVars := []v1.EnvVar{
		{Name: "GLASSFLOW_USAGE_STATS_ENABLED", Value: "true"},
	}

	if r.Config.UsageStats.Endpoint != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_USAGE_STATS_ENDPOINT",
			Value: r.Config.UsageStats.Endpoint,
		})
	}

	if r.Config.UsageStats.Username != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_USAGE_STATS_USERNAME",
			Value: r.Config.UsageStats.Username,
		})
	}

	if r.Config.UsageStats.Password != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_USAGE_STATS_PASSWORD",
			Value: r.Config.UsageStats.Password,
		})
	}

	if r.Config.UsageStats.InstallationID != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_USAGE_STATS_INSTALLATION_ID",
			Value: r.Config.UsageStats.InstallationID,
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
	if r.Config.DatabaseURL == "" {
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
	if !r.Config.Encryption.Enabled {
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
	if !r.Config.Encryption.Enabled {
		return v1.VolumeMount{}, false
	}
	return v1.VolumeMount{
		Name:      "encryption-key",
		ReadOnly:  true,
		MountPath: "/etc/glassflow/secrets",
	}, true
}
