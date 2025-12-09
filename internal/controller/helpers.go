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
	"strings"

	v1 "k8s.io/api/core/v1"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
)

// getKafkaIngestorLabels returns labels for Kafka ingestor components
func (r *PipelineReconciler) getKafkaIngestorLabels(topic string) map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": "ingestor",
		"etl.glassflow.io/topic":     topic,
	}

	return labels
}

// getJoinLabels returns labels for join components
func (r *PipelineReconciler) getJoinLabels() map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": "join",
	}

	return labels
}

// getSinkLabels returns labels for sink components
func (r *PipelineReconciler) getSinkLabels() map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": "sink",
	}

	return labels
}

// getDedupLabels returns labels for dedup components
func (r *PipelineReconciler) getDedupLabels(topic string) map[string]string {
	labels := map[string]string{
		"etl.glassflow.io/component": "dedup",
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

// getTrackingEnvVars returns environment variables for tracking configuration
// Values are passed directly (not from secrets)
func (r *PipelineReconciler) getTrackingEnvVars() []v1.EnvVar {
	if !r.TrackingEnabled {
		return []v1.EnvVar{}
	}

	envVars := []v1.EnvVar{
		{Name: "GLASSFLOW_TRACKING_ENABLED", Value: "true"},
	}

	if r.TrackingEndpoint != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_TRACKING_ENDPOINT",
			Value: r.TrackingEndpoint,
		})
	}

	if r.TrackingUsername != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_TRACKING_USERNAME",
			Value: r.TrackingUsername,
		})
	}

	if r.TrackingPassword != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_TRACKING_PASSWORD",
			Value: r.TrackingPassword,
		})
	}

	if r.TrackingInstallationID != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  "GLASSFLOW_TRACKING_INSTALLATION_ID",
			Value: r.TrackingInstallationID,
		})
	}

	return envVars
}
