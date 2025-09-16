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

package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PipelineSpec defines the desired state of Pipeline.
type PipelineSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// TODO: This CRD accepts "stream" names from API. It must be changed in
	// future as there is no reason for the app to decide "naming" for infra resources.
	// Leaks from non-optimal implementation of schema mapper on API side.

	// +kubebuilder:validation:MinLength=5
	ID       string  `json:"pipeline_id"`
	DLQ      string  `json:"dlq"`
	Ingestor Sources `json:"sources"`
	Join     Join    `json:"join"`
	Sink     Sink    `json:"sink"`
	Config   string  `json:"config"`
}

type Sources struct {
	// +kubebuilder:validation:Enum=kafka
	Type string `json:"type"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=2
	Streams []SourceStream `json:"topics"`
}

type SourceStream struct {
	// +kubebuilder:validation:MinLength=1
	TopicName    string        `json:"topic_name"`
	OutputStream string        `json:"stream"`
	DedupWindow  time.Duration `json:"dedup_window"`
	// +kubebuilder:validation:Minimum=1
	Replicas int `json:"replicas"`
}

type Join struct {
	Type         string `json:"type"`
	OutputStream string `json:"stream"`
	// +kubebuilder:validation:Minimum=1
	Replicas       int           `json:"replicas"`
	Enabled        bool          `json:"enabled"`
	LeftBufferTTL  time.Duration `json:"left_buffer_ttl,omitempty"`
	RightBufferTTL time.Duration `json:"right_buffer_ttl,omitempty"`
}

type Sink struct {
	// +kubebuilder:validation:Enum=clickhouse
	Type string `json:"type"`
	// +kubebuilder:validation:Minimum=1
	Replicas int `json:"replicas"`
}

// PipelineStatus defines the observed state of Pipeline.
type PipelineStatus string

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status

// Pipeline is the Schema for the pipelines API.
type Pipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineSpec   `json:"spec,omitempty"`
	Status PipelineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PipelineList contains a list of Pipeline.
type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Pipeline{}, &PipelineList{})
}
