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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PipelineSpec defines the desired state of Pipeline.
type PipelineSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// +kubebuilder:validation:MinLength=5
	ID string `json:"pipeline_id"`
	// TODO: Put all in secret config map and only pass configmap / secret name?
	Mapper   MapperConfig           `json:"mapper"`
	Ingestor IngestorOperatorConfig `json:"kafka_topics"`
	Join     JoinOperatorConfig     `json:"join"`
	Sink     SinkOperatorConfig     `json:"sink"`
}

type MapperConfig struct {
	// +kubebuilder:default="jsonToClickhouse"
	Type    string         `json:"type"`
	Streams []StreamSchema `json:"streams"`
	// +kubebuilder:validation:MinItems=1
	SinkMapping []SinkMappingConfig `json:"sink_mapping"`
}

type StreamSchema struct {
	Name   string             `json:"name"`
	Config StreamSchemaConfig `json:"schema_config"`
}

type StreamSchemaConfig struct {
	Fields []StreamDataField `json:"fields"`
	// +kubebuilder:validation:MinLength=1
	JoinKeyField string `json:"join_key_field,omitempty"`
	// +kubebuilder:validation:Enum=left;right
	JoinOrientation string        `json:"join_orientation,omitempty"`
	JoinWindow      time.Duration `json:"join_window,omitempty"`
}

type StreamDataField struct {
	// +kubebuilder:validation:MinLength=1
	FieldName string `json:"field_name"`
	// +kubebuilder:validation:MinLength=1
	FieldType string `json:"field_type"`
}

type SinkMappingConfig struct {
	// +kubebuilder:validation:MinLength=1
	ColumnName string `json:"column_name"`
	// +kubebuilder:validation:MinLength=1
	StreamName string `json:"stream_name"`
	// +kubebuilder:validation:MinLength=1
	FieldName string `json:"field_name"`
	// +kubebuilder:validation:MinLength=1
	ColumnType string `json:"column_type"`
}

type IngestorOperatorConfig struct {
	// +kubebuilder:validation:Enum=kafka
	Type                  string                      `json:"type"`
	KafkaConnectionParams KafkaConnectionParamsConfig `json:"kafka_connection_params"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=2
	KafkaTopics []KafkaTopicsConfig `json:"topics"`
}

type KafkaConnectionParamsConfig struct {
	// +kubebuilder:validation:MinItems=1
	Brokers       []string `json:"brokers"`
	SkipAuth      bool     `json:"skip_auth,omitempty"`
	SASLTLSEnable bool     `json:"sasl_tls_enable"`
	// +kubebuilder:validation:Enum=SASL_SSL;SSL;SASL_PLAINTEXT;PLAINTEXT
	SASLProtocol string `json:"protocol"`
	// +kubebuilder:validation:Enum=SCRAM-SHA-256;SCRAM-SHA-512;PLAIN
	SASLMechanism string `json:"mechanism"`
	SASLUsername  string `json:"username"`
	SASLPassword  string `json:"password"`
	TLSRoot       string `json:"root_ca"`
}

type KafkaTopicsConfig struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=earliest;latest
	ConsumerGroupInitialOffset string `json:"consumer_group_initial_offset"`

	Deduplication DeduplicationConfig `json:"deduplication"`
}

type DeduplicationConfig struct {
	Enabled bool `json:"enabled"`
	// +kubebuilder:validation:MinLength=1
	ID string `json:"id_field,omitempty"`
	// +kubebuilder:validation:MinLength=1
	Type string `json:"id_field_type,omitempty"`

	Window time.Duration `json:"time_window,omitempty"`
}

type JoinOperatorConfig struct {
	// +kubebuilder:validation:Enum=temporal
	Type    string `json:"type"`
	Enabled bool   `json:"enabled"`
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=2
	Sources []JoinSourceConfig `json:"sources,omitempty"`
}

type JoinSourceConfig struct {
	// +kubebuilder:validation:MinLength=1
	SourceID string `json:"source_id"`
	// +kubebuilder:validation:MinLength=1
	JoinKey string        `json:"join_key"`
	Window  time.Duration `json:"time_window"`
	// +kubebuilder:validation:Enum=left;right
	// +kubebuilder:validation:items:UniqeItems=true
	Orientation string `json:"orientation"`
}

type SinkOperatorConfig struct {
	// +kubebuilder:validation:Enum=clickhouse
	Type         string        `json:"type"`
	MaxBatchSize int           `json:"max_batch_size"`
	MaxDelayTime time.Duration `json:"max_delay_time"`

	// +kubebuilder:validation:MinLength=5
	Host string `json:"host"`
	// +kubebuilder:validation:MinLength=4
	Port string `json:"port"`
	// +kubebuilder:validation:MinLength=1
	DB       string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	// +kubebuilder:validation:MinLength=1
	Table                string `json:"table"`
	Secure               bool   `json:"secure"`
	SkipCertificateCheck bool   `json:"skip_certificate_check,omitempty"`
}

// PipelineStatus defines the observed state of Pipeline.
type PipelineStatus struct {
	IngestorOperatorStatus ComponentStatus `json:"ingestor_operator_status"`
	JoinOperatorStatus     ComponentStatus `json:"join_operator_status"`
	SinkOperatorStatus     ComponentStatus `json:"sink_operator_status"`
}

// +kubebuilder:validation:Enum=none;started;stopped
type ComponentStatus string

const (
	ComponentStatusNone    ComponentStatus = "none"
	ComponentStatusStarted ComponentStatus = "started"
	ComponentStatusStopped ComponentStatus = "stopped"
)

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
