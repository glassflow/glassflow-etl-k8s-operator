package controller

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
)

const testMinimalPipelineConfig = `{"pipeline_id":"%s","sources":[],"sink":{"type":"clickhouse"}}`

func minimalConfig(id string) string {
	return fmt.Sprintf(testMinimalPipelineConfig, id)
}

func simplePipeline(id string, ingestorReplicas, sinkReplicas int32) *etlv1alpha1.Pipeline {
	return &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: "default",
			Annotations: map[string]string{
				constants.PipelineCreateAnnotation: "create",
			},
		},
		Spec: etlv1alpha1.PipelineSpec{
			ID:     id,
			Config: minimalConfig(id),
			Source: etlv1alpha1.Sources{
				Type: "kafka",
				Streams: []etlv1alpha1.SourceStream{
					{TopicName: "test-topic-" + id},
				},
			},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			Resources: &etlv1alpha1.PipelineResources{
				Ingestor: &etlv1alpha1.IngestorResources{
					Base: &etlv1alpha1.ComponentResources{Replicas: &ingestorReplicas},
				},
				Sink: &etlv1alpha1.ComponentResources{Replicas: &sinkReplicas},
			},
		},
	}
}

func dedupPipeline(id string) *etlv1alpha1.Pipeline {
	one := int32(1)
	dedupWindow := 1 * time.Hour
	return &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: "default",
			Annotations: map[string]string{
				constants.PipelineCreateAnnotation: "create",
			},
		},
		Spec: etlv1alpha1.PipelineSpec{
			ID:     id,
			Config: minimalConfig(id),
			Source: etlv1alpha1.Sources{
				Type: "kafka",
				Streams: []etlv1alpha1.SourceStream{
					{
						TopicName:   "dedup-topic-" + id,
						DedupWindow: dedupWindow,
						Deduplication: &etlv1alpha1.Deduplication{
							Enabled:     true,
							StorageSize: "1Gi",
						},
					},
				},
			},
			Transform: etlv1alpha1.Transform{IsDedupEnabled: true},
			Sink:      etlv1alpha1.Sink{Type: "clickhouse"},
			Resources: &etlv1alpha1.PipelineResources{
				Ingestor: &etlv1alpha1.IngestorResources{
					Base: &etlv1alpha1.ComponentResources{Replicas: &one},
				},
				Sink: &etlv1alpha1.ComponentResources{Replicas: &one},
				Dedup: &etlv1alpha1.ComponentResources{
					Replicas: &one,
				},
			},
		},
	}
}

func joinPipeline(id string, leftReplicas, rightReplicas, joinReplicas, sinkReplicas int32) *etlv1alpha1.Pipeline {
	return &etlv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: "default",
			Annotations: map[string]string{
				constants.PipelineCreateAnnotation: "create",
			},
		},
		Spec: etlv1alpha1.PipelineSpec{
			ID:     id,
			Config: minimalConfig(id),
			Source: etlv1alpha1.Sources{
				Type: "kafka",
				Streams: []etlv1alpha1.SourceStream{
					{TopicName: "join-left-" + id},
					{TopicName: "join-right-" + id},
				},
			},
			Join: etlv1alpha1.Join{
				Type:         "temporal",
				Enabled:      true,
				LeftBufferTTL:  5 * time.Minute,
				RightBufferTTL: 5 * time.Minute,
			},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			Resources: &etlv1alpha1.PipelineResources{
				Ingestor: &etlv1alpha1.IngestorResources{
					Left:  &etlv1alpha1.ComponentResources{Replicas: &leftReplicas},
					Right: &etlv1alpha1.ComponentResources{Replicas: &rightReplicas},
				},
				Join: &etlv1alpha1.ComponentResources{Replicas: &joinReplicas},
				Sink: &etlv1alpha1.ComponentResources{Replicas: &sinkReplicas},
			},
		},
	}
}

func multiReplicaPipeline(id string, replicas int32) *etlv1alpha1.Pipeline {
	return simplePipeline(id, replicas, replicas)
}
