package controller

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
)

func TestBuildNATSResourcePlanJoinless(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipe-1",
			Ingestor: etlv1alpha1.Sources{
				Type: "kafka",
				Streams: []etlv1alpha1.SourceStream{
					{TopicName: "orders.events"},
				},
			},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			Resources: &etlv1alpha1.PipelineResources{
				Nats: &etlv1alpha1.NatsResources{
					Stream: &etlv1alpha1.NatsStreamResources{
						MaxAge:   metav1.Duration{Duration: 5 * time.Minute},
						MaxBytes: resource.MustParse("42"),
					},
				},
				Ingestor: &etlv1alpha1.IngestorResources{
					Base: &etlv1alpha1.ComponentResources{Replicas: ptrInt32(2)},
				},
				Sink: &etlv1alpha1.ComponentResources{Replicas: ptrInt32(2)},
			},
		},
	}

	plan, err := reconciler.buildNATSResourcePlan(pipeline)
	if err != nil {
		t.Fatalf("buildNATSResourcePlan() returned error: %v", err)
	}

	hash := generatePipelineHash(pipeline.Spec.ID)
	wantDLQ := nats.StreamConfig{
		Name:     getDLQStreamName(pipeline.Spec.ID),
		MaxAge:   5 * time.Minute,
		MaxBytes: 42,
	}
	if !reflect.DeepEqual(plan.DLQStream, wantDLQ) {
		t.Fatalf("plan.DLQStream = %#v, want %#v", plan.DLQStream, wantDLQ)
	}

	wantStreams := []nats.StreamConfig{
		{
			Name:     fmt.Sprintf("gfm-%s-ingestor-out_0", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-ingestor-out.0", hash)},
			MaxAge:   5 * time.Minute,
			MaxBytes: 42,
		},
		{
			Name:     fmt.Sprintf("gfm-%s-ingestor-out_1", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-ingestor-out.1", hash)},
			MaxAge:   5 * time.Minute,
			MaxBytes: 42,
		},
	}
	if !reflect.DeepEqual(plan.Streams, wantStreams) {
		t.Fatalf("plan.Streams = %#v, want %#v", plan.Streams, wantStreams)
	}
	if len(plan.JoinKVStores) != 0 {
		t.Fatalf("plan.JoinKVStores = %#v, want none", plan.JoinKVStores)
	}
}

func TestBuildNATSResourcePlanJoinWithKVStores(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipe-join",
			Ingestor: etlv1alpha1.Sources{
				Type: "kafka",
				Streams: []etlv1alpha1.SourceStream{
					{TopicName: "users"},
					{
						TopicName: "accounts",
						Deduplication: &etlv1alpha1.Deduplication{
							Enabled: true,
						},
					},
				},
			},
			Join: etlv1alpha1.Join{
				Enabled:        true,
				LeftBufferTTL:  time.Minute,
				RightBufferTTL: 2 * time.Minute,
			},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			Resources: &etlv1alpha1.PipelineResources{
				Ingestor: &etlv1alpha1.IngestorResources{
					Left:  &etlv1alpha1.ComponentResources{Replicas: ptrInt32(2)},
					Right: &etlv1alpha1.ComponentResources{Replicas: ptrInt32(3)},
				},
				Dedup: &etlv1alpha1.ComponentResources{Replicas: ptrInt32(2)},
				Join:  &etlv1alpha1.ComponentResources{Replicas: ptrInt32(1)},
				Sink:  &etlv1alpha1.ComponentResources{Replicas: ptrInt32(1)},
			},
		},
	}

	plan, err := reconciler.buildNATSResourcePlan(pipeline)
	if err != nil {
		t.Fatalf("buildNATSResourcePlan() returned error: %v", err)
	}

	hash := generatePipelineHash(pipeline.Spec.ID)
	wantStreams := []nats.StreamConfig{
		{
			Name: fmt.Sprintf("gfm-%s-ingestor_left-out_0", hash),
			Subjects: []string{
				fmt.Sprintf("gfm-%s-ingestor_left-out.0", hash),
				fmt.Sprintf("gfm-%s-ingestor_left-out.1", hash),
			},
		},
		{
			Name: fmt.Sprintf("gfm-%s-ingestor_right-out_0", hash),
			Subjects: []string{
				fmt.Sprintf("gfm-%s-ingestor_right-out.0", hash),
				fmt.Sprintf("gfm-%s-ingestor_right-out.2", hash),
			},
		},
		{
			Name:     fmt.Sprintf("gfm-%s-ingestor_right-out_1", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-ingestor_right-out.1", hash)},
		},
		{
			Name: fmt.Sprintf("gfm-%s-dedup_right-out_0", hash),
			Subjects: []string{
				fmt.Sprintf("gfm-%s-dedup_right-out.0", hash),
				fmt.Sprintf("gfm-%s-dedup_right-out.1", hash),
			},
		},
		{
			Name:     fmt.Sprintf("gfm-%s-join-out_0", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-join-out.0", hash)},
		},
	}
	if !reflect.DeepEqual(plan.Streams, wantStreams) {
		t.Fatalf("plan.Streams = %#v, want %#v", plan.Streams, wantStreams)
	}

	wantJoinKVStores := []natsJoinKVStorePlan{
		{
			Name: fmt.Sprintf("gfm-%s-ingestor_left-out_0", hash),
			TTL:  time.Minute,
		},
		{
			Name: fmt.Sprintf("gfm-%s-dedup_right-out_0", hash),
			TTL:  2 * time.Minute,
		},
	}
	if !reflect.DeepEqual(plan.JoinKVStores, wantJoinKVStores) {
		t.Fatalf("plan.JoinKVStores = %#v, want %#v", plan.JoinKVStores, wantJoinKVStores)
	}
}

func TestBuildNATSResourcePlanJoinWithLeftDedupKVStores(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipe-join-left-dedup",
			Ingestor: etlv1alpha1.Sources{
				Type: "kafka",
				Streams: []etlv1alpha1.SourceStream{
					{
						TopicName: "users",
						Deduplication: &etlv1alpha1.Deduplication{
							Enabled: true,
						},
					},
					{TopicName: "accounts"},
				},
			},
			Join: etlv1alpha1.Join{
				Enabled:        true,
				LeftBufferTTL:  time.Minute,
				RightBufferTTL: 2 * time.Minute,
			},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			Resources: &etlv1alpha1.PipelineResources{
				Ingestor: &etlv1alpha1.IngestorResources{
					Left:  &etlv1alpha1.ComponentResources{Replicas: ptrInt32(1)},
					Right: &etlv1alpha1.ComponentResources{Replicas: ptrInt32(1)},
				},
				Dedup: &etlv1alpha1.ComponentResources{Replicas: ptrInt32(1)},
				Join:  &etlv1alpha1.ComponentResources{Replicas: ptrInt32(1)},
				Sink:  &etlv1alpha1.ComponentResources{Replicas: ptrInt32(1)},
			},
		},
	}

	plan, err := reconciler.buildNATSResourcePlan(pipeline)
	if err != nil {
		t.Fatalf("buildNATSResourcePlan() returned error: %v", err)
	}

	hash := generatePipelineHash(pipeline.Spec.ID)
	wantJoinKVStores := []natsJoinKVStorePlan{
		{
			Name: fmt.Sprintf("gfm-%s-dedup_left-out_0", hash),
			TTL:  time.Minute,
		},
		{
			Name: fmt.Sprintf("gfm-%s-ingestor_right-out_0", hash),
			TTL:  2 * time.Minute,
		},
	}
	if !reflect.DeepEqual(plan.JoinKVStores, wantJoinKVStores) {
		t.Fatalf("plan.JoinKVStores = %#v, want %#v", plan.JoinKVStores, wantJoinKVStores)
	}
}
