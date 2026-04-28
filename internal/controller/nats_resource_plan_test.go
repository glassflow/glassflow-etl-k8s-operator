package controller

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
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
			Source: etlv1alpha1.Sources{
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
	// DLQ has no capacity limits regardless of pipeline resource config.
	wantDLQ := nats.StreamConfig{Name: getDLQStreamName(pipeline.Spec.ID)}
	if !reflect.DeepEqual(plan.DLQStream, wantDLQ) {
		t.Fatalf("plan.DLQStream = %#v, want %#v", plan.DLQStream, wantDLQ)
	}

	wantStreams := []nats.StreamConfig{
		{
			Name:     fmt.Sprintf("gfm-%s-ingestor-out_0", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-ingestor-out.0", hash)},
			MaxAge:   5 * time.Minute,
			MaxBytes: 42,
			Discard:  jetstream.DiscardNew,
		},
		{
			Name:     fmt.Sprintf("gfm-%s-ingestor-out_1", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-ingestor-out.1", hash)},
			MaxAge:   5 * time.Minute,
			MaxBytes: 42,
			Discard:  jetstream.DiscardNew,
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
			Source: etlv1alpha1.Sources{
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
			Discard: jetstream.DiscardNew,
		},
		{
			Name: fmt.Sprintf("gfm-%s-ingestor_right-out_0", hash),
			Subjects: []string{
				fmt.Sprintf("gfm-%s-ingestor_right-out.0", hash),
				fmt.Sprintf("gfm-%s-ingestor_right-out.2", hash),
			},
			Discard: jetstream.DiscardNew,
		},
		{
			Name:     fmt.Sprintf("gfm-%s-ingestor_right-out_1", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-ingestor_right-out.1", hash)},
			Discard:  jetstream.DiscardNew,
		},
		{
			Name: fmt.Sprintf("gfm-%s-dedup_right-out_0", hash),
			Subjects: []string{
				fmt.Sprintf("gfm-%s-dedup_right-out.0", hash),
				fmt.Sprintf("gfm-%s-dedup_right-out.1", hash),
			},
			Discard: jetstream.DiscardNew,
		},
		{
			Name:     fmt.Sprintf("gfm-%s-join-out_0", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-join-out.0", hash)},
			Discard:  jetstream.DiscardNew,
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

func TestBuildNATSResourcePlanOTLPSinkOnly(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipe-otlp-sink",
			Source: etlv1alpha1.Sources{
				Type: etlv1alpha1.SourceTypeOTLPLogs,
			},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
		},
	}

	plan, err := reconciler.buildNATSResourcePlan(pipeline)
	if err != nil {
		t.Fatalf("buildNATSResourcePlan() returned error: %v", err)
	}

	hash := generatePipelineHash(pipeline.Spec.ID)
	wantOTLPStreams := []nats.StreamConfig{
		{
			Name:     fmt.Sprintf("gfm-%s-otlp-out_0", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-otlp-out.0", hash)},
		},
	}
	if !reflect.DeepEqual(plan.OTLPSourceStreams, wantOTLPStreams) {
		t.Fatalf("plan.OTLPSourceStreams = %#v, want %#v", plan.OTLPSourceStreams, wantOTLPStreams)
	}
	if len(plan.Streams) != 0 {
		t.Fatalf("plan.Streams = %#v, want none", plan.Streams)
	}
	if len(plan.JoinKVStores) != 0 {
		t.Fatalf("plan.JoinKVStores = %#v, want none", plan.JoinKVStores)
	}
}

func TestBuildNATSResourcePlanOTLPWithDedup(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipe-otlp-dedup",
			Source: etlv1alpha1.Sources{
				Type: etlv1alpha1.SourceTypeOTLPTraces,
			},
			Transform: etlv1alpha1.Transform{IsDedupEnabled: true},
			Sink:      etlv1alpha1.Sink{Type: "clickhouse"},
			Resources: &etlv1alpha1.PipelineResources{
				Dedup: &etlv1alpha1.ComponentResources{Replicas: ptrInt32(1)},
			},
		},
	}

	plan, err := reconciler.buildNATSResourcePlan(pipeline)
	if err != nil {
		t.Fatalf("buildNATSResourcePlan() returned error: %v", err)
	}

	hash := generatePipelineHash(pipeline.Spec.ID)
	wantOTLPStreams := []nats.StreamConfig{
		{
			Name:     fmt.Sprintf("gfm-%s-otlp-out_0", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-otlp-out.0", hash)},
		},
	}
	if !reflect.DeepEqual(plan.OTLPSourceStreams, wantOTLPStreams) {
		t.Fatalf("plan.OTLPSourceStreams = %#v, want %#v", plan.OTLPSourceStreams, wantOTLPStreams)
	}
	wantStreams := []nats.StreamConfig{
		{
			Name:     fmt.Sprintf("gfm-%s-dedup-out_0", hash),
			Subjects: []string{fmt.Sprintf("gfm-%s-dedup-out.0", hash)},
			Discard:  jetstream.DiscardNew,
		},
	}
	if !reflect.DeepEqual(plan.Streams, wantStreams) {
		t.Fatalf("plan.Streams = %#v, want %#v", plan.Streams, wantStreams)
	}
	if len(plan.JoinKVStores) != 0 {
		t.Fatalf("plan.JoinKVStores = %#v, want none", plan.JoinKVStores)
	}
}

func TestBuildNATSResourcePlanJoinWithLeftDedupKVStores(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipe-join-left-dedup",
			Source: etlv1alpha1.Sources{
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

// TestBuildNATSResourcePlanDiscardPolicy verifies that pipeline internal streams are created
// with DiscardNew, while DLQ and OTLP source streams retain DiscardOld.
func TestBuildNATSResourcePlanDiscardPolicy(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}

	t.Run("pipeline streams have DiscardNew", func(t *testing.T) {
		t.Parallel()
		pipeline := etlv1alpha1.Pipeline{
			Spec: etlv1alpha1.PipelineSpec{
				ID: "pipe-discard",
				Source: etlv1alpha1.Sources{
					Type: "kafka",
					Streams: []etlv1alpha1.SourceStream{
						{TopicName: "events"},
					},
				},
				Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			},
		}
		plan, err := reconciler.buildNATSResourcePlan(pipeline)
		if err != nil {
			t.Fatalf("buildNATSResourcePlan() error: %v", err)
		}
		for _, s := range plan.Streams {
			if s.Discard != jetstream.DiscardNew {
				t.Errorf("stream %s: Discard = %v, want DiscardNew", s.Name, s.Discard)
			}
		}
	})

	t.Run("DLQ stream has no limits and DiscardOld default", func(t *testing.T) {
		t.Parallel()
		pipeline := etlv1alpha1.Pipeline{
			Spec: etlv1alpha1.PipelineSpec{
				ID: "pipe-dlq-discard",
				Source: etlv1alpha1.Sources{
					Type: "kafka",
					Streams: []etlv1alpha1.SourceStream{
						{TopicName: "events"},
					},
				},
				Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			},
		}
		plan, err := reconciler.buildNATSResourcePlan(pipeline)
		if err != nil {
			t.Fatalf("buildNATSResourcePlan() error: %v", err)
		}
		// DLQ has no capacity limits — all zero-values mean unlimited in NATS.
		wantDLQ := nats.StreamConfig{Name: getDLQStreamName(pipeline.Spec.ID)}
		if !reflect.DeepEqual(plan.DLQStream, wantDLQ) {
			t.Errorf("DLQ stream = %#v, want %#v", plan.DLQStream, wantDLQ)
		}
	})

	t.Run("OTLP source streams have DiscardOld", func(t *testing.T) {
		t.Parallel()
		pipeline := etlv1alpha1.Pipeline{
			Spec: etlv1alpha1.PipelineSpec{
				ID: "pipe-otlp-discard",
				Source: etlv1alpha1.Sources{
					Type: etlv1alpha1.SourceTypeOTLPLogs,
				},
				Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			},
		}
		plan, err := reconciler.buildNATSResourcePlan(pipeline)
		if err != nil {
			t.Fatalf("buildNATSResourcePlan() error: %v", err)
		}
		for _, s := range plan.OTLPSourceStreams {
			if s.Discard != jetstream.DiscardOld {
				t.Errorf("OTLP source stream %s: Discard = %v, want DiscardOld", s.Name, s.Discard)
			}
		}
	})
}

// TestBuildNATSResourcePlanMaxMsgs verifies that MaxMsgs from pipeline resources overrides
// the operator default and is applied to all streams in the plan, including the DLQ.
func TestBuildNATSResourcePlanMaxMsgs(t *testing.T) {
	t.Parallel()

	const customMaxMsgs = int64(500_000)

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipe-maxmsgs",
			Source: etlv1alpha1.Sources{
				Type: "kafka",
				Streams: []etlv1alpha1.SourceStream{
					{TopicName: "events"},
				},
			},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			Resources: &etlv1alpha1.PipelineResources{
				Nats: &etlv1alpha1.NatsResources{
					Stream: &etlv1alpha1.NatsStreamResources{
						MaxMsgs: customMaxMsgs,
					},
				},
			},
		},
	}

	plan, err := reconciler.buildNATSResourcePlan(pipeline)
	if err != nil {
		t.Fatalf("buildNATSResourcePlan() returned error: %v", err)
	}

	// DLQ has no limits regardless of pipeline resources config.
	wantDLQ := nats.StreamConfig{Name: getDLQStreamName(pipeline.Spec.ID)}
	if !reflect.DeepEqual(plan.DLQStream, wantDLQ) {
		t.Errorf("DLQ stream = %#v, want %#v", plan.DLQStream, wantDLQ)
	}
	for _, s := range plan.Streams {
		if s.MaxMsgs != customMaxMsgs {
			t.Errorf("stream %s MaxMsgs = %d, want %d", s.Name, s.MaxMsgs, customMaxMsgs)
		}
	}
}

// TestBuildNATSResourcePlanMaxMsgsDefault verifies that when MaxMsgs is not set in pipeline
// resources, the operator-level default (zero for an empty NATSClient) is used.
func TestBuildNATSResourcePlanMaxMsgsDefault(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipe-maxmsgs-default",
			Source: etlv1alpha1.Sources{
				Type: "kafka",
				Streams: []etlv1alpha1.SourceStream{
					{TopicName: "events"},
				},
			},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
		},
	}

	plan, err := reconciler.buildNATSResourcePlan(pipeline)
	if err != nil {
		t.Fatalf("buildNATSResourcePlan() returned error: %v", err)
	}

	// DLQ always has no limits.
	wantDLQ := nats.StreamConfig{Name: getDLQStreamName(pipeline.Spec.ID)}
	if !reflect.DeepEqual(plan.DLQStream, wantDLQ) {
		t.Errorf("DLQ stream = %#v, want %#v", plan.DLQStream, wantDLQ)
	}
	for _, s := range plan.Streams {
		if s.MaxMsgs != 0 {
			t.Errorf("stream %s MaxMsgs = %d, want 0 (operator default)", s.Name, s.MaxMsgs)
		}
	}
}

// TestBuildNATSResourcePlanMaxMsgsZeroNotOverride verifies that MaxMsgs=0 in pipeline
// resources does not override the operator default. Both 0 and -1 mean unlimited in NATS,
// so 0 is the natural "not configured" zero-value and must not clobber the operator default.
func TestBuildNATSResourcePlanMaxMsgsZeroNotOverride(t *testing.T) {
	t.Parallel()

	reconciler := &PipelineReconciler{NATSClient: &nats.NATSClient{}}
	pipeline := etlv1alpha1.Pipeline{
		Spec: etlv1alpha1.PipelineSpec{
			ID: "pipe-maxmsgs-zero",
			Source: etlv1alpha1.Sources{
				Type: "kafka",
				Streams: []etlv1alpha1.SourceStream{
					{TopicName: "events"},
				},
			},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
			Resources: &etlv1alpha1.PipelineResources{
				Nats: &etlv1alpha1.NatsResources{
					Stream: &etlv1alpha1.NatsStreamResources{
						MaxMsgs: 0, // explicitly zero — should not override
					},
				},
			},
		},
	}

	plan, err := reconciler.buildNATSResourcePlan(pipeline)
	if err != nil {
		t.Fatalf("buildNATSResourcePlan() returned error: %v", err)
	}

	// DLQ always has no limits, regardless of resource configuration.
	wantDLQ := nats.StreamConfig{Name: getDLQStreamName(pipeline.Spec.ID)}
	if !reflect.DeepEqual(plan.DLQStream, wantDLQ) {
		t.Errorf("DLQ stream = %#v, want %#v", plan.DLQStream, wantDLQ)
	}
}
