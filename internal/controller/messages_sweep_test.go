package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"
)

// TestBuildConsumerComponentMap_Joinless covers the simplest topology: ingestor → sink.
// The sole internal stream (ingestor output) is consumed by the sink.
func TestBuildConsumerComponentMap_Joinless(t *testing.T) {
	t.Parallel()

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
		},
	}

	got, err := buildConsumerComponentMap(pipeline)
	if err != nil {
		t.Fatalf("buildConsumerComponentMap() returned error: %v", err)
	}

	hash := generatePipelineHash(pipeline.Spec.ID)
	want := map[string]string{
		fmt.Sprintf("gfm-%s-ingestor-out_0", hash): "sink",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildConsumerComponentMap() = %#v, want %#v", got, want)
	}
}

// TestBuildConsumerComponentMap_JoinWithDedup covers a realistic topology where ingestor
// outputs feed left-side dedup, dedup output feeds join, and join output feeds sink. Each
// internal stream maps to its downstream component.
func TestBuildConsumerComponentMap_JoinWithDedup(t *testing.T) {
	t.Parallel()

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
			Join: etlv1alpha1.Join{Enabled: true},
			Sink: etlv1alpha1.Sink{Type: "clickhouse"},
		},
	}

	got, err := buildConsumerComponentMap(pipeline)
	if err != nil {
		t.Fatalf("buildConsumerComponentMap() returned error: %v", err)
	}

	hash := generatePipelineHash(pipeline.Spec.ID)
	wantSubset := map[string]string{
		// Ingestor on the right side outputs into a dedup input.
		fmt.Sprintf("gfm-%s-ingestor_right-out_0", hash): "dedup",
		// Dedup output feeds the join's right side.
		fmt.Sprintf("gfm-%s-dedup_right-out_0", hash): "join",
		// Ingestor on the left side feeds the join directly (no dedup configured for it).
		fmt.Sprintf("gfm-%s-ingestor_left-out_0", hash): "join",
		// Join output is consumed by the sink.
		fmt.Sprintf("gfm-%s-join-out_0", hash): "sink",
	}

	for stream, wantComponent := range wantSubset {
		if got[stream] != wantComponent {
			t.Errorf("stream %s: got component=%q, want %q (full map: %#v)",
				stream, got[stream], wantComponent, got)
		}
	}
}

// TestDLQMessage_RoundTrip verifies the operator's DLQ envelope serializes with the same
// JSON shape glassflow-api consumers expect, with new optional fields omitted when unset.
func TestDLQMessage_RoundTrip(t *testing.T) {
	t.Parallel()

	original := models.DLQMessage{
		Component:       "sink",
		Reason:          models.DLQReasonRetryExhaustedOnStop,
		OriginalMessage: `{"id":42}`,
	}
	encoded, err := original.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}

	// Unmarshal as a generic map so we can assert the wire-level shape rather than
	// trusting the struct definition to round-trip with itself.
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if wire["component"] != "sink" {
		t.Errorf("component = %v, want sink", wire["component"])
	}
	if wire["reason"] != "retry_exhausted_on_stop" {
		t.Errorf("reason = %v, want retry_exhausted_on_stop", wire["reason"])
	}
	if wire["original_message"] != `{"id":42}` {
		t.Errorf("original_message = %v, want {\"id\":42}", wire["original_message"])
	}
	// Empty optional fields must not appear in the JSON (omitempty).
	if _, present := wire["error"]; present {
		t.Errorf("expected %q to be omitted, got %v", "error", wire["error"])
	}
}

// TestDLQWireContractsStable guards the wire contracts shared with glassflow-api: the reason
// label and the per-stream DLQ subject suffix. Both must change in lockstep with
// glassflow-api/internal/models/dlq.go and internal/constants.go.
func TestDLQWireContractsStable(t *testing.T) {
	t.Parallel()

	if got := models.DLQReasonRetryExhaustedOnStop; got != "retry_exhausted_on_stop" {
		t.Errorf("DLQReasonRetryExhaustedOnStop = %q; coordinate any change with glassflow-api", got)
	}
	if got := models.DLQSubjectSuffix; got != "failed" {
		t.Errorf("DLQSubjectSuffix = %q; must match glassflow-api internal.DLQSubjectName", got)
	}
	if got := models.DLQSubjectFromStream("gfm-abc-DLQ"); got != "gfm-abc-DLQ.failed" {
		t.Errorf("DLQSubjectFromStream(...) = %q, want gfm-abc-DLQ.failed", got)
	}
}
