package controller

import (
	"fmt"
	"time"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/pipelinegraph"
)

type natsJoinKVStorePlan struct {
	Name string
	TTL  time.Duration
}

type natsResourcePlan struct {
	DLQStream    nats.StreamConfig
	Streams      []nats.StreamConfig
	JoinKVStores []natsJoinKVStorePlan
}

type natsNodePlan struct {
	Streams      []nats.StreamConfig
	JoinKVStores []natsJoinKVStorePlan
}

func (r *PipelineReconciler) buildNATSResourcePlan(p etlv1alpha1.Pipeline) (natsResourcePlan, error) {
	if !p.Spec.IsOTLPSource() {
		if len(p.Spec.Source.Streams) == 0 {
			return natsResourcePlan{}, fmt.Errorf("pipeline spec must contain at least one source stream")
		}
		if p.Spec.Join.Enabled && len(p.Spec.Source.Streams) < 2 {
			return natsResourcePlan{}, fmt.Errorf("join pipelines must contain at least two source streams")
		}
	}

	maxAge, maxBytes := r.NATSClient.DefaultStreamLimits()
	if p.Spec.Resources != nil && p.Spec.Resources.Nats != nil && p.Spec.Resources.Nats.Stream != nil {
		s := p.Spec.Resources.Nats.Stream
		if s.MaxAge.Duration != 0 {
			maxAge = s.MaxAge.Duration
		}
		if !s.MaxBytes.IsZero() {
			maxBytes = s.MaxBytes.Value()
		}
	}

	graphConfig, err := pipelinegraph.ConfigFromPipelineSpec(p.Spec)
	if err != nil {
		return natsResourcePlan{}, fmt.Errorf("build pipeline graph graphConfig: %w", err)
	}

	graph, err := pipelinegraph.New(graphConfig)
	if err != nil {
		return natsResourcePlan{}, fmt.Errorf("build pipeline graph: %w", err)
	}

	plan := natsResourcePlan{
		DLQStream: nats.StreamConfig{
			Name:     getDLQStreamName(p.Spec.ID),
			MaxAge:   maxAge,
			MaxBytes: maxBytes,
		},
		Streams: make([]nats.StreamConfig, 0, len(graphConfig.Nodes)),
	}

	for _, node := range graphConfig.Nodes {
		var nodePlan natsNodePlan

		switch node.Type {
		case pipelinegraph.NodeTypeJoin:
			nodePlan, err = buildJoinNodePlan(graph, node, p.Spec.Join, maxAge, maxBytes)
		case pipelinegraph.NodeTypeIngestor, pipelinegraph.NodeTypeOTLPSource, pipelinegraph.NodeTypeDedup:
			nodePlan, err = buildNodePlan(graph, node, maxAge, maxBytes)
		// sink doesn't have output
		default:
			continue
		}

		if err != nil {
			return natsResourcePlan{}, err
		}

		plan.Streams = append(plan.Streams, nodePlan.Streams...)
		plan.JoinKVStores = append(plan.JoinKVStores, nodePlan.JoinKVStores...)
	}

	return plan, nil
}

func buildNodePlan(
	graph *pipelinegraph.Graph,
	node pipelinegraph.NodeConfig,
	maxAge time.Duration,
	maxBytes int64,
) (natsNodePlan, error) {
	output, err := graph.GetOutput(node.ID)
	if err != nil {
		return natsNodePlan{}, fmt.Errorf("resolve output for node %s: %w", node.ID, err)
	}

	return natsNodePlan{
		Streams: buildOutputStreams(output, maxAge, maxBytes),
	}, nil
}

func buildJoinNodePlan(
	graph *pipelinegraph.Graph,
	node pipelinegraph.NodeConfig,
	join etlv1alpha1.Join,
	maxAge time.Duration,
	maxBytes int64,
) (natsNodePlan, error) {
	producerPlan, err := buildNodePlan(graph, node, maxAge, maxBytes)
	if err != nil {
		return natsNodePlan{}, err
	}

	inputs, err := graph.GetJoinInput(node.ID)
	if err != nil {
		return natsNodePlan{}, fmt.Errorf("resolve join inputs for node %s: %w", node.ID, err)
	}

	leftStore, err := inputBindingStoreName(inputs.Left)
	if err != nil {
		return natsNodePlan{}, fmt.Errorf("resolve left join input store name: %w", err)
	}

	rightStore, err := inputBindingStoreName(inputs.Right)
	if err != nil {
		return natsNodePlan{}, fmt.Errorf("resolve right join input store name: %w", err)
	}

	return natsNodePlan{
		Streams: producerPlan.Streams,
		JoinKVStores: []natsJoinKVStorePlan{
			{
				Name: leftStore,
				TTL:  join.LeftBufferTTL,
			},
			{
				Name: rightStore,
				TTL:  join.RightBufferTTL,
			},
		},
	}, nil
}

func buildOutputStreams(
	output pipelinegraph.OutputBinding,
	maxAge time.Duration,
	maxBytes int64,
) []nats.StreamConfig {
	streams := make([]nats.StreamConfig, 0, len(output.Streams))
	for _, stream := range output.Streams {
		streams = append(streams, nats.StreamConfig{
			Name:     stream.Name,
			Subjects: stream.Subjects,
			MaxAge:   maxAge,
			MaxBytes: maxBytes,
		})
	}
	return streams
}

// streamDiff computes which NATS streams need to be removed after a pipeline edit.
// It compares what currently exists in NATS ("have") against what the new plan requires
// ("want"), and returns the names that are in "have" but not in "want".
//
// "want" includes: all planned streams + DLQ stream + KV store backing streams (KV_ prefix).
// "have" is the list of stream names discovered in NATS for this pipeline.
func streamDiff(have []string, want natsResourcePlan) []string {
	wantSet := make(map[string]bool, len(want.Streams)+1+len(want.JoinKVStores))
	// DLQ is always kept — never stale
	wantSet[want.DLQStream.Name] = true
	// Pipeline data streams (one per component output per replica)
	for _, s := range want.Streams {
		wantSet[s.Name] = true
	}
	// NATS backs KV stores as streams with "KV_" prefix
	for _, kv := range want.JoinKVStores {
		wantSet["KV_"+kv.Name] = true
	}

	var toRemove []string
	for _, name := range have {
		if !wantSet[name] {
			toRemove = append(toRemove, name)
		}
	}
	return toRemove
}

func inputBindingStoreName(binding pipelinegraph.InputBinding) (string, error) {
	if len(binding.Streams) == 0 {
		return "", fmt.Errorf("input binding has no streams")
	}

	return binding.Streams[0].Name, nil
}
