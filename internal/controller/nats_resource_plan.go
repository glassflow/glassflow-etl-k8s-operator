package controller

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/pipelinegraph"
)

// streamLimits bundles the NATS stream capacity parameters that travel together
// through the plan-building helpers.
type streamLimits struct {
	MaxAge   time.Duration
	MaxBytes int64
	MaxMsgs  int64
	Discard  jetstream.DiscardPolicy
}

type natsJoinKVStorePlan struct {
	Name string
	TTL  time.Duration
}

type natsResourcePlan struct {
	DLQStream         nats.StreamConfig
	Streams           []nats.StreamConfig
	OTLPSourceStreams []nats.StreamConfig
	JoinKVStores      []natsJoinKVStorePlan
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

	defaultMaxAge, defaultMaxBytes, defaultMaxMsgs := r.NATSClient.DefaultStreamLimits()
	limits := streamLimits{MaxAge: defaultMaxAge, MaxBytes: defaultMaxBytes, MaxMsgs: defaultMaxMsgs}
	if p.Spec.Resources != nil && p.Spec.Resources.Nats != nil && p.Spec.Resources.Nats.Stream != nil {
		s := p.Spec.Resources.Nats.Stream
		if s.MaxAge.Duration != 0 {
			limits.MaxAge = s.MaxAge.Duration
		}
		if !s.MaxBytes.IsZero() {
			limits.MaxBytes = s.MaxBytes.Value()
		}
		if s.MaxMsgs != 0 {
			// 0 is the Go zero-value (field omitted in YAML) — treat as "use operator default".
			// NATS considers both 0 and -1 unlimited, so any non-zero value here is intentional.
			limits.MaxMsgs = s.MaxMsgs
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
			MaxAge:   limits.MaxAge,
			MaxBytes: limits.MaxBytes,
			MaxMsgs:  limits.MaxMsgs,
		},
		Streams: make([]nats.StreamConfig, 0, len(graphConfig.Nodes)),
	}

	for _, node := range graphConfig.Nodes {
		var nodePlan natsNodePlan

		switch node.Type {
		case pipelinegraph.NodeTypeJoin:
			nodePlan, err = buildJoinNodePlan(graph, node, p.Spec.Join, limits)
		case pipelinegraph.NodeTypeIngestor, pipelinegraph.NodeTypeDedup:
			nodePlan, err = buildNodePlan(graph, node, limits.withDiscard(jetstream.DiscardNew))
		case pipelinegraph.NodeTypeOTLPSource:
			nodePlan, err = buildNodePlan(graph, node, limits.withDiscard(jetstream.DiscardOld))
		// sink doesn't have output
		default:
			continue
		}

		if err != nil {
			return natsResourcePlan{}, err
		}

		if node.Type == pipelinegraph.NodeTypeOTLPSource {
			plan.OTLPSourceStreams = append(plan.OTLPSourceStreams, nodePlan.Streams...)
		} else {
			plan.Streams = append(plan.Streams, nodePlan.Streams...)
		}
		plan.JoinKVStores = append(plan.JoinKVStores, nodePlan.JoinKVStores...)
	}

	return plan, nil
}

func (l streamLimits) withDiscard(d jetstream.DiscardPolicy) streamLimits {
	l.Discard = d
	return l
}

func buildNodePlan(graph *pipelinegraph.Graph, node pipelinegraph.NodeConfig, lim streamLimits) (natsNodePlan, error) {
	output, err := graph.GetOutput(node.ID)
	if err != nil {
		return natsNodePlan{}, fmt.Errorf("resolve output for node %s: %w", node.ID, err)
	}

	return natsNodePlan{
		Streams: buildOutputStreams(output, lim),
	}, nil
}

func buildJoinNodePlan(
	graph *pipelinegraph.Graph,
	node pipelinegraph.NodeConfig,
	join etlv1alpha1.Join,
	lim streamLimits,
) (natsNodePlan, error) {
	producerPlan, err := buildNodePlan(graph, node, lim.withDiscard(jetstream.DiscardNew))
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

func buildOutputStreams(output pipelinegraph.OutputBinding, lim streamLimits) []nats.StreamConfig {
	streams := make([]nats.StreamConfig, 0, len(output.Streams))
	for _, stream := range output.Streams {
		streams = append(streams, nats.StreamConfig{
			Name:     stream.Name,
			Subjects: stream.Subjects,
			MaxAge:   lim.MaxAge,
			MaxBytes: lim.MaxBytes,
			MaxMsgs:  lim.MaxMsgs,
			Discard:  lim.Discard,
		})
	}
	return streams
}

// staleStreamNames returns stream names that exist in NATS but are not in the planned
// OTLP source streams or DLQ. On a stopped OTLP pipeline only OTLP source streams + DLQ
// survive, so only OTLP source streams can be stale after a downscale.
func staleStreamNames(existingNames []string, newPlan natsResourcePlan) []string {
	planned := make(map[string]bool, len(newPlan.OTLPSourceStreams)+1)
	planned[newPlan.DLQStream.Name] = true
	for _, s := range newPlan.OTLPSourceStreams {
		planned[s.Name] = true
	}
	var stale []string
	for _, name := range existingNames {
		if !planned[name] {
			stale = append(stale, name)
		}
	}
	return stale
}

func inputBindingStoreName(binding pipelinegraph.InputBinding) (string, error) {
	if len(binding.Streams) == 0 {
		return "", fmt.Errorf("input binding has no streams")
	}

	return binding.Streams[0].Name, nil
}
