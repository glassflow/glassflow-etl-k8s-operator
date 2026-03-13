package controller

import (
	"fmt"
	"strconv"
	"strings"
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

func (r *PipelineReconciler) buildNATSResourcePlan(p etlv1alpha1.Pipeline) (natsResourcePlan, error) {
	if len(p.Spec.Ingestor.Streams) == 0 {
		return natsResourcePlan{}, fmt.Errorf("pipeline spec must contain at least one source stream")
	}
	if p.Spec.Join.Enabled && len(p.Spec.Ingestor.Streams) < 2 {
		return natsResourcePlan{}, fmt.Errorf("join pipelines must contain at least two source streams")
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

	config, err := pipelinegraph.ConfigFromPipelineSpec(p.Spec)
	if err != nil {
		return natsResourcePlan{}, fmt.Errorf("build pipeline graph config: %w", err)
	}

	graph, err := pipelinegraph.New(config)
	if err != nil {
		return natsResourcePlan{}, fmt.Errorf("build pipeline graph: %w", err)
	}

	plan := natsResourcePlan{
		DLQStream: nats.StreamConfig{
			Name:     getDLQStreamName(p.Spec.ID),
			MaxAge:   maxAge,
			MaxBytes: maxBytes,
		},
		Streams: make([]nats.StreamConfig, 0, len(config.Nodes)),
	}

	for _, node := range config.Nodes {
		switch node.Type {
		// sink doesn't have output
		case pipelinegraph.NodeTypeIngestor, pipelinegraph.NodeTypeDedup, pipelinegraph.NodeTypeJoin:
		default:
			continue
		}

		output, err := graph.GetOutput(node.ID)
		if err != nil {
			return natsResourcePlan{}, fmt.Errorf("resolve output for node %s: %w", node.ID, err)
		}

		for _, stream := range output.Streams {
			plan.Streams = append(plan.Streams, nats.StreamConfig{
				Name:     stream.Name,
				Subjects: stream.Subjects,
				MaxAge:   maxAge,
				MaxBytes: maxBytes,
			})
		}

		if node.Type != pipelinegraph.NodeTypeJoin {
			continue
		}

		inputs, err := graph.GetJoinInput(node.ID)
		if err != nil {
			return natsResourcePlan{}, fmt.Errorf("resolve join inputs for node %s: %w", node.ID, err)
		}

		leftStore, err := inputBindingPrefix(inputs.Left)
		if err != nil {
			return natsResourcePlan{}, fmt.Errorf("resolve left join input prefix: %w", err)
		}
		plan.JoinKVStores = append(plan.JoinKVStores, natsJoinKVStorePlan{
			Name: leftStore,
			TTL:  p.Spec.Join.LeftBufferTTL,
		})

		rightStore, err := inputBindingPrefix(inputs.Right)
		if err != nil {
			return natsResourcePlan{}, fmt.Errorf("resolve right join input prefix: %w", err)
		}
		plan.JoinKVStores = append(plan.JoinKVStores, natsJoinKVStorePlan{
			Name: rightStore,
			TTL:  p.Spec.Join.RightBufferTTL,
		})
	}

	return plan, nil
}

func inputBindingPrefix(binding pipelinegraph.InputBinding) (string, error) {
	if len(binding.Streams) == 0 {
		return "", fmt.Errorf("input binding has no streams")
	}

	return streamNamePrefix(binding.Streams[0].Name)
}

func streamNamePrefix(streamName string) (string, error) {
	idx := strings.LastIndex(streamName, "_")
	if idx == -1 || idx == len(streamName)-1 {
		return "", fmt.Errorf("stream name %q is missing replica suffix", streamName)
	}

	if _, err := strconv.Atoi(streamName[idx+1:]); err != nil {
		return "", fmt.Errorf("stream name %q has invalid replica suffix: %w", streamName, err)
	}

	return streamName[:idx], nil
}
