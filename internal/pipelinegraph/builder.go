package pipelinegraph

import (
	"fmt"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
)

const (
	joinNodeID     = "join"
	sinkNodeID     = "sink"
	ingestorNodeID = "ingestor"
	dedupNodeID    = "dedup"
)

// ConfigFromPipelineSpec converts a PipelineSpec into a graph topology config.
func ConfigFromPipelineSpec(spec etlv1alpha1.PipelineSpec) (Config, error) {
	if spec.Join.Enabled {
		return ConfigFromJoinPipelineSpec(spec)
	}

	return ConfigFromJoinlessPipelineSpec(spec)
}

// ConfigFromJoinlessPipelineSpec builds a graph config for a single-source pipeline:
func ConfigFromJoinlessPipelineSpec(spec etlv1alpha1.PipelineSpec) (Config, error) {
	config := Config{
		PipelineID: spec.ID,
		Nodes: []NodeConfig{
			{
				ID:       ingestorNodeID,
				Type:     NodeTypeIngestor,
				Replicas: getIngestorReplicas(spec, 0),
			},
		},
	}

	upstreamID := ingestorNodeID
	if transformsAreEnabled(spec) {
		config.Nodes = append(config.Nodes, NodeConfig{
			ID:       dedupNodeID,
			Type:     NodeTypeDedup,
			Replicas: getDedupReplicas(spec),
		})
		config.Edges = append(config.Edges, EdgeConfig{
			ID:              edgeID(ingestorNodeID, dedupNodeID),
			SourceID:        ingestorNodeID,
			TargetID:        dedupNodeID,
			TargetInputType: InputTypeIn,
		})
		upstreamID = dedupNodeID
	}

	config.Nodes = append(config.Nodes, NodeConfig{
		ID:       sinkNodeID,
		Type:     NodeTypeSink,
		Replicas: getSinkReplicas(spec),
	})
	config.Edges = append(config.Edges, EdgeConfig{
		ID:              edgeID(upstreamID, sinkNodeID),
		SourceID:        upstreamID,
		TargetID:        sinkNodeID,
		TargetInputType: InputTypeIn,
	})

	return config, nil
}

// ConfigFromJoinPipelineSpec builds a graph config for join pipeline:
func ConfigFromJoinPipelineSpec(spec etlv1alpha1.PipelineSpec) (Config, error) {
	config := Config{
		PipelineID: spec.ID,
		Nodes:      make([]NodeConfig, 0, len(spec.Ingestor.Streams)),
		Edges:      make([]EdgeConfig, 0, len(spec.Ingestor.Streams)),
	}

	upstreamNodeIDs := make([]string, 0, len(spec.Ingestor.Streams))
	for i, stream := range spec.Ingestor.Streams {
		side := joinSide(i)
		ingestorID := ingestorNodeID + "_" + side.String()
		config.Nodes = append(config.Nodes, NodeConfig{
			ID:       ingestorID,
			Type:     NodeTypeIngestor,
			Replicas: getIngestorReplicas(spec, i),
		})

		upstreamID := ingestorID
		if isStreamDedupEnabled(stream) {
			dedupID := dedupNodeID + "_" + side.String()
			config.Nodes = append(config.Nodes, NodeConfig{
				ID:       dedupID,
				Type:     NodeTypeDedup,
				Replicas: getDedupReplicas(spec),
			})
			config.Edges = append(config.Edges, EdgeConfig{
				ID:              edgeID(ingestorID, dedupID),
				SourceID:        ingestorID,
				TargetID:        dedupID,
				TargetInputType: InputTypeIn,
			})
			upstreamID = dedupID
		}

		upstreamNodeIDs = append(upstreamNodeIDs, upstreamID)
	}

	config.Nodes = append(config.Nodes, NodeConfig{
		ID:       joinNodeID,
		Type:     NodeTypeJoin,
		Replicas: getJoinReplicas(spec),
	})
	config.Edges = append(config.Edges, joinEdgeConfig(upstreamNodeIDs[0], 0))
	config.Edges = append(config.Edges, joinEdgeConfig(upstreamNodeIDs[1], 1))

	config.Nodes = append(config.Nodes, NodeConfig{
		ID:       sinkNodeID,
		Type:     NodeTypeSink,
		Replicas: getSinkReplicas(spec),
	})

	sinkSourceID := joinNodeID
	config.Edges = append(config.Edges, EdgeConfig{
		ID:              edgeID(sinkSourceID, sinkNodeID),
		SourceID:        sinkSourceID,
		TargetID:        sinkNodeID,
		TargetInputType: InputTypeIn,
	})

	return config, nil
}

// NewFromPipelineSpec builds and validates a graph directly from a PipelineSpec.
func NewFromPipelineSpec(spec etlv1alpha1.PipelineSpec) (*Graph, error) {
	config, err := ConfigFromPipelineSpec(spec)
	if err != nil {
		return nil, err
	}

	return New(config)
}

func joinEdgeConfig(sourceID string, streamIndex int) EdgeConfig {
	inputType := joinSide(streamIndex)
	return EdgeConfig{
		ID:              edgeID(sourceID, joinNodeID),
		SourceID:        sourceID,
		TargetID:        joinNodeID,
		TargetInputType: inputType,
	}
}

func edgeID(sourceID, targetID string) string {
	return fmt.Sprintf("%s_to_%s", sourceID, targetID)
}

func joinSide(streamIndex int) InputType {
	if streamIndex == 0 {
		return InputTypeLeft
	}
	return InputTypeRight
}

func getIngestorReplicas(spec etlv1alpha1.PipelineSpec, streamIndex int) int {
	replicas := constants.DefaultMinReplicas
	if spec.Resources == nil || spec.Resources.Ingestor == nil {
		return replicas
	}

	ingestorResources := spec.Resources.Ingestor
	var component *etlv1alpha1.ComponentResources
	if spec.Join.Enabled {
		if streamIndex == 0 {
			component = ingestorResources.Left
		} else {
			component = ingestorResources.Right
		}
	} else {
		component = ingestorResources.Base
	}

	if component != nil && component.Replicas != nil {
		replicas = int(*component.Replicas)
	}
	if replicas <= 0 {
		return constants.DefaultMinReplicas
	}

	return replicas
}

func getDedupReplicas(spec etlv1alpha1.PipelineSpec) int {
	replicas := constants.DefaultMinReplicas
	if spec.Resources != nil && spec.Resources.Dedup != nil && spec.Resources.Dedup.Replicas != nil {
		replicas = int(*spec.Resources.Dedup.Replicas)
	}
	if replicas <= 0 {
		return constants.DefaultMinReplicas
	}

	return replicas
}

func getJoinReplicas(spec etlv1alpha1.PipelineSpec) int {
	replicas := constants.DefaultMinReplicas
	if spec.Resources != nil && spec.Resources.Join != nil && spec.Resources.Join.Replicas != nil {
		replicas = int(*spec.Resources.Join.Replicas)
	}
	if replicas <= 0 {
		return constants.DefaultMinReplicas
	}

	return replicas
}

func getSinkReplicas(spec etlv1alpha1.PipelineSpec) int {
	replicas := constants.DefaultMinReplicas
	if spec.Resources != nil && spec.Resources.Sink != nil && spec.Resources.Sink.Replicas != nil {
		replicas = int(*spec.Resources.Sink.Replicas)
	}
	if replicas <= 0 {
		return constants.DefaultMinReplicas
	}

	return replicas
}

func isStreamDedupEnabled(stream etlv1alpha1.SourceStream) bool {
	return stream.Deduplication != nil && stream.Deduplication.Enabled
}

func transformsAreEnabled(spec etlv1alpha1.PipelineSpec) bool {
	return spec.Transform.IsStatelessTransformEnabled ||
		spec.Transform.IsFilterEnabled ||
		spec.Transform.IsDedupEnabled
}
