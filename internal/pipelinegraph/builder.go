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

// IngestorNodeID returns the graph node ID for the ingestor serving streamIndex.
func IngestorNodeID(spec etlv1alpha1.PipelineSpec, streamIndex int) string {
	if spec.Join.Enabled {
		return ingestorNodeID + "_" + joinSide(streamIndex).String()
	}
	return ingestorNodeID
}

// DedupNodeID returns the graph node ID for the dedup serving streamIndex.
func DedupNodeID(spec etlv1alpha1.PipelineSpec, streamIndex int) string {
	if spec.Join.Enabled {
		return dedupNodeID + "_" + joinSide(streamIndex).String()
	}
	return dedupNodeID
}

// JoinNodeID returns the graph node ID for the join component.
func JoinNodeID() string {
	return joinNodeID
}

// SinkNodeID returns the graph node ID for the sink component.
func SinkNodeID() string {
	return sinkNodeID
}

// ConfigFromPipelineSpec converts a PipelineSpec into a graph topology config.
func ConfigFromPipelineSpec(spec etlv1alpha1.PipelineSpec) (Config, error) {
	if spec.Join.Enabled {
		return ConfigFromJoinPipelineSpec(spec)
	}

	return ConfigFromJoinlessPipelineSpec(spec)
}

// ConfigFromJoinlessPipelineSpec builds a graph config for a single-source pipeline:
func ConfigFromJoinlessPipelineSpec(spec etlv1alpha1.PipelineSpec) (Config, error) {
	ingestorID := IngestorNodeID(spec, 0)
	sinkID := SinkNodeID()

	config := Config{
		PipelineID: spec.ID,
		Nodes: []NodeConfig{
			{
				ID:       ingestorID,
				Type:     NodeTypeIngestor,
				Replicas: getIngestorReplicas(spec, 0),
			},
		},
	}

	upstreamID := ingestorID
	if transformsAreEnabled(spec) {
		dedupID := DedupNodeID(spec, 0)
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

	config.Nodes = append(config.Nodes, NodeConfig{
		ID:       sinkID,
		Type:     NodeTypeSink,
		Replicas: getSinkReplicas(spec),
	})
	config.Edges = append(config.Edges, EdgeConfig{
		ID:              edgeID(upstreamID, sinkID),
		SourceID:        upstreamID,
		TargetID:        sinkID,
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
		ingestorID := IngestorNodeID(spec, i)
		config.Nodes = append(config.Nodes, NodeConfig{
			ID:       ingestorID,
			Type:     NodeTypeIngestor,
			Replicas: getIngestorReplicas(spec, i),
		})

		upstreamID := ingestorID
		if isStreamDedupEnabled(stream) {
			dedupID := DedupNodeID(spec, i)
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
		ID:       JoinNodeID(),
		Type:     NodeTypeJoin,
		Replicas: getJoinReplicas(spec),
	})
	config.Edges = append(config.Edges, joinEdgeConfig(upstreamNodeIDs[0], 0))
	config.Edges = append(config.Edges, joinEdgeConfig(upstreamNodeIDs[1], 1))

	config.Nodes = append(config.Nodes, NodeConfig{
		ID:       SinkNodeID(),
		Type:     NodeTypeSink,
		Replicas: getSinkReplicas(spec),
	})

	sinkSourceID := JoinNodeID()
	config.Edges = append(config.Edges, EdgeConfig{
		ID:              edgeID(sinkSourceID, SinkNodeID()),
		SourceID:        sinkSourceID,
		TargetID:        SinkNodeID(),
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
		ID:              edgeID(sourceID, JoinNodeID()),
		SourceID:        sourceID,
		TargetID:        JoinNodeID(),
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
