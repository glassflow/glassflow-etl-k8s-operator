/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"fmt"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/pipelinegraph"
)

// ensureResolved returns the resolved allocation for a pipeline. Production
// path: read directly from spec.Resolved when the API has populated it
// (ETL-1064). Transitional fallback: when spec.Resolved is empty (legacy CRs
// pre-migration), compute locally via pipelinegraph and return the same
// shape. The fallback exists so the operator continues to function during
// the upgrade window before ETL-1067's migration rewrites every CR; once
// telemetry confirms no CRs lack Resolved, this fallback (and the
// pipelinegraph dependency) can be deleted.
func ensureResolved(spec etlv1alpha1.PipelineSpec) (etlv1alpha1.ResolvedPipeline, error) {
	if spec.Resolved != nil && len(spec.Resolved.Nodes) > 0 {
		return *spec.Resolved, nil
	}

	cfg, err := pipelinegraph.ConfigFromPipelineSpec(spec)
	if err != nil {
		return etlv1alpha1.ResolvedPipeline{}, fmt.Errorf("resolve pipeline graph: %w", err)
	}
	graph, err := pipelinegraph.New(cfg)
	if err != nil {
		return etlv1alpha1.ResolvedPipeline{}, fmt.Errorf("build pipeline graph: %w", err)
	}

	out := etlv1alpha1.ResolvedPipeline{Nodes: make([]etlv1alpha1.ResolvedNode, 0, len(cfg.Nodes))}
	for _, node := range cfg.Nodes {
		rn := etlv1alpha1.ResolvedNode{
			ID:       node.ID,
			Type:     string(node.Type),
			Replicas: int32(node.Replicas), //nolint:gosec // node.Replicas comes from CRD int32, range-safe
		}

		switch node.Type {
		case pipelinegraph.NodeTypeIngestor, pipelinegraph.NodeTypeOTLPSource:
			ob, err := graph.GetOutput(node.ID)
			if err != nil {
				return etlv1alpha1.ResolvedPipeline{}, fmt.Errorf("resolve output for %s: %w", node.ID, err)
			}
			rn.Output = convertOutput(ob)

		case pipelinegraph.NodeTypeDedup:
			ib, err := graph.GetInput(node.ID)
			if err != nil {
				return etlv1alpha1.ResolvedPipeline{}, fmt.Errorf("resolve input for %s: %w", node.ID, err)
			}
			rn.Input = convertInput(ib)
			ob, err := graph.GetOutput(node.ID)
			if err != nil {
				return etlv1alpha1.ResolvedPipeline{}, fmt.Errorf("resolve output for %s: %w", node.ID, err)
			}
			rn.Output = convertOutput(ob)

		case pipelinegraph.NodeTypeJoin:
			ji, err := graph.GetJoinInput(node.ID)
			if err != nil {
				return etlv1alpha1.ResolvedPipeline{}, fmt.Errorf("resolve join input for %s: %w", node.ID, err)
			}
			rn.JoinInput = &etlv1alpha1.NodeJoinInput{
				Left:  *convertInput(ji.Left),
				Right: *convertInput(ji.Right),
			}
			ob, err := graph.GetOutput(node.ID)
			if err != nil {
				return etlv1alpha1.ResolvedPipeline{}, fmt.Errorf("resolve output for %s: %w", node.ID, err)
			}
			rn.Output = convertOutput(ob)

		case pipelinegraph.NodeTypeSink:
			ib, err := graph.GetInput(node.ID)
			if err != nil {
				return etlv1alpha1.ResolvedPipeline{}, fmt.Errorf("resolve input for %s: %w", node.ID, err)
			}
			rn.Input = convertInput(ib)
		}

		out.Nodes = append(out.Nodes, rn)
	}

	return out, nil
}

func convertOutput(ob pipelinegraph.OutputBinding) *etlv1alpha1.NodeOutput {
	return &etlv1alpha1.NodeOutput{
		StreamPrefix:      ob.StreamPrefix,
		SubjectPrefix:     ob.SubjectPrefix,
		Streams:           convertStreams(ob.Streams),
		TotalSubjectCount: int32(ob.TotalSubjectCount), //nolint:gosec // pipeline subject counts fit comfortably in int32
	}
}

func convertInput(ib pipelinegraph.InputBinding) *etlv1alpha1.NodeInput {
	return &etlv1alpha1.NodeInput{
		StreamPrefix: ib.StreamPrefix,
		Streams:      convertStreams(ib.Streams),
	}
}

func convertStreams(in []pipelinegraph.StreamBinding) []etlv1alpha1.ResolvedStream {
	out := make([]etlv1alpha1.ResolvedStream, 0, len(in))
	for _, s := range in {
		out = append(out, etlv1alpha1.ResolvedStream{
			Name:     s.Name,
			Subjects: s.Subjects,
		})
	}
	return out
}
