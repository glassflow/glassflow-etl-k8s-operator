/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"
	natspkg "github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/pipelinegraph"
)

const (
	// sweepCtxCheckEvery defines how often (every N sequences) the inner sweep loop
	// checks ctx cancellation. Cheap enough to not matter, frequent enough to keep stop
	// responsive on a large backlog.
	sweepCtxCheckEvery = 100

	// outcome values for gfm_operator_sweeped_messages_total.
	streamOutcomeSwept          = "swept"
	streamOutcomeSweepFailed    = "sweep_failed"
	streamOutcomeStreamEmpty    = "stream_empty"
	streamOutcomeAlreadyDeleted = "already_deleted"
)

// sweepOrphanedMessages publishes any messages remaining in internal pipeline streams to the
// pipeline's DLQ before stream cleanup deletes them. Called from reconcileStop after components
// have been torn down — at that point any message still in an internal stream is by definition
// orphaned (no consumer alive to redeliver it).
//
// Per-message failures are logged + counted in the sweep_failed metric and the sweep continues.
// A whole-sweep failure (e.g. NATS unavailable) returns an error so the reconcile retries
// before cleanup deletes the streams.
func (r *PipelineReconciler) sweepMessagesToDLQ(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	pipelineID := p.Spec.ID
	sweepStart := time.Now()
	defer func() {
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordSweepDuration(ctx, pipelineID, time.Since(sweepStart))
		})
	}()

	plan, err := r.buildNATSResourcePlan(p)
	if err != nil {
		return fmt.Errorf("build NATS plan for orphan sweep: %w", err)
	}

	consumerByStream, err := buildConsumerComponentMap(p)
	if err != nil {
		return fmt.Errorf("map streams to consuming components for orphan sweep: %w", err)
	}

	dlqSubject := models.DLQSubjectFromStream(plan.DLQStream.Name)

	for _, stream := range plan.Streams {
		component, ok := consumerByStream[stream.Name]
		if !ok {
			return fmt.Errorf("orphan sweep: no consuming component resolved for stream %s", stream.Name)
		}
		if err := r.sweepStreamMessages(ctx, log, pipelineID, stream.Name, component, dlqSubject); err != nil {
			return fmt.Errorf("sweep stream %s: %w", stream.Name, err)
		}
	}

	return nil
}

func (r *PipelineReconciler) sweepStreamMessages(
	ctx context.Context,
	log logr.Logger,
	pipelineID, streamName, component, dlqSubject string,
) error {
	view, err := r.NATSClient.StreamView(ctx, streamName)
	if err != nil {
		return fmt.Errorf("open stream view: %w", err)
	}
	if view == nil {
		// Stream already gone — treat as empty for the metric.
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordSweepedMessages(ctx, pipelineID, streamOutcomeStreamEmpty)
		})
		return nil
	}

	seqRange, err := view.SeqRange(ctx)
	if err != nil {
		return fmt.Errorf("get seq range: %w", err)
	}
	if seqRange.Msgs == 0 {
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordSweepedMessages(ctx, pipelineID, streamOutcomeStreamEmpty)
		})
		return nil
	}

	log.Info("sweeping orphaned messages from internal stream",
		"pipeline_id", pipelineID,
		"stream", streamName,
		"first_seq", seqRange.FirstSeq,
		"last_seq", seqRange.LastSeq,
		"msgs", seqRange.Msgs,
	)

	var swept, failed, alreadyDeleted int
	for seq := seqRange.FirstSeq; seq <= seqRange.LastSeq; seq++ {
		if seq%sweepCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		switch r.sweepMessage(ctx, log, pipelineID, view, component, dlqSubject, seq) {
		case streamOutcomeSwept:
			swept++
		case streamOutcomeAlreadyDeleted:
			alreadyDeleted++
		default:
			failed++
		}
	}

	log.Info("orphan sweep complete for stream",
		"pipeline_id", pipelineID,
		"stream", streamName,
		"swept", swept,
		"already_deleted", alreadyDeleted,
		"failed", failed,
	)
	return nil
}

// sweepOneMessage handles a single sequence: read, publish to DLQ, delete from source.
// Returns the outcome label (also recorded to the metric inside this function). The caller
// uses the return value to bump local counters for logging.
func (r *PipelineReconciler) sweepMessage(
	ctx context.Context,
	log logr.Logger,
	pipelineID string,
	view *natspkg.StreamView,
	component, dlqSubject string,
	seq uint64,
) string {
	record := func(outcome string) string {
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordSweepedMessages(ctx, pipelineID, outcome)
		})
		return outcome
	}

	msg, err := view.GetMessage(ctx, seq)
	if err != nil {
		log.Error(err, "orphan sweep: get message failed", "stream", view.Name(), "seq", seq)
		return record(streamOutcomeSweepFailed)
	}
	if msg == nil {
		// Sequence already deleted (compaction or a prior partial sweep). Track separately
		// from "swept" so replay-after-crash behaviour is observable.
		log.V(1).Info("orphan sweep: sequence already deleted, skipping",
			"stream", view.Name(), "seq", seq)
		return record(streamOutcomeAlreadyDeleted)
	}

	envelope := models.DLQMessage{
		Component:       component,
		Reason:          models.DLQReasonRetryExhaustedOnStop,
		OriginalMessage: string(msg.Data),
	}
	payload, err := envelope.ToJSON()
	if err != nil {
		log.Error(err, "orphan sweep: marshal envelope failed", "stream", view.Name(), "seq", seq)
		return record(streamOutcomeSweepFailed)
	}

	if err := r.NATSClient.PublishMessage(ctx, dlqSubject, payload); err != nil {
		log.Error(err, "orphan sweep: publish to DLQ failed",
			"stream", view.Name(), "seq", seq, "dlq_subject", dlqSubject)
		return record(streamOutcomeSweepFailed)
	}

	if err := view.DeleteMessage(ctx, seq); err != nil {
		// DLQ entry is published; failing to delete here means a re-run would double-publish.
		// Tolerated for v1 because cleanup deletes the source stream right after this sweep.
		log.Error(err, "orphan sweep: delete from source stream failed",
			"stream", view.Name(), "seq", seq)
		return record(streamOutcomeSweepFailed)
	}

	return record(streamOutcomeSwept)
}

// buildConsumerComponentMap returns a stream-name → consuming-component-type lookup
// (e.g. "gfm-abc-ingestor-0-out" → "dedup"). Used to label orphan DLQ envelopes with the
// component that would have processed each message.
func buildConsumerComponentMap(p etlv1alpha1.Pipeline) (map[string]string, error) {
	graph, err := pipelinegraph.NewFromPipelineSpec(p.Spec)
	if err != nil {
		return nil, fmt.Errorf("build pipeline graph: %w", err)
	}

	out := make(map[string]string)

	sinkInput, err := graph.GetInput(pipelinegraph.SinkNodeID())
	if err != nil {
		return nil, fmt.Errorf("resolve sink input: %w", err)
	}
	for _, s := range sinkInput.Streams {
		out[s.Name] = "sink"
	}

	if p.Spec.Join.Enabled {
		joinInputs, err := graph.GetJoinInput(pipelinegraph.JoinNodeID())
		if err != nil {
			return nil, fmt.Errorf("resolve join input: %w", err)
		}
		for _, s := range joinInputs.Left.Streams {
			out[s.Name] = "join"
		}
		for _, s := range joinInputs.Right.Streams {
			out[s.Name] = "join"
		}
	}

	for _, i := range dedupStreamIndices(p) {
		dedupInput, err := graph.GetInput(pipelinegraph.DedupNodeID(p.Spec, i))
		if err != nil {
			return nil, fmt.Errorf("resolve dedup input %d: %w", i, err)
		}
		for _, s := range dedupInput.Streams {
			out[s.Name] = "dedup"
		}
	}

	return out, nil
}
