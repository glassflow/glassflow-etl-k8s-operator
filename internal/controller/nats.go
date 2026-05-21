/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"
	ctrl "sigs.k8s.io/controller-runtime"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/errs"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/pipelinegraph"
)

// checkConsumerPendingMessages checks if a specific consumer has pending messages
func (r *PipelineReconciler) checkConsumerPendingMessages(ctx context.Context, streamName, consumerName string) error {
	hasPending, pending, unack, err := r.NATSClient.CheckConsumerPendingMessages(ctx, streamName, consumerName)
	if err != nil {
		return fmt.Errorf("check consumer %s: %w", consumerName, err)
	}
	if hasPending {
		return errs.NewConsumerPendingMessagesError(consumerName, pending, unack)
	}

	return nil
}

func (r *PipelineReconciler) checkInputBindingPendingMessages(
	ctx context.Context,
	binding pipelinegraph.InputBinding,
	consumerName string,
) error {
	for _, stream := range binding.Streams {
		if err := r.checkConsumerPendingMessages(ctx, stream.Name, consumerName); err != nil {
			return err
		}
	}

	return nil
}

// countInputBindingPending returns the total pending + unacknowledged count across all streams in a binding.
func (r *PipelineReconciler) countInputBindingPending(ctx context.Context, binding pipelinegraph.InputBinding, consumerName string) (int, error) {
	total := 0
	for _, stream := range binding.Streams {
		_, pending, unack, err := r.NATSClient.CheckConsumerPendingMessages(ctx, stream.Name, consumerName)
		if err != nil {
			return 0, fmt.Errorf("count pending for consumer %s on stream %s: %w", consumerName, stream.Name, err)
		}
		total += pending + unack
	}
	return total, nil
}

// getTotalPendingCount returns the total pending + unacknowledged message count across all
// consumers for a pipeline (sink, join left/right, dedup per stream).
func (r *PipelineReconciler) getTotalPendingCount(ctx context.Context, p etlv1alpha1.Pipeline) (int, error) {
	graph, err := pipelinegraph.NewFromPipelineSpec(p.Spec)
	if err != nil {
		return 0, fmt.Errorf("build pipeline graph: %w", err)
	}

	total := 0

	// Sink
	sinkInput, err := graph.GetInput(pipelinegraph.SinkNodeID())
	if err != nil {
		return 0, fmt.Errorf("resolve sink input: %w", err)
	}
	n, err := r.countInputBindingPending(ctx, sinkInput, getNATSSinkConsumerName(p.Spec.ID))
	if err != nil {
		return 0, err
	}
	total += n

	// Join (if enabled)
	if p.Spec.Join.Enabled {
		inputs, err := graph.GetJoinInput(pipelinegraph.JoinNodeID())
		if err != nil {
			return 0, fmt.Errorf("resolve join input: %w", err)
		}
		leftN, err := r.countInputBindingPending(ctx, inputs.Left, getNATSJoinLeftConsumerName(p.Spec.ID))
		if err != nil {
			return 0, err
		}
		total += leftN
		rightN, err := r.countInputBindingPending(ctx, inputs.Right, getNATSJoinRightConsumerName(p.Spec.ID))
		if err != nil {
			return 0, err
		}
		total += rightN
	}

	// Dedup (if enabled)
	for _, i := range dedupStreamIndices(p) {
		dedupInput, err := graph.GetInput(pipelinegraph.DedupNodeID(p.Spec, i))
		if err != nil {
			return 0, fmt.Errorf("resolve dedup input for index %d: %w", i, err)
		}
		n, err := r.countInputBindingPending(ctx, dedupInput, getNATSDedupConsumerName(p.Spec.ID))
		if err != nil {
			return 0, err
		}
		total += n
	}

	return total, nil
}

// checkJoinPendingMessages checks if join consumers have pending messages
func (r *PipelineReconciler) checkJoinPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline) error {
	if !p.Spec.Join.Enabled {
		return nil // No join, nothing to check
	}

	graph, err := pipelinegraph.NewFromPipelineSpec(p.Spec)
	if err != nil {
		return fmt.Errorf("build pipeline graph for join checks: %w", err)
	}
	inputs, err := graph.GetJoinInput(pipelinegraph.JoinNodeID())
	if err != nil {
		return fmt.Errorf("resolve join input: %w", err)
	}

	// Consumer names are generated from pipeline ID (aligned with glassflow-api).
	leftConsumerName := getNATSJoinLeftConsumerName(p.Spec.ID)
	rightConsumerName := getNATSJoinRightConsumerName(p.Spec.ID)

	if err := r.checkInputBindingPendingMessages(ctx, inputs.Left, leftConsumerName); err != nil {
		return err
	}

	if err := r.checkInputBindingPendingMessages(ctx, inputs.Right, rightConsumerName); err != nil {
		return err
	}

	return nil
}

// checkSinkPendingMessages checks if sink consumer(s) have pending messages.
func (r *PipelineReconciler) checkSinkPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline) error {
	baseConsumerName := getNATSSinkConsumerName(p.Spec.ID)
	graph, err := pipelinegraph.NewFromPipelineSpec(p.Spec)
	if err != nil {
		return fmt.Errorf("build pipeline graph for sink checks: %w", err)
	}
	sinkInput, err := graph.GetInput(pipelinegraph.SinkNodeID())
	if err != nil {
		return fmt.Errorf("resolve sink input: %w", err)
	}

	return r.checkInputBindingPendingMessages(ctx, sinkInput, baseConsumerName)
}

// checkDedupPendingMessages checks if a specific dedup consumer has pending messages.
// Only called for indices returned by dedupStreamIndices, so dedup is guaranteed enabled.
func (r *PipelineReconciler) checkDedupPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline, streamIndex int) error {
	graph, err := pipelinegraph.NewFromPipelineSpec(p.Spec)
	if err != nil {
		return fmt.Errorf("build pipeline graph for dedup checks: %w", err)
	}
	dedupInput, err := graph.GetInput(pipelinegraph.DedupNodeID(p.Spec, streamIndex))
	if err != nil {
		return fmt.Errorf("resolve dedup input for index %d: %w", streamIndex, err)
	}

	return r.checkInputBindingPendingMessages(ctx, dedupInput, getNATSDedupConsumerName(p.Spec.ID))
}

// discoverStaleStreams finds NATS streams that belong to this pipeline but are not in the
// current OTLP source stream plan — orphaned after a downscale. Also returns the old OTLP
// source stream count (existing streams minus DLQ), needed for subject override.
// Only meaningful for OTLP pipelines: on a stopped pipeline only OTLP source streams + DLQ survive.
func (r *PipelineReconciler) discoverStaleStreams(ctx context.Context, p etlv1alpha1.Pipeline) (staleNames []string, oldSubjectCount int, err error) {
	plan, err := r.buildNATSResourcePlan(p)
	if err != nil {
		return nil, 0, fmt.Errorf("build NATS plan for stale detection: %w", err)
	}
	hash := generatePipelineHash(p.Spec.ID)
	existing, err := r.NATSClient.ListPipelineStreams(ctx, hash)
	if err != nil {
		return nil, 0, fmt.Errorf("list pipeline streams: %w", err)
	}
	stale := staleStreamNames(existing, plan)
	oldOTLPStreamCount := max(len(existing)-1, 0) // subtract DLQ
	return stale, oldOTLPStreamCount, nil
}

// overrideOTLPSourceSubjects redistributes oldSubjectCount subjects round-robin across the
// OTLP source streams in the plan so the OTLP receiver's cached SubjectCount is fully covered.
//
// Example: downscale 3→2, oldSubjectCount=3, new streams=2:
//
//	stream_0 gets [.0, .2]  stream_1 gets [.1]
//
// The extra binding (.2 on stream_0) becomes idle once the receiver refreshes its config.
func overrideOTLPSourceSubjects(plan *natsResourcePlan, oldSubjectCount int) {
	newStreamCount := len(plan.OTLPSourceStreams)
	if newStreamCount == 0 || oldSubjectCount <= newStreamCount {
		return
	}
	// Extract subject prefix from the first stream's first subject (e.g., "gfm-abc-otlp-out.0" → "gfm-abc-otlp-out")
	firstSubjects := plan.OTLPSourceStreams[0].Subjects
	if len(firstSubjects) == 0 {
		return
	}
	dotIdx := strings.LastIndex(firstSubjects[0], ".")
	if dotIdx < 0 {
		return
	}
	subjectPrefix := firstSubjects[0][:dotIdx]

	for i := range plan.OTLPSourceStreams {
		plan.OTLPSourceStreams[i].Subjects = nil
	}
	for s := range oldSubjectCount {
		streamIdx := s % newStreamCount
		plan.OTLPSourceStreams[streamIdx].Subjects = append(
			plan.OTLPSourceStreams[streamIdx].Subjects,
			fmt.Sprintf("%s.%d", subjectPrefix, s),
		)
	}
}

// unbindStaleStreamSubjects releases subject ownership from stale streams so that active
// streams can claim those subjects without NATS subject-overlap errors.
// Must be called BEFORE createNATSStreamsFromPlan.
func (r *PipelineReconciler) unbindStaleStreamSubjects(ctx context.Context, log logr.Logger, staleNames []string) error {
	for _, name := range staleNames {
		log.Info("unbinding subjects from stale stream", "stream", name)
		if err := r.NATSClient.UnbindStreamSubjects(ctx, name); err != nil {
			return fmt.Errorf("unbind subjects from %s: %w", name, err)
		}
	}
	return nil
}

// reassignStaleStreams moves messages from stale OTLP source streams into active streams via
// NATS Stream Sources (server-side move). Returns requeue=true when a transfer is still in
// progress so the reconciler retries after a short delay.
func (r *PipelineReconciler) reassignStaleStreams(
	ctx context.Context,
	log logr.Logger,
	p etlv1alpha1.Pipeline,
	staleNames []string,
	plan natsResourcePlan,
) (bool, error) {
	if len(staleNames) == 0 || len(plan.OTLPSourceStreams) == 0 {
		return false, nil
	}

	for i, staleName := range staleNames {
		destStream := plan.OTLPSourceStreams[i%len(plan.OTLPSourceStreams)]

		msgCount, err := r.NATSClient.GetStreamMessageCount(ctx, staleName)
		if err != nil {
			return false, fmt.Errorf("check stale stream %s: %w", staleName, err)
		}
		if msgCount == 0 {
			log.Info("stale stream is empty, deleting", "stream", staleName)
			if err := r.deleteNATSStream(ctx, log, staleName); err != nil {
				return false, err
			}
			continue
		}

		// Delete old durable consumer so it doesn't compete with Stream Source for messages.
		_ = r.NATSClient.DeleteConsumer(ctx, staleName, getNATSSinkConsumerName(p.Spec.ID))

		log.Info("adding stale stream as source for message transfer",
			"stale_stream", staleName, "dest_stream", destStream.Name, "pending_messages", msgCount)
		if err := r.NATSClient.AddStreamSource(ctx, destStream.Name, staleName); err != nil {
			return false, fmt.Errorf("add source %s to %s: %w", staleName, destStream.Name, err)
		}

		complete, err := r.NATSClient.IsStreamSourceComplete(ctx, destStream.Name, staleName)
		if err != nil {
			return false, fmt.Errorf("check source progress %s→%s: %w", staleName, destStream.Name, err)
		}
		if !complete {
			log.Info("stream source transfer in progress, requeuing",
				"stale_stream", staleName, "dest_stream", destStream.Name)
			return true, nil
		}

		log.Info("stream source transfer complete, cleaning up",
			"stale_stream", staleName, "dest_stream", destStream.Name)
		if err := r.NATSClient.RemoveStreamSource(ctx, destStream.Name, staleName); err != nil {
			return false, fmt.Errorf("remove source %s from %s: %w", staleName, destStream.Name, err)
		}
		if err := r.deleteNATSStream(ctx, log, staleName); err != nil {
			return false, err
		}
	}

	return false, nil
}

// setupEditNATSStreams handles the NATS portion of an edit operation: unbinds stale stream
// subjects (if any), builds and applies the resource plan (with subject override for downscale),
// creates/updates streams, and drains messages from stale streams via Stream Sources.
// Returns requeue=true when the drain is still in progress.
func (r *PipelineReconciler) setupEditNATSStreams(
	ctx context.Context,
	log logr.Logger,
	pipelineID string,
	p etlv1alpha1.Pipeline,
	staleNames []string,
	oldSubjectCount int,
) (ctrl.Result, error) {
	if len(staleNames) > 0 {
		if err := r.unbindStaleStreamSubjects(ctx, log, staleNames); err != nil {
			r.recordReconcileError(ctx, "edit", pipelineID, err)
			return ctrl.Result{}, fmt.Errorf("unbind stale stream subjects: %w", err)
		}
	}

	plan, err := r.buildNATSResourcePlan(p)
	if err != nil {
		r.recordReconcileError(ctx, "edit", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("build NATS plan: %w", err)
	}
	if oldSubjectCount > len(plan.OTLPSourceStreams) {
		overrideOTLPSourceSubjects(&plan, oldSubjectCount)
		log.Info("applied OTLP subject override for downscale",
			"old_subject_count", oldSubjectCount,
			"new_stream_count", len(plan.OTLPSourceStreams))
	}

	if err = r.createNATSStreamsFromPlan(ctx, p, plan); err != nil {
		r.recordReconcileError(ctx, "edit", pipelineID, err)
		return ctrl.Result{}, fmt.Errorf("setup streams: %w", err)
	}

	if len(staleNames) > 0 {
		requeue, err := r.reassignStaleStreams(ctx, log, p, staleNames, plan)
		if err != nil {
			r.recordReconcileError(ctx, "edit", pipelineID, err)
			return ctrl.Result{}, fmt.Errorf("reassign stale streams: %w", err)
		}
		if requeue {
			return ctrl.Result{Requeue: true, RequeueAfter: 2 * time.Second}, nil
		}
	}

	return ctrl.Result{}, nil
}

// createNATSStreams creates all NATS streams and KV stores for a pipeline.
func (r *PipelineReconciler) createNATSStreams(ctx context.Context, p etlv1alpha1.Pipeline) error {
	plan, err := r.buildNATSResourcePlan(p)
	if err != nil {
		return fmt.Errorf("build NATS resource plan: %w", err)
	}
	return r.createNATSStreamsFromPlan(ctx, p, plan)
}

// createNATSStreamsFromPlan creates all NATS streams and KV stores from a (possibly overridden) plan.
func (r *PipelineReconciler) createNATSStreamsFromPlan(ctx context.Context, p etlv1alpha1.Pipeline, plan natsResourcePlan) error {
	err := r.NATSClient.CreateOrUpdateStream(ctx, plan.DLQStream)
	if err != nil {
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "create_stream", "failure", p.Spec.ID)
		})
		return fmt.Errorf("create stream %s: %w", plan.DLQStream.Name, err)
	}
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordNATSOperation(ctx, "create_dlq_stream", "success", p.Spec.ID)
	})

	for _, stream := range plan.OTLPSourceStreams {
		if err = r.NATSClient.CreateOrUpdateStream(ctx, stream); err != nil {
			return fmt.Errorf("create OTLP source stream %s: %w", stream.Name, err)
		}
	}

	for _, stream := range plan.Streams {
		if err = r.NATSClient.CreateOrUpdateStream(ctx, stream); err != nil {
			return fmt.Errorf("create stream %s: %w", stream.Name, err)
		}
	}

	for _, kvStore := range plan.JoinKVStores {
		if err = r.NATSClient.CreateOrUpdateJoinKeyValueStore(ctx, kvStore.Name, kvStore.TTL); err != nil {
			return fmt.Errorf("create join KV store %s: %w", kvStore.Name, err)
		}
	}

	return nil
}

type natsCleanupOptions struct {
	deleteDLQ       bool
	keepOTLPStreams bool
}

// cleanupNATSPipelineResources cleans up all NATS resources for a pipeline, including DLQ.
func (r *PipelineReconciler) cleanupNATSPipelineResources(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	return r.cleanupNATSPipelineResourcesWithOptions(ctx, log, p, natsCleanupOptions{
		deleteDLQ:       true,
		keepOTLPStreams: false,
	})
}

// cleanupNATSPipelineResourcesKeepDLQ cleans up pipeline NATS resources while preserving DLQ
// and OTLP source streams (so the shared OTLP receiver can continue buffering events).
func (r *PipelineReconciler) cleanupNATSPipelineResourcesKeepDLQ(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	return r.cleanupNATSPipelineResourcesWithOptions(ctx, log, p, natsCleanupOptions{
		deleteDLQ:       false,
		keepOTLPStreams: true,
	})
}

// cleanupNATSPipelineResourcesKeepOTLP cleans up pipeline NATS resources while preserving only
// OTLP source streams (so the shared OTLP receiver can continue buffering events after terminate).
// DLQ and all other operation streams/KV stores are removed.
func (r *PipelineReconciler) cleanupNATSPipelineResourcesKeepOTLP(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	return r.cleanupNATSPipelineResourcesWithOptions(ctx, log, p, natsCleanupOptions{
		deleteDLQ:       true,
		keepOTLPStreams: true,
	})
}

func (r *PipelineReconciler) cleanupNATSPipelineResourcesWithOptions(
	ctx context.Context,
	log logr.Logger,
	p etlv1alpha1.Pipeline,
	opts natsCleanupOptions,
) error {
	log.Info("cleaning up NATS streams", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping stream cleanup")
		return fmt.Errorf("NATS client not available, skipping stream cleanup")
	}

	plan, err := r.buildNATSResourcePlan(p)
	if err != nil {
		return fmt.Errorf("build NATS resource plan: %w", err)
	}

	if opts.deleteDLQ {
		// delete the DLQ stream
		log.Info("deleting NATS DLQ stream", "stream", plan.DLQStream.Name)
		err = r.deleteNATSStream(ctx, log, plan.DLQStream.Name)
		if err != nil {
			log.Error(err, "failed to cleanup NATS DLQ stream", "pipeline", plan.DLQStream.Name)
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_dlq_stream", "failure", p.Spec.ID)
			})
			return fmt.Errorf("failed to cleanup NATS DLQ stream: %w", err)
		}
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "delete_dlq_stream", "success", p.Spec.ID)
		})
		log.Info("NATS DLQ stream deleted successfully", "stream", plan.DLQStream.Name)
	} else {
		log.Info("skipping NATS DLQ stream cleanup", "pipeline_id", p.Spec.ID)
	}

	if opts.keepOTLPStreams && len(plan.OTLPSourceStreams) > 0 {
		for _, stream := range plan.OTLPSourceStreams {
			log.Info("preserving OTLP source stream during stop", "stream", stream.Name, "pipeline_id", p.Spec.ID)
		}
	} else {
		for _, stream := range plan.OTLPSourceStreams {
			err = r.deleteNATSStream(ctx, log, stream.Name)
			if err != nil {
				return fmt.Errorf("delete OTLP source stream %s: %w", stream.Name, err)
			}
		}
	}

	for _, kvStore := range plan.JoinKVStores {
		err = r.deleteNATSKeyValueStore(ctx, log, kvStore.Name)
		if err != nil {
			log.Error(err, "failed to delete join key-value store", "pipeline", p.Name, "pipeline_id", p.Spec.ID, "bucket", kvStore.Name)
			return fmt.Errorf("failed to delete NATS KV Store: %w", err)
		}
	}

	for _, stream := range plan.Streams {
		err = r.deleteNATSStream(ctx, log, stream.Name)
		if err != nil {
			return fmt.Errorf("delete stream %s: %w", stream.Name, err)
		}
	}

	return nil
}

// deleteNATSStream deletes a NATS stream
func (r *PipelineReconciler) deleteNATSStream(ctx context.Context, log logr.Logger, streamName string) error {
	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping key value store cleanup")
		return fmt.Errorf("NATS client not available, skipping NATS cleanup")
	}

	err := r.NATSClient.JetStream().DeleteStream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			log.V(1).Info("NATS stream already deleted", "stream", streamName)
			return nil
		}
		log.Error(err, "failed to delete NATS output stream", "stream", streamName)
		return fmt.Errorf("failed to delete NATS output stream: %w", err)
	}

	return nil
}

func (r *PipelineReconciler) deleteNATSKeyValueStore(ctx context.Context, log logr.Logger, bucketName string) error {
	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping key value store cleanup")
		return fmt.Errorf("NATS client not available, skipping NATS cleanup")
	}

	err := r.NATSClient.JetStream().DeleteKeyValue(ctx, bucketName)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			log.V(1).Info("NATS key value store already deleted", "bucket", bucketName)
			return nil
		}
		log.Error(err, "failed to delete NATS key value store", "bucket", bucketName)
		return fmt.Errorf("failed to delete NATS key value store: %w", err)
	}

	return nil
}
