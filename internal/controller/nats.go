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

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"

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

// createNATSStreams creates all NATS streams and KV stores for a pipeline.
func (r *PipelineReconciler) createNATSStreams(ctx context.Context, p etlv1alpha1.Pipeline) error {
	plan, err := r.buildNATSResourcePlan(p)
	if err != nil {
		return fmt.Errorf("build NATS resource plan: %w", err)
	}

	// create DLQ
	err = r.NATSClient.CreateOrUpdateStream(ctx, plan.DLQStream)
	if err != nil {
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "create_stream", "failure", p.Spec.ID)
		})
		return fmt.Errorf("create stream %s: %w", plan.DLQStream.Name, err)
	}
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordNATSOperation(ctx, "create_dlq_stream", "success", p.Spec.ID)
	})

	for _, stream := range plan.Streams {
		err = r.NATSClient.CreateOrUpdateStream(ctx, stream)
		if err != nil {
			return fmt.Errorf("create stream %s: %w", stream.Name, err)
		}
	}

	for _, kvStore := range plan.JoinKVStores {
		err = r.NATSClient.CreateOrUpdateJoinKeyValueStore(ctx, kvStore.Name, kvStore.TTL)
		if err != nil {
			return fmt.Errorf("create join KV store %s: %w", kvStore.Name, err)
		}
	}

	return nil
}

// cleanupNATSPipelineResources cleans up all NATS resources for a pipeline, including DLQ.
func (r *PipelineReconciler) cleanupNATSPipelineResources(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	return r.cleanupNATSPipelineResourcesWithDLQOption(ctx, log, p, true)
}

// cleanupNATSPipelineResourcesKeepDLQ cleans up pipeline NATS resources while preserving DLQ.
func (r *PipelineReconciler) cleanupNATSPipelineResourcesKeepDLQ(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	return r.cleanupNATSPipelineResourcesWithDLQOption(ctx, log, p, false)
}

func (r *PipelineReconciler) cleanupNATSPipelineResourcesWithDLQOption(
	ctx context.Context,
	log logr.Logger,
	p etlv1alpha1.Pipeline,
	deleteDLQ bool,
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

	if deleteDLQ {
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
