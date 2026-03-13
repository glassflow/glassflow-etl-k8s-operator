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
	"strconv"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/errs"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
)

func getJoinInputStreamName(p etlv1alpha1.Pipeline, stream etlv1alpha1.SourceStream) string {
	return getIngestorOutputSubjectPrefix(p.Spec.ID, stream.TopicName)
}

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

// checkJoinPendingMessages checks if join consumers have pending messages
func (r *PipelineReconciler) checkJoinPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline) error {
	if !p.Spec.Join.Enabled {
		return nil // No join, nothing to check
	}

	// Consumer names are generated from pipeline ID (aligned with glassflow-api).
	leftConsumerName := getNATSJoinLeftConsumerName(p.Spec.ID)
	rightConsumerName := getNATSJoinRightConsumerName(p.Spec.ID)

	// Use generated stream names resolved by operator naming rules.
	leftStreamName := getJoinInputStreamName(p, p.Spec.Ingestor.Streams[0])
	rightStreamName := getJoinInputStreamName(p, p.Spec.Ingestor.Streams[1])

	// Check left stream
	err := r.checkConsumerPendingMessages(ctx, leftStreamName, leftConsumerName)
	if err != nil {
		return err
	}

	// Check right stream
	err = r.checkConsumerPendingMessages(ctx, rightStreamName, rightConsumerName)
	if err != nil {
		return err
	}

	return nil
}

// checkSinkPendingMessages checks if sink consumer(s) have pending messages.
func (r *PipelineReconciler) checkSinkPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline) error {
	baseConsumerName := getNATSSinkConsumerName(p.Spec.ID)
	if p.Spec.Join.Enabled {
		sinkStreamName := getSinkInputStreamPrefix(p.Spec.ID) + "_0"
		return r.checkConsumerPendingMessages(ctx, sinkStreamName, baseConsumerName)
	}
	if useNStreamSinkPath(p) || useDedupNStreamPath(p) {
		// sinkReplicas streams; sink uses the same durable consumer name on each stream.
		sinkReplicas := getSinkReplicas(p)
		streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
		for n := 0; n < sinkReplicas; n++ {
			streamName := streamNamePrefix + "_" + strconv.Itoa(n)
			consumerName := baseConsumerName
			if err := r.checkConsumerPendingMessages(ctx, streamName, consumerName); err != nil {
				return err
			}
		}
		return nil
	}

	sinkStreamName := getSinkInputStreamPrefix(p.Spec.ID) + "_0"
	return r.checkConsumerPendingMessages(ctx, sinkStreamName, baseConsumerName)
}

// checkDedupPendingMessages checks if a specific dedup consumer has pending messages.
// When useDedupNStreamPath: dedup pod d reads from getDedupInputStreamPrefix_d with shared consumer base name;
// check all dedupReplicas streams.
func (r *PipelineReconciler) checkDedupPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline, streamIndex int) error {
	stream := p.Spec.Ingestor.Streams[streamIndex]
	if stream.Deduplication == nil || !stream.Deduplication.Enabled {
		return nil
	}

	baseConsumerName := getNATSDedupConsumerName(p.Spec.ID)
	if useDedupNStreamPath(p) {
		dedupReplicas := getDedupReplicas(p)
		streamNamePrefix := getDedupInputStreamPrefix(p.Spec.ID, stream.TopicName)
		for d := 0; d < dedupReplicas; d++ {
			streamName := streamNamePrefix + "_" + strconv.Itoa(d)
			consumerName := baseConsumerName
			if err := r.checkConsumerPendingMessages(ctx, streamName, consumerName); err != nil {
				return err
			}
		}
		return nil
	}
	inputStreamName := getIngestorOutputSubjectPrefix(p.Spec.ID, stream.TopicName)
	return r.checkConsumerPendingMessages(ctx, inputStreamName, baseConsumerName)
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
