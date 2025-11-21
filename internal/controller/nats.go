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
	"fmt"
	"time"

	"github.com/go-logr/logr"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
)

// checkConsumerPendingMessages checks if a specific consumer has pending messages
func (r *PipelineReconciler) checkConsumerPendingMessages(ctx context.Context, streamName, consumerName string) error {
	hasPending, pending, unack, err := r.NATSClient.CheckConsumerPendingMessages(ctx, streamName, consumerName)
	if err != nil {
		return fmt.Errorf("check consumer %s: %w", consumerName, err)
	}
	if hasPending {
		return fmt.Errorf("consumer %s has %d pending and %d unacknowledged messages", consumerName, pending, unack)
	}

	return nil
}

// checkJoinPendingMessages checks if join consumers have pending messages
func (r *PipelineReconciler) checkJoinPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline) error {
	if !p.Spec.Join.Enabled {
		return nil // No join, nothing to check
	}

	// Get consumer names and stream names directly from spec
	leftConsumerName := p.Spec.Join.NATSLeftConsumerName
	rightConsumerName := p.Spec.Join.NATSRightConsumerName
	leftStreamName := p.Spec.Ingestor.Streams[0].OutputStream
	rightStreamName := p.Spec.Ingestor.Streams[1].OutputStream

	// Check left stream
	err := r.checkConsumerPendingMessages(ctx, leftStreamName, leftConsumerName)
	if err != nil {
		return fmt.Errorf("left join consumer: %w", err)
	}

	// Check right stream
	err = r.checkConsumerPendingMessages(ctx, rightStreamName, rightConsumerName)
	if err != nil {
		return fmt.Errorf("right join consumer: %w", err)
	}

	return nil
}

// checkSinkPendingMessages checks if sink consumer has pending messages
func (r *PipelineReconciler) checkSinkPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline) error {
	// Get consumer name and stream name directly from spec
	sinkConsumerName := p.Spec.Sink.NATSConsumerName

	// Get stream name based on whether join is enabled
	var sinkStreamName string
	if p.Spec.Join.Enabled {
		sinkStreamName = p.Spec.Join.OutputStream
	} else {
		sinkStreamName = p.Spec.Ingestor.Streams[0].OutputStream
	}

	// Check sink stream
	err := r.checkConsumerPendingMessages(ctx, sinkStreamName, sinkConsumerName)
	if err != nil {
		return fmt.Errorf("sink consumer: %w", err)
	}

	return nil
}

// createNATSStreams creates all NATS streams and KV stores for a pipeline
func (r *PipelineReconciler) createNATSStreams(ctx context.Context, p etlv1alpha1.Pipeline) error {
	// create DLQ
	err := r.NATSClient.CreateOrUpdateStream(ctx, p.Spec.DLQ, 0)
	if err != nil {
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "create_stream", "failure", p.Spec.ID)
		})
		return fmt.Errorf("create stream %s: %w", p.Spec.DLQ, err)
	}
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordNATSOperation(ctx, "create_dlq_stream", "success", p.Spec.ID)
	})

	// create source streams
	for _, s := range p.Spec.Ingestor.Streams {
		err := r.NATSClient.CreateOrUpdateStream(ctx, s.OutputStream, s.DedupWindow)
		if err != nil {
			return fmt.Errorf("create stream %s: %w", s.OutputStream, err)
		}
	}

	// create join stream
	if p.Spec.Join.Enabled {
		err := r.NATSClient.CreateOrUpdateStream(ctx, p.Spec.Join.OutputStream, 0)
		if err != nil {
			return fmt.Errorf("create stream %s: %w", p.Spec.Join.OutputStream, err)
		}

		// create join KV stores for each source stream
		// The join KV store names are the same as the stream names
		for _, stream := range p.Spec.Ingestor.Streams {
			// Use LeftBufferTTL for the first stream, RightBufferTTL for the second
			var ttl time.Duration
			if stream.OutputStream == p.Spec.Ingestor.Streams[0].OutputStream {
				ttl = p.Spec.Join.LeftBufferTTL
			} else {
				ttl = p.Spec.Join.RightBufferTTL
			}

			err := r.NATSClient.CreateOrUpdateJoinKeyValueStore(ctx, stream.OutputStream, ttl)
			if err != nil {
				return fmt.Errorf("create join KV store %s: %w", stream.OutputStream, err)
			}
		}
	}

	return nil
}

// cleanupNATSPipelineResources cleans up all NATS resources for a pipeline
func (r *PipelineReconciler) cleanupNATSPipelineResources(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("cleaning up NATS streams", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping stream cleanup")
		return fmt.Errorf("NATS client not available, skipping stream cleanup")
	}

	// delete the DLQ stream
	if p.Spec.DLQ != "" {
		log.Info("deleting NATS DLQ stream", "stream", p.Spec.DLQ)
		err := r.deleteNATSStream(ctx, log, p.Spec.DLQ)
		if err != nil {
			log.Error(err, "failed to cleanup NATS DLQ stream", "pipeline", p.Spec.DLQ)
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_dlq_stream", "failure", p.Spec.ID)
			})
			return fmt.Errorf("failed to cleanup NATS DLQ stream: %w", err)
		}

		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "delete_dlq_stream", "success", p.Spec.ID)
		})
		log.Info("NATS DLQ stream deleted successfully", "stream", p.Spec.DLQ)
	}

	// delete Ingestor Streams
	for _, stream := range p.Spec.Ingestor.Streams {
		if stream.OutputStream != "" {
			log.Info("deleting NATS ingestor output stream", "stream", stream.OutputStream)
			err := r.deleteNATSStream(ctx, log, stream.OutputStream)
			if err != nil {
				r.recordMetricsIfEnabled(func(m *observability.Meter) {
					m.RecordNATSOperation(ctx, "delete_ingestor_stream", "failure", p.Spec.ID)
				})
				log.Error(err, "failed to cleanup NATS Ingestor Output Stream", "stream", stream)
				return fmt.Errorf("cleanup NATS Ingestor Output Stream: %w", err)
			}
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_ingestor_stream", "success", p.Spec.ID)
			})
			log.Info("NATS ingestor output stream deleted successfully", "stream", stream.OutputStream)
		}
	}

	// delete Join Streams and key value stores
	if p.Spec.Join.Enabled {
		if p.Spec.Join.OutputStream != "" {
			log.Info("deleting NATS join output stream", "stream", p.Spec.Join.OutputStream)
			err := r.deleteNATSStream(ctx, log, p.Spec.Join.OutputStream)
			if err != nil {
				r.recordMetricsIfEnabled(func(m *observability.Meter) {
					m.RecordNATSOperation(ctx, "delete_join_stream", "failure", p.Spec.ID)
				})
				log.Error(err, "failed to cleanup join output stream", "stream", p.Spec.Join.OutputStream)
				return fmt.Errorf("cleanup join output stream: %w", err)
			}
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_join_stream", "success", p.Spec.ID)
			})
			log.Info("NATS join output stream deleted successfully", "stream", p.Spec.Join.OutputStream)
		}

		err := r.cleanupNATSPipelineJoinKeyValueStore(ctx, log, p)
		if err != nil {
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_join_kv", "failure", p.Spec.ID)
			})
			log.Error(err, "failed to cleanup NATS join KV store", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
			return fmt.Errorf("failed cleanup NATS join KV store: %w", err)
		}
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "delete_join_kv", "success", p.Spec.ID)
		})
	}

	return nil
}

// cleanupNATSPipelineJoinKeyValueStore cleans up NATS join key value stores
func (r *PipelineReconciler) cleanupNATSPipelineJoinKeyValueStore(ctx context.Context, log logr.Logger, p etlv1alpha1.Pipeline) error {
	log.Info("cleaning up NATS join key value store", "pipeline", p.Name, "pipeline_id", p.Spec.ID)

	if r.NATSClient == nil {
		log.Info("NATS client not available, skipping key value store cleanup")
		return fmt.Errorf("NATS client not available, skipping NATS cleanup")
	}

	// since join key value names are same as stream names in CH-ETL
	for _, stream := range p.Spec.Ingestor.Streams {
		err := r.NATSClient.JetStream().DeleteKeyValue(ctx, stream.OutputStream)
		if err != nil {
			log.Error(err, "failed to delete join key-value store", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
			return fmt.Errorf("failed to delete NATS KV Store: %w", err)
		}
	}

	log.Info("NATS join key value store deleted successfully", "pipeline", p.Name, "pipeline_id", p.Spec.ID)
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
		log.Error(err, "failed to delete NATS output stream", "stream", streamName)
		return fmt.Errorf("failed to delete NATS output stream: %w", err)
	}

	return nil
}
