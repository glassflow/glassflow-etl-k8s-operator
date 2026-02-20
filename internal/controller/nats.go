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
	"strconv"
	"time"

	"github.com/go-logr/logr"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/errs"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/observability"
)

// useNStreamSinkPath returns true when the sink consumes directly from N ingestor-output streams (single topic, no join, no dedup).
// When dedup is enabled, the sink consumes from the dedup output stream (one stream), so we use the single-stream path.
func useNStreamSinkPath(p etlv1alpha1.Pipeline) bool {
	if p.Spec.Join.Enabled || len(p.Spec.Ingestor.Streams) != 1 {
		return false
	}
	s := &p.Spec.Ingestor.Streams[0]
	return s.Deduplication == nil || !s.Deduplication.Enabled
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

	// Get consumer names and stream names directly from spec
	leftConsumerName := p.Spec.Join.NATSLeftConsumerName
	rightConsumerName := p.Spec.Join.NATSRightConsumerName

	// Use dedup output stream if dedup enabled, otherwise ingestor output
	leftStreamName := getEffectiveOutputStream(p.Spec.Ingestor.Streams[0])
	rightStreamName := getEffectiveOutputStream(p.Spec.Ingestor.Streams[1])

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
// For single-topic no-join no-dedup or single-topic with dedup (N-stream path) there are N streams; each has a per-pod consumer. Otherwise one stream.
func (r *PipelineReconciler) checkSinkPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline) error {
	baseConsumerName := p.Spec.Sink.NATSConsumerName
	if p.Spec.Join.Enabled {
		sinkStreamName := p.Spec.Join.OutputStream
		return r.checkConsumerPendingMessages(ctx, sinkStreamName, baseConsumerName)
	}
	if useNStreamSinkPath(p) || useDedupNStreamPath(p) {
		// N streams, per-pod consumer name = baseConsumerName + "_" + podIndex
		N := getSinkReplicaCount(p)
		streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
		for n := 0; n < N; n++ {
			streamName := streamNamePrefix + "_" + strconv.Itoa(n)
			consumerName := baseConsumerName + "_" + strconv.Itoa(n)
			if err := r.checkConsumerPendingMessages(ctx, streamName, consumerName); err != nil {
				return err
			}
		}
		return nil
	}
	// Single stream (e.g. legacy single-topic with dedup: sink reads from dedup output stream)
	sinkStreamName := getEffectiveOutputStream(p.Spec.Ingestor.Streams[0])
	return r.checkConsumerPendingMessages(ctx, sinkStreamName, baseConsumerName)
}

// checkDedupPendingMessages checks if a specific dedup consumer has pending messages.
// When useDedupNStreamPath: dedup pod d reads from getDedupInputStreamPrefix_d with consumer base+"_"+d; check all D streams.
func (r *PipelineReconciler) checkDedupPendingMessages(ctx context.Context, p etlv1alpha1.Pipeline, streamIndex int) error {
	stream := p.Spec.Ingestor.Streams[streamIndex]
	if stream.Deduplication == nil || !stream.Deduplication.Enabled {
		return nil
	}

	baseConsumerName := stream.Deduplication.NATSConsumerName
	if useDedupNStreamPath(p) {
		D := getDedupReplicaCount(stream)
		streamNamePrefix := getDedupInputStreamPrefix(p.Spec.ID, stream.TopicName)
		for d := 0; d < D; d++ {
			streamName := streamNamePrefix + "_" + strconv.Itoa(d)
			consumerName := baseConsumerName + "_" + strconv.Itoa(d)
			if err := r.checkConsumerPendingMessages(ctx, streamName, consumerName); err != nil {
				return err
			}
		}
		return nil
	}
	inputStreamName := stream.OutputStream
	return r.checkConsumerPendingMessages(ctx, inputStreamName, baseConsumerName)
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

	// create source streams (single-topic no-join no-dedup: N streams with subject mapping; single-topic with dedup: N sink + D dedup streams; else: one stream per topic + optional dedup output)
	useNStreams := useNStreamSinkPath(p)
	useDedupStreams := useDedupNStreamPath(p)
	for _, s := range p.Spec.Ingestor.Streams {
		if useNStreams {
			subjectPrefix := getIngestorSubjectPrefix(p.Spec.ID, s.TopicName)
			streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
			M := s.Replicas
			if M <= 0 {
				M = 1
			}
			N := getSinkReplicaCount(p)
			for n := 0; n < N; n++ {
				streamName := streamNamePrefix + "_" + strconv.Itoa(n)
				subjects := getSubjectsForStreamIndex(subjectPrefix, M, N, n)
				if len(subjects) == 0 {
					continue
				}
				err := r.NATSClient.CreateOrUpdateStreamWithSubjects(ctx, streamName, subjects)
				if err != nil {
					return fmt.Errorf("create stream %s: %w", streamName, err)
				}
			}
		} else if useDedupStreams {
			// Dedup path: do not create s.OutputStream or s.Deduplication.OutputStream.
			// Create N sink streams (sink reads from dedup output).
			D := getDedupReplicaCount(s)
			N := getSinkReplicaCount(p)
			dedupOutputSubjectPrefix := getDedupOutputSubjectPrefix(p.Spec.ID, s.TopicName)
			streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
			for n := 0; n < N; n++ {
				streamName := streamNamePrefix + "_" + strconv.Itoa(n)
				subjects := getSubjectsForStreamIndex(dedupOutputSubjectPrefix, D, N, n)
				if len(subjects) == 0 {
					continue
				}
				err := r.NATSClient.CreateOrUpdateStreamWithSubjects(ctx, streamName, subjects)
				if err != nil {
					return fmt.Errorf("create stream %s: %w", streamName, err)
				}
			}
			// Create D dedup input streams (dedup reads from ingestor output).
			ingestorSubjectPrefix := getIngestorSubjectPrefix(p.Spec.ID, s.TopicName)
			dedupInputStreamPrefix := getDedupInputStreamPrefix(p.Spec.ID, s.TopicName)
			M := s.Replicas
			if M <= 0 {
				M = 1
			}
			for d := 0; d < D; d++ {
				streamName := dedupInputStreamPrefix + "_" + strconv.Itoa(d)
				subjects := getSubjectsForStreamIndex(ingestorSubjectPrefix, M, D, d)
				if len(subjects) == 0 {
					continue
				}
				err := r.NATSClient.CreateOrUpdateStreamWithSubjects(ctx, streamName, subjects)
				if err != nil {
					return fmt.Errorf("create stream %s: %w", streamName, err)
				}
			}
		} else {
			err := r.NATSClient.CreateOrUpdateStream(ctx, s.OutputStream, 0)
			if err != nil {
				return fmt.Errorf("create stream %s: %w", s.OutputStream, err)
			}
		}

		// Create dedup output stream if dedup service is enabled (legacy path: not useDedupNStreamPath)
		if !useDedupStreams && s.Deduplication != nil && s.Deduplication.Enabled && s.Deduplication.OutputStream != "" {
			err := r.NATSClient.CreateOrUpdateStream(ctx, s.Deduplication.OutputStream, 0)
			if err != nil {
				return fmt.Errorf("create dedup output stream %s: %w", s.Deduplication.OutputStream, err)
			}
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

			// Use dedup output stream as input to join if dedup enabled
			kvStreamName := stream.OutputStream
			if stream.Deduplication != nil && stream.Deduplication.Enabled && stream.Deduplication.OutputStream != "" {
				kvStreamName = stream.Deduplication.OutputStream
			}

			err := r.NATSClient.CreateOrUpdateJoinKeyValueStore(ctx, kvStreamName, ttl)
			if err != nil {
				return fmt.Errorf("create join KV store %s: %w", kvStreamName, err)
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

	// delete Ingestor Streams (single-topic no-join no-dedup: delete N sink-input streams; single-topic with dedup: delete N sink + D dedup input streams; else: delete per-topic output stream and optional dedup output)
	useNStreams := useNStreamSinkPath(p)
	useDedupStreams := useDedupNStreamPath(p)
	for _, stream := range p.Spec.Ingestor.Streams {
		if useNStreams {
			N := getSinkReplicaCount(p)
			streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
			for n := 0; n < N; n++ {
				streamName := streamNamePrefix + "_" + strconv.Itoa(n)
				log.Info("deleting NATS sink input stream", "stream", streamName)
				err := r.deleteNATSStream(ctx, log, streamName)
				if err != nil {
					r.recordMetricsIfEnabled(func(m *observability.Meter) {
						m.RecordNATSOperation(ctx, "delete_ingestor_stream", "failure", p.Spec.ID)
					})
					log.Error(err, "failed to cleanup NATS sink input stream", "stream", streamName)
					return fmt.Errorf("cleanup NATS sink input stream %s: %w", streamName, err)
				}
				r.recordMetricsIfEnabled(func(m *observability.Meter) {
					m.RecordNATSOperation(ctx, "delete_ingestor_stream", "success", p.Spec.ID)
				})
				log.Info("NATS sink input stream deleted successfully", "stream", streamName)
			}
			break // only one topic in single-topic path
		}
		if useDedupStreams {
			// Delete N sink streams and D dedup input streams; do not delete s.OutputStream or s.Deduplication.OutputStream (we never created them)
			N := getSinkReplicaCount(p)
			streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
			for n := 0; n < N; n++ {
				streamName := streamNamePrefix + "_" + strconv.Itoa(n)
				log.Info("deleting NATS sink input stream", "stream", streamName)
				err := r.deleteNATSStream(ctx, log, streamName)
				if err != nil {
					r.recordMetricsIfEnabled(func(m *observability.Meter) {
						m.RecordNATSOperation(ctx, "delete_ingestor_stream", "failure", p.Spec.ID)
					})
					log.Error(err, "failed to cleanup NATS sink input stream", "stream", streamName)
					return fmt.Errorf("cleanup NATS sink input stream %s: %w", streamName, err)
				}
				r.recordMetricsIfEnabled(func(m *observability.Meter) {
					m.RecordNATSOperation(ctx, "delete_ingestor_stream", "success", p.Spec.ID)
				})
				log.Info("NATS sink input stream deleted successfully", "stream", streamName)
			}
			D := getDedupReplicaCount(stream)
			dedupInputStreamPrefix := getDedupInputStreamPrefix(p.Spec.ID, stream.TopicName)
			for d := 0; d < D; d++ {
				streamName := dedupInputStreamPrefix + "_" + strconv.Itoa(d)
				log.Info("deleting NATS dedup input stream", "stream", streamName)
				err := r.deleteNATSStream(ctx, log, streamName)
				if err != nil {
					log.Error(err, "failed to cleanup dedup input stream", "stream", streamName)
					return fmt.Errorf("cleanup dedup input stream %s: %w", streamName, err)
				}
				log.Info("NATS dedup input stream deleted successfully", "stream", streamName)
			}
			break // only one topic in dedup N-stream path
		}
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

		// Delete dedup output stream if it exists (legacy path only; not set when useDedupNStreamPath)
		if stream.Deduplication != nil &&
			stream.Deduplication.Enabled &&
			stream.Deduplication.OutputStream != "" {
			log.Info("deleting NATS dedup output stream", "stream", stream.Deduplication.OutputStream)
			err := r.deleteNATSStream(ctx, log, stream.Deduplication.OutputStream)
			if err != nil {
				log.Error(err, "failed to cleanup dedup output stream", "stream", stream.Deduplication.OutputStream)
				return fmt.Errorf("cleanup dedup output stream: %w", err)
			}
			log.Info("NATS dedup output stream deleted successfully", "stream", stream.Deduplication.OutputStream)
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
