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
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/errs"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
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
		sinkStreamName := getJoinOutputStreamName(p.Spec.ID)
		return r.checkConsumerPendingMessages(ctx, sinkStreamName, baseConsumerName)
	}
	if useNStreamSinkPath(p) || useDedupNStreamPath(p) {
		// sinkReplicas streams, per-pod consumer name = baseConsumerName + "_" + podIndex
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

	// Single stream path.
	sinkStreamName := getJoinInputStreamName(p, p.Spec.Ingestor.Streams[0])
	return r.checkConsumerPendingMessages(ctx, sinkStreamName, baseConsumerName)
}

// checkDedupPendingMessages checks if a specific dedup consumer has pending messages.
// When useDedupNStreamPath: dedup pod d reads from getDedupInputStreamPrefix_d with consumer base+"_"+d;
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

// resolveStreamLimits returns effective stream limits for a pipeline, with CRD values
// taking precedence over operator-level defaults.
func (r *PipelineReconciler) resolveStreamLimits(p etlv1alpha1.Pipeline) (time.Duration, int64) {
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

	return maxAge, maxBytes
}

// createNATSStreams creates all NATS streams and KV stores for a pipeline
func (r *PipelineReconciler) createNATSStreams(ctx context.Context, p etlv1alpha1.Pipeline) error {
	maxAge, maxBytes := r.resolveStreamLimits(p)

	dlqStreamName := getDLQStreamName(p.Spec.ID)

	// create DLQ
	err := r.NATSClient.CreateOrUpdateStream(ctx, nats.StreamConfig{
		Name:     dlqStreamName,
		MaxAge:   maxAge,
		MaxBytes: maxBytes,
	})
	if err != nil {
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "create_stream", "failure", p.Spec.ID)
		})
		return fmt.Errorf("create stream %s: %w", dlqStreamName, err)
	}
	r.recordMetricsIfEnabled(func(m *observability.Meter) {
		m.RecordNATSOperation(ctx, "create_dlq_stream", "success", p.Spec.ID)
	})

	// create join stream
	if p.Spec.Join.Enabled {

		subjectPrefix := getJoinOutputSubjectPrefix(p.Spec.ID)
		// create sink input stream (join output stream)
		streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
		streamName := streamNamePrefix + "_0"
		subjects := getSubjectsForStreamIndex(subjectPrefix, 1, 1, 0)

		err = r.NATSClient.CreateOrUpdateStream(ctx, nats.StreamConfig{
			Name:     streamName,
			Subjects: subjects,
			MaxAge:   maxAge,
			MaxBytes: maxBytes,
		})
		if err != nil {
			return fmt.Errorf("create stream %s: %w", streamName, err)
		}

		// create join KV stores for each source stream
		// The join KV store names are the same as the stream names
		for streamIndex, stream := range p.Spec.Ingestor.Streams {
			// Use LeftBufferTTL for the first stream, RightBufferTTL for the second
			var ttl time.Duration
			if streamIndex == 0 {
				ttl = p.Spec.Join.LeftBufferTTL
			} else {
				ttl = p.Spec.Join.RightBufferTTL
			}

			kvStreamName := getJoinInputStreamName(p, stream)

			err = r.NATSClient.CreateOrUpdateJoinKeyValueStore(ctx, kvStreamName, ttl)
			if err != nil {
				return fmt.Errorf("create join KV store %s: %w", kvStreamName, err)
			}

			subjectPrefix = getIngestorOutputSubjectPrefix(p.Spec.ID, stream.TopicName)
			subjects = getSubjectsForStreamIndex(subjectPrefix, 1, 1, 0)

			err = r.NATSClient.CreateOrUpdateStream(ctx, nats.StreamConfig{
				Name:     kvStreamName,
				Subjects: subjects,
				MaxAge:   maxAge,
				MaxBytes: maxBytes,
			})
			if err != nil {
				return fmt.Errorf("create stream %s: %w", kvStreamName, err)
			}
		}
	} else {
		source := p.Spec.Ingestor.Streams[0]
		var subjectPrefix string
		var subjectCount int
		if isDedupEnabled(p) {
			// create dedup input / ingestor output streams
			ingestorSubjectPrefix := getIngestorOutputSubjectPrefix(p.Spec.ID, source.TopicName)
			dedupInputStreamPrefix := getDedupInputStreamPrefix(p.Spec.ID, source.TopicName)
			ingestorReplicas := getIngestorReplicas(p, 0)
			dedupReplicas := getDedupReplicas(p)
			for d := 0; d < dedupReplicas; d++ {
				streamName := dedupInputStreamPrefix + "_" + strconv.Itoa(d)
				subjects := getSubjectsForStreamIndex(ingestorSubjectPrefix, ingestorReplicas, dedupReplicas, d)
				if len(subjects) == 0 {
					continue
				}
				err := r.NATSClient.CreateOrUpdateStream(ctx, nats.StreamConfig{
					Name:     streamName,
					Subjects: subjects,
					MaxAge:   maxAge,
					MaxBytes: maxBytes,
				})
				if err != nil {
					return fmt.Errorf("create stream %s: %w", streamName, err)
				}
			}

			subjectPrefix = getDedupOutputSubjectPrefix(p.Spec.ID, source.TopicName)
			subjectCount = getDedupReplicas(p)
		} else {
			subjectPrefix = getIngestorOutputSubjectPrefix(p.Spec.ID, source.TopicName)
			subjectCount = getIngestorReplicas(p, 0)
		}

		// create sink input stream (ingestor/dedup output stream)
		sinkReplicas := getSinkReplicas(p)
		for n := 0; n < sinkReplicas; n++ {
			streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
			streamName := streamNamePrefix + "_" + strconv.Itoa(n)
			subjects := getSubjectsForStreamIndex(subjectPrefix, subjectCount, sinkReplicas, n)
			if len(subjects) == 0 {
				continue
			}
			err := r.NATSClient.CreateOrUpdateStream(ctx, nats.StreamConfig{
				Name:     streamName,
				Subjects: subjects,
				MaxAge:   maxAge,
				MaxBytes: maxBytes,
			})
			if err != nil {
				return fmt.Errorf("create stream %s: %w", streamName, err)
			}
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

	if deleteDLQ {
		// delete the DLQ stream
		dlqStreamName := getDLQStreamName(p.Spec.ID)
		log.Info("deleting NATS DLQ stream", "stream", dlqStreamName)
		err := r.deleteNATSStream(ctx, log, dlqStreamName)
		if err != nil {
			log.Error(err, "failed to cleanup NATS DLQ stream", "pipeline", dlqStreamName)
			r.recordMetricsIfEnabled(func(m *observability.Meter) {
				m.RecordNATSOperation(ctx, "delete_dlq_stream", "failure", p.Spec.ID)
			})
			return fmt.Errorf("failed to cleanup NATS DLQ stream: %w", err)
		}
		r.recordMetricsIfEnabled(func(m *observability.Meter) {
			m.RecordNATSOperation(ctx, "delete_dlq_stream", "success", p.Spec.ID)
		})
		log.Info("NATS DLQ stream deleted successfully", "stream", dlqStreamName)
	} else {
		log.Info("skipping NATS DLQ stream cleanup", "pipeline_id", p.Spec.ID)
	}

	// delete Join Streams and key value stores
	if p.Spec.Join.Enabled {
		streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
		streamName := streamNamePrefix + "_0"

		err := r.deleteNATSStream(ctx, log, streamName)
		if err != nil {
			return fmt.Errorf("delete stream %s: %w", streamName, err)
		}

		// delete join KV stores
		for _, stream := range p.Spec.Ingestor.Streams {
			kvStreamName := getJoinInputStreamName(p, stream)

			err := r.deleteNATSKeyValueStore(ctx, log, kvStreamName)
			if err != nil {
				log.Error(err, "failed to delete join key-value store", "pipeline", p.Name, "pipeline_id", p.Spec.ID, "stream", streamName)
				return fmt.Errorf("failed to delete NATS KV Store: %w", err)
			}

			err = r.deleteNATSStream(ctx, log, kvStreamName)
			if err != nil {
				return fmt.Errorf("delete stream %s: %w", kvStreamName, err)
			}
		}

	} else {
		source := p.Spec.Ingestor.Streams[0]
		if isDedupEnabled(p) {
			// create dedup input / ingestor output streams
			dedupInputStreamPrefix := getDedupInputStreamPrefix(p.Spec.ID, source.TopicName)
			dedupReplicas := getDedupReplicas(p)
			for d := 0; d < dedupReplicas; d++ {
				streamName := dedupInputStreamPrefix + "_" + strconv.Itoa(d)

				err := r.deleteNATSStream(ctx, log, streamName)
				if err != nil {
					return fmt.Errorf("delete stream %s: %w", streamName, err)
				}
			}
		}

		// create sink input stream (ingestor/dedup output stream)
		sinkReplicas := getSinkReplicas(p)
		for n := 0; n < sinkReplicas; n++ {
			streamNamePrefix := getSinkInputStreamPrefix(p.Spec.ID)
			streamName := streamNamePrefix + "_" + strconv.Itoa(n)
			err := r.deleteNATSStream(ctx, log, streamName)
			if err != nil {
				return fmt.Errorf("delete stream %s: %w", streamName, err)
			}
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
