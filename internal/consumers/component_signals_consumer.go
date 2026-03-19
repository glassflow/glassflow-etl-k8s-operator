package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"
	errorsAPI "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/constants"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/errs"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/models"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/storage/postgres"
)

type ComponentSignalsConsumer struct {
	natsClient *nats.NATSClient
	log        logr.Logger
	client     client.Client
	storage    *postgres.PostgresStorage
	namespace  string
}

func NewComponentSignalsConsumer(
	natsClient *nats.NATSClient,
	log logr.Logger,
	client client.Client,
	storage *postgres.PostgresStorage,
	namespace string,
) *ComponentSignalsConsumer {
	return &ComponentSignalsConsumer{
		natsClient: natsClient,
		log:        log,
		client:     client,
		storage:    storage,
		namespace:  namespace,
	}
}

func (c *ComponentSignalsConsumer) Start(ctx context.Context) error {
	c.log.Info("starting component signals messages consumer")

	// Get the stream (created at operator startup)
	stream, err := c.natsClient.JetStream().Stream(ctx, constants.ComponentSignalsStream)
	if err != nil {
		return fmt.Errorf("get component signals messages stream: %w", err)
	}

	// Create or get the consumer
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:      constants.OperatorConsumer,
		Durable:   constants.OperatorConsumer,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("create component signals messages consumer: %w", err)
	}

	c.log.Info("component signals messages consumer started", "stream", constants.ComponentSignalsStream, "consumer", constants.OperatorConsumer)

	// Start consuming messages
	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		c.handleMessage(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	<-ctx.Done()
	consumeCtx.Stop()

	c.log.Info("component signals messages consumer stopped")
	return nil
}

func (c *ComponentSignalsConsumer) handleMessage(ctx context.Context, msg jetstream.Msg) {
	var message models.ComponentSignal

	err := json.Unmarshal(msg.Data(), &message)
	if err != nil {
		c.log.Error(err, "failed to unmarshal message")
		if nakErr := msg.Nak(); nakErr != nil {
			c.log.Error(nakErr, "failed to nak message")
		}
		return
	}

	c.log.Info(
		"stopping pipeline",
		"pipeline_id",
		message.PipelineID,
		"reason",
		message.Reason,
		"text",
		message.Text,
		"component",
		message.Component,
	)

	err = c.stopPipeline(ctx, message)
	if err != nil {
		if errors.Is(err, errs.ErrPipelineNotFound) {
			c.log.Info("pipeline not found, acking message", "pipeline_id", message.PipelineID)
			err = msg.Ack()
			if err != nil {
				c.log.Error(err, "failed to ack msg, trying to nak", "pipeline_id", message.PipelineID)
				if nakErr := msg.Nak(); nakErr != nil {
					c.log.Error(nakErr, "failed to nak message")
				}
			}
			return
		}
		c.log.Error(err, "failed to stop pipeline", "pipeline_id", message.PipelineID)
		if nakErr := msg.Nak(); nakErr != nil {
			c.log.Error(nakErr, "failed to nak message")
		}
		return
	}

	if err := msg.Ack(); err != nil {
		c.log.Error(err, "failed to ack message")
	}
}

func (c *ComponentSignalsConsumer) stopPipeline(ctx context.Context, message models.ComponentSignal) error {
	c.log.Info("stopping k8s pipeline", "pipeline_id", message.PipelineID)

	pipeline := &etlv1alpha1.Pipeline{}
	err := c.client.Get(ctx, types.NamespacedName{
		Namespace: c.namespace,
		Name:      message.PipelineID,
	}, pipeline)
	if err != nil {
		if errorsAPI.IsNotFound(err) {
			return errs.ErrPipelineNotFound
		}
		return fmt.Errorf("get pipeline CRD: %w", err)
	}

	pipelineStatus := getPipelineStatusFromK8SResource(pipeline)

	// Validate status transition using the centralized validation system
	err = models.ValidatePipelineOperation(pipelineStatus, models.PipelineStatusStopping)
	if err != nil {
		return fmt.Errorf("validation of pipeline operation: %w", err)
	}

	annotations := pipeline.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	// Add stop annotation
	annotations[constants.PipelineStopAnnotation] = "true"
	pipeline.SetAnnotations(annotations)

	// Update the resource with the stop annotation
	err = c.client.Update(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("update pipeline CRD with stop annotation: %w", err)
	}

	err = c.storage.UpdatePipelineStatus(ctx, message.PipelineID, models.PipelineStatusStopping, nil)
	if err != nil {
		return fmt.Errorf("update pipeline status: %w", err)
	}

	c.log.Info("requested stop of k8s pipeline", "pipeline_id", message.PipelineID)

	return nil
}

func getPipelineStatusFromK8SResource(pipeline *etlv1alpha1.Pipeline) models.PipelineStatus {
	statusStr := string(pipeline.Status)
	var currentStatus models.PipelineStatus
	if statusStr != "" {
		// Convert K8s status string to internal status
		switch statusStr {
		case "Created":
			currentStatus = models.PipelineStatusCreated
		case "Running":
			currentStatus = models.PipelineStatusRunning
		case "Resuming":
			currentStatus = models.PipelineStatusResuming
		case "Stopping":
			currentStatus = models.PipelineStatusStopping
		case "Stopped":
			currentStatus = models.PipelineStatusStopped
		case "Terminating":
			currentStatus = models.PipelineStatusTerminating
		case "Failed":
			currentStatus = models.PipelineStatusFailed
		default:
			currentStatus = models.PipelineStatusCreated
		}
	} else {
		currentStatus = models.PipelineStatusCreated
	}

	return currentStatus
}
