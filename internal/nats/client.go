package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const DefaultStreamMaxAge = 168 * time.Hour       // 7 days
const DefaultStreamMaxBytes = int64(107374182400) // 100GB

// PipelineStatus represents the overall status of a pipeline
type PipelineStatus string

// Pipeline status constants - must match the main API constants
const (
	PipelineStatusCreated     PipelineStatus = "Created"
	PipelineStatusRunning     PipelineStatus = "Running"
	PipelineStatusPausing     PipelineStatus = "Pausing"
	PipelineStatusPaused      PipelineStatus = "Paused"
	PipelineStatusResuming    PipelineStatus = "Resuming"
	PipelineStatusStopping    PipelineStatus = "Stopping"
	PipelineStatusStopped     PipelineStatus = "Stopped"
	PipelineStatusTerminating PipelineStatus = "Terminating"
	PipelineStatusTerminated  PipelineStatus = "Terminated"
	PipelineStatusFailed      PipelineStatus = "Failed"
)

// PipelineHealth represents the health status of a pipeline and its components
type PipelineHealth struct {
	PipelineID    string         `json:"pipeline_id"`
	PipelineName  string         `json:"pipeline_name"`
	OverallStatus PipelineStatus `json:"overall_status"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// PipelineConfig represents the pipeline configuration stored in NATS KV
type PipelineConfig struct {
	ID        string                 `json:"pipeline_id"`
	Name      string                 `json:"name"`
	Mapper    map[string]interface{} `json:"mapper"`
	Ingestor  map[string]interface{} `json:"ingestor"`
	Join      map[string]interface{} `json:"join"`
	Sink      map[string]interface{} `json:"sink"`
	CreatedAt time.Time              `json:"created_at"`
	Status    PipelineHealth         `json:"status,omitempty"`
}

type NATSClient struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	maxAge   time.Duration
	maxBytes int64
}

func New(url string) (*NATSClient, error) {
	return NewWithStreamConfig(url, DefaultStreamMaxAge, DefaultStreamMaxBytes)
}

func NewWithStreamConfig(url string, maxAge time.Duration, maxBytes int64) (*NATSClient, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to JetStream: %w", err)
	}

	return &NATSClient{
		nc:       nc,
		js:       js,
		maxAge:   maxAge,
		maxBytes: maxBytes,
	}, nil
}

func (n *NATSClient) CreateOrUpdateStream(ctx context.Context, name string, dedupWindow time.Duration) error {
	//nolint:exhaustruct // readability
	sc := jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{name + ".*"},
		Storage:  jetstream.FileStorage,

		Retention: jetstream.LimitsPolicy,
		MaxAge:    n.maxAge,
		MaxBytes:  n.maxBytes,
		Discard:   jetstream.DiscardOld,
	}

	if dedupWindow > 0 {
		sc.Duplicates = dedupWindow
	}

	_, err := n.JetStream().CreateOrUpdateStream(ctx, sc)
	if err != nil {
		return fmt.Errorf("cannot create nats stream: %w", err)
	}

	return nil
}

// CreateOrUpdateJoinKeyValueStore creates or updates a NATS KeyValue store
func (n *NATSClient) CreateOrUpdateJoinKeyValueStore(ctx context.Context, storeName string, ttl time.Duration) error {
	//nolint:exhaustruct // optional config
	cfg := jetstream.KeyValueConfig{
		Bucket:      storeName,
		TTL:         ttl,
		Description: "Store for Join component KV",
	}

	_, err := n.JetStream().CreateOrUpdateKeyValue(ctx, cfg)
	if err != nil {
		return fmt.Errorf("cannot create nats key value store %s: %w", storeName, err)
	}

	return nil
}

func (n *NATSClient) JetStream() jetstream.JetStream {
	return n.js
}

// CheckConsumerPendingMessages checks if a consumer has any pending or unacknowledged messages
// Returns: hasPending (bool), pendingCount (int), unacknowledgedCount (int), error
func (n *NATSClient) CheckConsumerPendingMessages(ctx context.Context, streamName, consumerName string) (bool, int, int, error) {
	// Add timeout for consumer info retrieval
	infoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	stream, err := n.js.Stream(infoCtx, streamName)
	if err != nil {
		return false, 0, 0, fmt.Errorf("get stream %s: %w", streamName, err)
	}

	consumer, err := stream.Consumer(infoCtx, consumerName)
	if err != nil {
		// Consumer doesn't exist - fail with error as requested
		return false, 0, 0, fmt.Errorf("consumer %s does not exist in stream %s: %w", consumerName, streamName, err)
	}

	info, err := consumer.Info(infoCtx)
	if err != nil {
		return false, 0, 0, fmt.Errorf("get consumer info for %s: %w", consumerName, err)
	}

	pending := int(info.NumPending)
	unacknowledged := info.NumAckPending
	totalPending := pending + unacknowledged
	hasPending := totalPending > 0

	return hasPending, pending, unacknowledged, nil
}

func (n *NATSClient) Close() error {
	n.nc.Close()
	return nil
}

// UpdatePipelineStatus updates the status of a pipeline in NATS KV store
func (n *NATSClient) UpdatePipelineStatus(ctx context.Context, pipelineID string, status PipelineStatus) error {
	kv, err := n.JetStream().KeyValue(ctx, "glassflow-pipelines")
	if err != nil {
		return fmt.Errorf("failed to get glassflow pipelines key-value store: %w", err)
	}

	// Get the current pipeline configuration
	entry, err := kv.Get(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("failed to get pipeline %s from KV store: %w", pipelineID, err)
	}

	// Unmarshal the current configuration
	var pipelineConfig PipelineConfig
	err = json.Unmarshal(entry.Value(), &pipelineConfig)
	if err != nil {
		return fmt.Errorf("failed to unmarshal pipeline configuration: %w", err)
	}

	// Update the status
	pipelineConfig.Status.OverallStatus = status
	pipelineConfig.Status.UpdatedAt = time.Now().UTC()

	// Marshal the updated configuration
	updatedConfig, err := json.Marshal(pipelineConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal updated pipeline configuration: %w", err)
	}

	// Update the KV store
	_, err = kv.Update(ctx, pipelineID, updatedConfig, entry.Revision())
	if err != nil {
		return fmt.Errorf("failed to update pipeline status in KV store: %w", err)
	}

	return nil
}

// GetPipelineConfig retrieves a pipeline configuration from NATS KV store
func (n *NATSClient) GetPipelineConfig(ctx context.Context, pipelineID string) (*PipelineConfig, error) {
	kv, err := n.JetStream().KeyValue(ctx, "glassflow-pipelines")
	if err != nil {
		return nil, fmt.Errorf("failed to get glassflow pipelines key-value store: %w", err)
	}

	entry, err := kv.Get(ctx, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pipeline %s from KV store: %w", pipelineID, err)
	}

	var pipelineConfig PipelineConfig
	err = json.Unmarshal(entry.Value(), &pipelineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal pipeline configuration: %w", err)
	}

	return &pipelineConfig, nil
}
