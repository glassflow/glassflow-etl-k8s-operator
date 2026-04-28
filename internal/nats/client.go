package nats

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const JSErrCodeStreamRetentionPolicyChange jetstream.ErrorCode = 10052

const DefaultStreamMaxAge = 168 * time.Hour       // 7 days
const DefaultStreamMaxBytes = int64(107374182400) // 100GB

// ParseRetentionPolicy parses a string retention policy name to jetstream.RetentionPolicy
// Valid values: "WorkQueue", "Limits", "Interest"
// Returns jetstream.WorkQueuePolicy as default if the string is invalid
func ParseRetentionPolicy(policy string) jetstream.RetentionPolicy {
	switch policy {
	case "WorkQueue":
		return jetstream.WorkQueuePolicy
	case "Limits":
		return jetstream.LimitsPolicy
	case "Interest":
		return jetstream.InterestPolicy
	default:
		return jetstream.WorkQueuePolicy
	}
}

// NATS connection retry constants - similar to glassflow-api
const (
	NATSConnectionTimeout = 1 * time.Minute
	NATSConnectionRetries = 12
	NATSInitialRetryDelay = 1 * time.Second
	NATSMaxRetryDelay     = 30 * time.Second
	NATSMaxConnectionWait = 2 * time.Minute
)

// Config holds configuration for NATS client and stream creation
type Config struct {
	URL                string
	MaxAge             time.Duration
	MaxBytes           int64
	Retention          jetstream.RetentionPolicy
	AllowDirect        bool
	AllowAtomicPublish bool
}

type NATSClient struct {
	nc                 *nats.Conn
	js                 jetstream.JetStream
	maxAge             time.Duration
	maxBytes           int64
	retention          jetstream.RetentionPolicy
	allowDirect        bool
	allowAtomicPublish bool
}

func New(ctx context.Context, cfg Config) (*NATSClient, error) {
	var (
		nc  *nats.Conn
		err error
	)

	connCtx, cancel := context.WithTimeout(ctx, NATSMaxConnectionWait)
	defer cancel()

	retryDelay := NATSInitialRetryDelay

	for i := 0; i < NATSConnectionRetries; i++ {
		select {
		case <-connCtx.Done():
			return nil, fmt.Errorf("timeout after %v waiting to connect to NATS at %s", NATSMaxConnectionWait, cfg.URL)
		default:
		}

		nc, err = nats.Connect(cfg.URL, nats.Timeout(NATSConnectionTimeout))
		if err == nil {
			break
		}

		if i < NATSConnectionRetries-1 {
			select {
			case <-time.After(retryDelay):
				log.Printf("Retrying connection to NATS at %s in %v...", cfg.URL, retryDelay)
				// Continue with retry
			case <-connCtx.Done():
				return nil, fmt.Errorf("timeout during retry delay for NATS at %s: %w", cfg.URL, connCtx.Err())
			}
			// Exponential backoff
			retryDelay = min(time.Duration(float64(retryDelay)*1.5), NATSMaxRetryDelay)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to JetStream: %w", err)
	}

	return &NATSClient{
		nc:                 nc,
		js:                 js,
		maxAge:             cfg.MaxAge,
		maxBytes:           cfg.MaxBytes,
		retention:          cfg.Retention,
		allowDirect:        cfg.AllowDirect,
		allowAtomicPublish: cfg.AllowAtomicPublish,
	}, nil
}

// StreamConfig holds configuration for creating or updating a NATS stream.
type StreamConfig struct {
	Name     string
	Subjects []string // if empty, defaults to []string{Name + ".*"}
	MaxAge   time.Duration
	MaxBytes int64
	Discard  jetstream.DiscardPolicy
}

// DefaultStreamLimits returns the operator-level default stream limits.
func (n *NATSClient) DefaultStreamLimits() (time.Duration, int64) {
	return n.maxAge, n.maxBytes
}

// CreateOrUpdateStream creates or updates a NATS stream using the provided StreamConfig.
// If Subjects is empty, defaults to []string{cfg.Name + ".*"}.
func (n *NATSClient) CreateOrUpdateStream(ctx context.Context, cfg StreamConfig) error {
	subjects := cfg.Subjects
	if len(subjects) == 0 {
		subjects = []string{cfg.Name + ".*"}
	}
	//nolint:exhaustruct // readability
	sc := jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: subjects,
		Storage:  jetstream.FileStorage,

		MaxAge:   cfg.MaxAge,
		MaxBytes: cfg.MaxBytes,
		Discard:  cfg.Discard,

		Retention:          n.retention,
		AllowDirect:        n.allowDirect,
		AllowAtomicPublish: n.allowAtomicPublish,
	}

	_, err := n.JetStream().CreateOrUpdateStream(ctx, sc)
	if err != nil {
		// Skip if update fails for old pipelines with existing streams with different retention policy
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == JSErrCodeStreamRetentionPolicyChange {
			log.Printf("skipping stream %s: old pipeline with existing stream, retention policy change to/from workqueue not supported by NATS", cfg.Name)
			return nil
		}
		// Fallback: check error string for err_code=10052 in case error structure varies
		if strings.Contains(err.Error(), "10052") && strings.Contains(err.Error(), "retention policy") {
			log.Printf("skipping stream %s: old pipeline with existing stream, retention policy change to/from workqueue not supported by NATS", cfg.Name)
			return nil
		}

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

// UnbindStreamSubjects updates a stream to a sentinel subject so no real messages
// can be published to it, releasing ownership of its subjects for another stream to claim.
func (n *NATSClient) UnbindStreamSubjects(ctx context.Context, streamName string) error {
	stream, err := n.js.Stream(ctx, streamName)
	if err != nil {
		return fmt.Errorf("get stream %s: %w", streamName, err)
	}
	cfg := stream.CachedInfo().Config
	cfg.Subjects = []string{streamName + ".__unbound__"}
	_, err = n.js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("unbind subjects from stream %s: %w", streamName, err)
	}
	return nil
}

// AddStreamSource configures destStream to pull messages from sourceStream via NATS server-side
// copy. With WorkQueue retention, messages are removed from sourceStream as they are consumed.
// Idempotent — adding the same source twice is a no-op.
func (n *NATSClient) AddStreamSource(ctx context.Context, destStreamName, sourceStreamName string) error {
	stream, err := n.js.Stream(ctx, destStreamName)
	if err != nil {
		return fmt.Errorf("get stream %s: %w", destStreamName, err)
	}
	cfg := stream.CachedInfo().Config
	for _, src := range cfg.Sources {
		if src.Name == sourceStreamName {
			return nil
		}
	}
	cfg.Sources = append(cfg.Sources, &jetstream.StreamSource{
		Name: sourceStreamName,
	})
	_, err = n.js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("add source %s to stream %s: %w", sourceStreamName, destStreamName, err)
	}
	return nil
}

// IsStreamSourceComplete checks if all messages from sourceStream have been
// transferred to destStream (lag == 0). Returns true if the source is not found (already done).
func (n *NATSClient) IsStreamSourceComplete(ctx context.Context, destStreamName, sourceStreamName string) (bool, error) {
	stream, err := n.js.Stream(ctx, destStreamName)
	if err != nil {
		return false, fmt.Errorf("get stream %s: %w", destStreamName, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return false, fmt.Errorf("get stream info %s: %w", destStreamName, err)
	}
	for _, src := range info.Sources {
		if src.Name == sourceStreamName {
			return src.Lag == 0, nil
		}
	}
	return true, nil
}

// RemoveStreamSource removes a source from destStream's configuration.
func (n *NATSClient) RemoveStreamSource(ctx context.Context, destStreamName, sourceStreamName string) error {
	stream, err := n.js.Stream(ctx, destStreamName)
	if err != nil {
		return fmt.Errorf("get stream %s: %w", destStreamName, err)
	}
	cfg := stream.CachedInfo().Config
	filtered := make([]*jetstream.StreamSource, 0, len(cfg.Sources))
	for _, src := range cfg.Sources {
		if src.Name != sourceStreamName {
			filtered = append(filtered, src)
		}
	}
	cfg.Sources = filtered
	_, err = n.js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("remove source %s from stream %s: %w", sourceStreamName, destStreamName, err)
	}
	return nil
}

// ListPipelineStreams returns all NATS stream names belonging to this pipeline (by hash prefix).
func (n *NATSClient) ListPipelineStreams(ctx context.Context, pipelineHash string) ([]string, error) {
	prefix := fmt.Sprintf("gfm-%s-", pipelineHash)
	lister := n.js.StreamNames(ctx)
	var names []string
	for name := range lister.Name() {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("list streams with prefix %s: %w", prefix, err)
	}
	return names, nil
}

// GetStreamMessageCount returns the number of messages currently in a stream (0 if not found).
func (n *NATSClient) GetStreamMessageCount(ctx context.Context, streamName string) (uint64, error) {
	stream, err := n.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("get stream %s: %w", streamName, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("get stream info %s: %w", streamName, err)
	}
	return info.State.Msgs, nil
}

// DeleteConsumer deletes a consumer from a stream. Best-effort — returns nil if
// the consumer or stream does not exist.
func (n *NATSClient) DeleteConsumer(ctx context.Context, streamName, consumerName string) error {
	stream, err := n.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil
		}
		return fmt.Errorf("get stream %s: %w", streamName, err)
	}
	err = stream.DeleteConsumer(ctx, consumerName)
	if err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return fmt.Errorf("delete consumer %s from stream %s: %w", consumerName, streamName, err)
	}
	return nil
}

func (n *NATSClient) Close() error {
	n.nc.Close()
	return nil
}
