package nats

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const DefaultStreamMaxAge = 168 * time.Hour       // 7 days
const DefaultStreamMaxBytes = int64(107374182400) // 100GB

// NATS connection retry constants - similar to glassflow-api
const (
	NATSConnectionTimeout = 1 * time.Minute
	NATSConnectionRetries = 12
	NATSInitialRetryDelay = 1 * time.Second
	NATSMaxRetryDelay     = 30 * time.Second
	NATSMaxConnectionWait = 2 * time.Minute
)

type NATSClient struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	maxAge   time.Duration
	maxBytes int64
}

func New(ctx context.Context, url string, maxAge time.Duration, maxBytes int64) (*NATSClient, error) {
	var (
		nc  *nats.Conn
		err error
	)

	connCtx, cancel := context.WithTimeout(ctx, NATSMaxConnectionWait)
	defer cancel()

	retryDelay := NATSInitialRetryDelay

	for i := range NATSConnectionRetries {
		select {
		case <-connCtx.Done():
			return nil, fmt.Errorf("timeout after %v waiting to connect to NATS at %s", NATSMaxConnectionWait, url)
		default:
		}

		nc, err = nats.Connect(url, nats.Timeout(NATSConnectionTimeout))
		if err == nil {
			break
		}

		if i < NATSConnectionRetries-1 {
			select {
			case <-time.After(retryDelay):
				log.Printf("Retrying connection to NATS to %s in %v...", url, retryDelay)
				// Continue with retry
			case <-connCtx.Done():
				return nil, fmt.Errorf("timeout during retry delay for NATS at %s: %w", url, connCtx.Err())
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
		AllowDirect:       true,
		AllowAtomicPublish: true,
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
