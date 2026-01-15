package notifications

import "time"

const (
	// DefaultStreamName is the default name for the notifications NATS stream
	DefaultStreamName = "glassflow-notifications"
	// DefaultStreamMaxAge is the default maximum age for notifications in the stream (7 days)
	DefaultStreamMaxAge = 168 * time.Hour
	// DefaultStreamMaxBytes is the default maximum size for the notifications stream (100GB)
	DefaultStreamMaxBytes = int64(107374182400)
)

// Config holds the configuration for the notification client
type Config struct {
	// Enabled determines whether notifications are enabled
	Enabled bool
	// StreamName is the name of the NATS stream for notifications
	StreamName string
	// StreamMaxAge is the maximum age for notifications in the stream
	StreamMaxAge time.Duration
	// StreamMaxBytes is the maximum size for the notifications stream
	StreamMaxBytes int64
}

// DefaultConfig returns a default configuration with notifications disabled
func DefaultConfig() *Config {
	return &Config{
		Enabled:        false,
		StreamName:     DefaultStreamName,
		StreamMaxAge:   DefaultStreamMaxAge,
		StreamMaxBytes: DefaultStreamMaxBytes,
	}
}

// WithEnabled sets the enabled flag
func (c *Config) WithEnabled(enabled bool) *Config {
	c.Enabled = enabled
	return c
}

// WithStreamName sets the stream name
func (c *Config) WithStreamName(name string) *Config {
	c.StreamName = name
	return c
}

// WithStreamMaxAge sets the stream max age
func (c *Config) WithStreamMaxAge(maxAge time.Duration) *Config {
	c.StreamMaxAge = maxAge
	return c
}

// WithStreamMaxBytes sets the stream max bytes
func (c *Config) WithStreamMaxBytes(maxBytes int64) *Config {
	c.StreamMaxBytes = maxBytes
	return c
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.StreamName == "" {
		return ErrInvalidConfig{Field: "StreamName", Reason: "stream name cannot be empty"}
	}
	if c.StreamMaxAge <= 0 {
		return ErrInvalidConfig{Field: "StreamMaxAge", Reason: "stream max age must be positive"}
	}
	if c.StreamMaxBytes <= 0 {
		return ErrInvalidConfig{Field: "StreamMaxBytes", Reason: "stream max bytes must be positive"}
	}
	return nil
}
