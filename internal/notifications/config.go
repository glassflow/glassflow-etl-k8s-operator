package notifications

const (
	// DefaultStreamName is the default name for the notifications NATS stream
	DefaultStreamName = "glassflow-notifications"
)

// Config holds the configuration for the notification client
type Config struct {
	// Enabled determines whether notifications are enabled
	Enabled bool
}

// DefaultConfig returns a default configuration with notifications disabled
func DefaultConfig() *Config {
	return &Config{
		Enabled: false,
	}
}

// WithEnabled sets the enabled flag
func (c *Config) WithEnabled(enabled bool) *Config {
	c.Enabled = enabled
	return c
}

// Validate validates the configuration
func (c *Config) Validate() error {
	return nil
}
