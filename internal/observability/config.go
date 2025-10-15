package observability

// Config holds the configuration for observability
type Config struct {
	LogsEnabled       bool
	MetricsEnabled    bool
	ServiceName       string
	ServiceVersion    string
	ServiceNamespace  string
	ServiceInstanceID string
}
