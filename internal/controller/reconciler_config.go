package controller

import (
	"fmt"
	"strings"
)

// ReconcilerConfig groups runtime/operator configuration passed to PipelineReconciler.
type ReconcilerConfig struct {
	NATS               NATSSettings
	Images             ComponentImages
	PullPolicies       ComponentPullPolicies
	ResourceDefaults   ComponentResourceDefaults
	DedupStorage       DedupStorageDefaults
	Affinity           ComponentAffinity
	Observability      ComponentObservability
	Namespaces         PipelineNamespaces
	UsageStats         UsageStatsSettings
	ClusterProvider    string
	GlassflowNamespace string
	DatabaseURL        string
	Encryption         EncryptionSettings
}

type NATSSettings struct {
	ComponentAddr string
}

type ComponentImages struct {
	Ingestor string
	Join     string
	Sink     string
	Dedup    string
}

type ComponentPullPolicies struct {
	Ingestor string
	Join     string
	Sink     string
	Dedup    string
}

type ResourceDefaults struct {
	CPURequest    string
	CPULimit      string
	MemoryRequest string
	MemoryLimit   string
}

type ComponentResourceDefaults struct {
	Ingestor ResourceDefaults
	Join     ResourceDefaults
	Sink     ResourceDefaults
	Dedup    ResourceDefaults
}

type DedupStorageDefaults struct {
	Size       string
	StorageCls string
}

type ComponentAffinity struct {
	Ingestor string
	Join     string
	Sink     string
	Dedup    string
}

type ComponentLogLevels struct {
	Ingestor string
	Join     string
	Sink     string
	Dedup    string
}

type ComponentImageTags struct {
	Ingestor string
	Join     string
	Sink     string
	Dedup    string
}

type ComponentObservability struct {
	LogsEnabled    string
	MetricsEnabled string
	OTelEndpoint   string
	LogLevels      ComponentLogLevels
	ImageTags      ComponentImageTags
}

type PipelineNamespaces struct {
	Auto bool
	Name string
}

type UsageStatsSettings struct {
	Enabled        bool
	Endpoint       string
	Username       string
	Password       string
	InstallationID string
}

type EncryptionSettings struct {
	Enabled   bool
	Name      string
	Key       string
	Namespace string
}

// Validate checks for required configuration combinations.
func (c ReconcilerConfig) Validate() error {
	if !c.Namespaces.Auto && strings.TrimSpace(c.Namespaces.Name) == "" {
		return fmt.Errorf("namespaces.name must be set when namespaces.auto=false")
	}

	return nil
}
