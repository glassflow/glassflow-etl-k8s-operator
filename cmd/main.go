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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"path/filepath"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	etlv1alpha1 "github.com/glassflow/glassflow-etl-k8s-operator/api/v1alpha1"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/controller"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/nats"
	"github.com/glassflow/glassflow-etl-k8s-operator/internal/utils"
	// +kubebuilder:scaffold:imports
)

// getEnvOrDefault returns the value of the environment variable if set, otherwise returns the default value
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(etlv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	var natsAddr, natsOpAddr string

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")

	// TODO: update nats default value based on namespace
	flag.StringVar(&natsAddr, "nats-addr", "nats://nats.default.svc.cluster.local:4222",
		"NATS server address for operator and components")

	// useful for development to run operator outside of the cluster
	flag.StringVar(&natsOpAddr, "nats-op-addr", "",
		"NATS server address for operator and components")
	if natsOpAddr == "" {
		natsOpAddr = natsAddr
	}

	// NATS stream configuration
	var natsMaxStreamAge, natsMaxStreamBytes string
	flag.StringVar(&natsMaxStreamAge, "nats-max-stream-age", getEnvOrDefault(
		"NATS_MAX_STREAM_AGE", "168h"),
		"Maximum age for NATS streams (default: 7 days)")
	flag.StringVar(&natsMaxStreamBytes, "nats-max-stream-bytes", getEnvOrDefault(
		"NATS_MAX_STREAM_BYTES", "107374182400"),
		"Maximum bytes for NATS streams (default: 100GB)")

	// Component image configuration
	var ingestorImage, joinImage, sinkImage string
	flag.StringVar(&ingestorImage, "ingestor-image", getEnvOrDefault(
		"INGESTOR_IMAGE", "ghcr.io/glassflow/glassflow-etl-ingestor:latest"),
		"Image for the ingestor component")
	flag.StringVar(&joinImage, "join-image", getEnvOrDefault(
		"JOIN_IMAGE", "ghcr.io/glassflow/glassflow-etl-join:latest"),
		"Image for the join component")
	flag.StringVar(&sinkImage, "sink-image", getEnvOrDefault(
		"SINK_IMAGE", "ghcr.io/glassflow/glassflow-etl-sink:latest"),
		"Image for the sink component")

	// Component resource configuration
	var ingestorCPURequest, ingestorCPULimit, ingestorMemoryRequest, ingestorMemoryLimit string
	var joinCPURequest, joinCPULimit, joinMemoryRequest, joinMemoryLimit string
	var sinkCPURequest, sinkCPULimit, sinkMemoryRequest, sinkMemoryLimit string

	// Ingestor resources
	flag.StringVar(&ingestorCPURequest, "ingestor-cpu-request", getEnvOrDefault(
		"INGESTOR_CPU_REQUEST", "100m"),
		"CPU request for ingestor component")
	flag.StringVar(&ingestorCPULimit, "ingestor-cpu-limit", getEnvOrDefault(
		"INGESTOR_CPU_LIMIT", "150m"),
		"CPU limit for ingestor component")
	flag.StringVar(&ingestorMemoryRequest, "ingestor-memory-request", getEnvOrDefault(
		"INGESTOR_MEMORY_REQUEST", "128Mi"),
		"Memory request for ingestor component")
	flag.StringVar(&ingestorMemoryLimit, "ingestor-memory-limit", getEnvOrDefault(
		"INGESTOR_MEMORY_LIMIT", "128Mi"),
		"Memory limit for ingestor component")

	// Join resources
	flag.StringVar(&joinCPURequest, "join-cpu-request", getEnvOrDefault(
		"JOIN_CPU_REQUEST", "100m"),
		"CPU request for join component")
	flag.StringVar(&joinCPULimit, "join-cpu-limit", getEnvOrDefault(
		"JOIN_CPU_LIMIT", "150m"),
		"CPU limit for join component")
	flag.StringVar(&joinMemoryRequest, "join-memory-request", getEnvOrDefault(
		"JOIN_MEMORY_REQUEST", "128Mi"),
		"Memory request for join component")
	flag.StringVar(&joinMemoryLimit, "join-memory-limit", getEnvOrDefault(
		"JOIN_MEMORY_LIMIT", "128Mi"),
		"Memory limit for join component")

	// Sink resources
	flag.StringVar(&sinkCPURequest, "sink-cpu-request", getEnvOrDefault(
		"SINK_CPU_REQUEST", "100m"),
		"CPU request for sink component")
	flag.StringVar(&sinkCPULimit, "sink-cpu-limit", getEnvOrDefault(
		"SINK_CPU_LIMIT", "150m"),
		"CPU limit for sink component")
	flag.StringVar(&sinkMemoryRequest, "sink-memory-request", getEnvOrDefault(
		"SINK_MEMORY_REQUEST", "128Mi"),
		"Memory request for sink component")
	flag.StringVar(&sinkMemoryLimit, "sink-memory-limit", getEnvOrDefault(
		"SINK_MEMORY_LIMIT", "256Mi"),
		"Memory limit for sink component")

	// Component affinity configuration
	var ingestorAffinity, joinAffinity, sinkAffinity string
	flag.StringVar(&ingestorAffinity, "ingestor-affinity", getEnvOrDefault(
		"INGESTOR_AFFINITY", ""),
		"Node affinity for ingestor component (JSON)")
	flag.StringVar(&joinAffinity, "join-affinity", getEnvOrDefault(
		"JOIN_AFFINITY", ""),
		"Node affinity for join component (JSON)")
	flag.StringVar(&sinkAffinity, "sink-affinity", getEnvOrDefault(
		"SINK_AFFINITY", ""),
		"Node affinity for sink component (JSON)")

	// Observability configuration
	var observabilityLogsEnabled, observabilityMetricsEnabled, observabilityOTelEndpoint string
	var ingestorLogLevel, joinLogLevel, sinkLogLevel string
	var ingestorImageTag, joinImageTag, sinkImageTag string

	flag.StringVar(&observabilityLogsEnabled, "observability-logs-enabled", getEnvOrDefault(
		"OBSERVABILITY_LOGS_ENABLED", "false"),
		"Enable OpenTelemetry logs")
	flag.StringVar(&observabilityMetricsEnabled, "observability-metrics-enabled", getEnvOrDefault(
		"OBSERVABILITY_METRICS_ENABLED", "false"),
		"Enable OpenTelemetry metrics")
	flag.StringVar(&observabilityOTelEndpoint, "observability-otel-endpoint", getEnvOrDefault(
		"OBSERVABILITY_OTEL_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4318"),
		"OpenTelemetry collector endpoint")
	flag.StringVar(&ingestorLogLevel, "ingestor-log-level", getEnvOrDefault(
		"INGESTOR_LOG_LEVEL", "info"),
		"Log level for ingestor component")
	flag.StringVar(&joinLogLevel, "join-log-level", getEnvOrDefault(
		"JOIN_LOG_LEVEL", "info"),
		"Log level for join component")
	flag.StringVar(&sinkLogLevel, "sink-log-level", getEnvOrDefault(
		"SINK_LOG_LEVEL", "info"),
		"Log level for sink component")
	flag.StringVar(&ingestorImageTag, "ingestor-image-tag", getEnvOrDefault(
		"INGESTOR_IMAGE_TAG", "stable"),
		"Image tag for ingestor component (used as service version)")
	flag.StringVar(&joinImageTag, "join-image-tag", getEnvOrDefault(
		"JOIN_IMAGE_TAG", "stable"),
		"Image tag for join component (used as service version)")
	flag.StringVar(&sinkImageTag, "sink-image-tag", getEnvOrDefault(
		"SINK_IMAGE_TAG", "stable"),
		"Image tag for sink component (used as service version)")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for metrics and webhooks certificates
	var metricsCertWatcher, webhookCertWatcher *certwatcher.CertWatcher

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "to initialize metrics certificate watcher", "error", err)
			os.Exit(1)
		}

		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "c38088ea.glassflow.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Parse NATS stream configuration with fallback to defaults
	maxAge, err := time.ParseDuration(natsMaxStreamAge)
	if err != nil {
		setupLog.Error(err,
			"unable to parse nats max stream age, using default",
			"value", natsMaxStreamAge, "default", "168h")
		maxAge = 168 * time.Hour // 7 days default
	}

	maxBytes, err := utils.ParseBytes(natsMaxStreamBytes)
	if err != nil {
		setupLog.Error(err, "unable to parse nats max stream bytes, using default",
			"value", natsMaxStreamBytes, "default", "107374182400")
		maxBytes = 107374182400 // 100GB default
	}

	ctx := context.Background()
	natsClient, err := nats.New(ctx, natsOpAddr, maxAge, maxBytes)
	if err != nil {
		setupLog.Error(err, "unable to connect to nats")
		os.Exit(1)
	}

	if err = (&controller.PipelineReconciler{
		Client:                      mgr.GetClient(),
		Scheme:                      mgr.GetScheme(),
		NATSClient:                  natsClient,
		ComponentNATSAddr:           natsAddr,
		NATSMaxStreamAge:            natsMaxStreamAge,
		NATSMaxStreamBytes:          natsMaxStreamBytes,
		IngestorImage:               ingestorImage,
		JoinImage:                   joinImage,
		SinkImage:                   sinkImage,
		IngestorCPURequest:          ingestorCPURequest,
		IngestorCPULimit:            ingestorCPULimit,
		IngestorMemoryRequest:       ingestorMemoryRequest,
		IngestorMemoryLimit:         ingestorMemoryLimit,
		JoinCPURequest:              joinCPURequest,
		JoinCPULimit:                joinCPULimit,
		JoinMemoryRequest:           joinMemoryRequest,
		JoinMemoryLimit:             joinMemoryLimit,
		SinkCPURequest:              sinkCPURequest,
		SinkCPULimit:                sinkCPULimit,
		SinkMemoryRequest:           sinkMemoryRequest,
		SinkMemoryLimit:             sinkMemoryLimit,
		IngestorAffinity:            ingestorAffinity,
		JoinAffinity:                joinAffinity,
		SinkAffinity:                sinkAffinity,
		ObservabilityLogsEnabled:    observabilityLogsEnabled,
		ObservabilityMetricsEnabled: observabilityMetricsEnabled,
		ObservabilityOTelEndpoint:   observabilityOTelEndpoint,
		IngestorLogLevel:            ingestorLogLevel,
		JoinLogLevel:                joinLogLevel,
		SinkLogLevel:                sinkLogLevel,
		IngestorImageTag:            ingestorImageTag,
		JoinImageTag:                joinImageTag,
		SinkImageTag:                sinkImageTag,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Pipeline")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if metricsCertWatcher != nil {
		setupLog.Info("Adding metrics certificate watcher to manager")
		if err := mgr.Add(metricsCertWatcher); err != nil {
			setupLog.Error(err, "unable to add metrics certificate watcher to manager")
			os.Exit(1)
		}
	}

	if webhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
