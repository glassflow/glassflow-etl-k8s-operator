# GlassFlow ETL Kubernetes Operator

A Kubernetes operator for deploying and managing [GlassFlow ClickHouse ETL](https://github.com/glassflow/clickhouse-etl) pipelines on Kubernetes clusters.

## Overview

This operator automates the deployment and lifecycle management of GlassFlow ETL pipelines by creating and managing the necessary Kubernetes resources. It orchestrates three main components:

- **Ingestor**: Consumes data from Kafka topics and streams to NATS
- **Join**: Performs stream joining operations (optional)
- **Sink**: Writes processed data to ClickHouse

The operator uses NATS JetStream for stream processing and coordination between components.

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Kafka     │───▶│  Ingestor   │───▶│    NATS     │
│   Topics    │    │             │    │  JetStream  │
└─────────────┘    └─────────────┘    └─────────────┘
                                              │
                                              ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  ClickHouse │◀───│    Sink     │◀───│    Join     │
│             │    │             │    │ (Optional)  │
└─────────────┘    └─────────────┘    └─────────────┘
```

## Prerequisites

- Go 1.23.0+
- Docker 17.03+
- kubectl 1.11.3+
- Kubernetes 1.11.3+ cluster (or Kind for local development)
- Helm 3.0+

## Quick Start

### Using Helm (Recommended)

1. **Add the Helm repository:**
   ```bash
   helm repo add glassflow https://glassflow.github.io/charts
   helm repo update
   ```

2. **Install the operator:**
   ```bash
   helm install glassflow-etl-operator glassflow/glassflow-etl-k8s-operator \
     --create-namespace \
     --namespace glassflow-etl-operator
   ```

3. **Create a pipeline:**
   ```bash
   kubectl apply -f config/samples/etl_v1alpha1_pipeline.yaml
   ```

### Standalone Installation

1. **Build and push the operator image:**
   ```bash
   make docker-build docker-push IMG=<registry>/glassflow-etl-k8s-operator:tag
   ```

2. **Install CRDs:**
   ```bash
   make install
   ```

3. **Deploy the operator:**
   ```bash
   make deploy IMG=<registry>/glassflow-etl-k8s-operator:tag
   ```

## Development Setup

### Local Development with Kind

1. **Create a Kind cluster:**
   ```bash
   kind create cluster --name glassflow-dev
   ```

2. **Install NATS:**
   ```bash
   kubectl apply -f https://raw.githubusercontent.com/nats-io/k8s/main/helm/charts/nats/templates/namespace.yaml
   helm repo add nats https://nats-io.github.io/k8s/helm/charts/
   helm install nats nats/nats --namespace nats --create-namespace
   ```

3. **Build and load the operator image:**
   ```bash
   make docker-build
   kind load docker-image controller:latest --name glassflow-dev
   ```

4. **Deploy to Kind cluster:**
   ```bash
   make deploy IMG=controller:latest
   ```

5. **Run tests:**
   ```bash
   make test          # Unit tests
   make test-e2e      # End-to-end tests (requires Kind cluster)
   ```

### Component Images

The operator uses these GlassFlow component images by default:

```yaml
componentImages:
  ingestor: ghcr.io/glassflow/glassflow-etl-ingestor:stable
  join: ghcr.io/glassflow/glassflow-etl-join:stable
  sink: ghcr.io/glassflow/glassflow-etl-sink:stable
```

You can override these via environment variables or Helm values.

## Pipeline Configuration

### Pipeline CRD Example

```yaml
apiVersion: etl.glassflow.io/v1alpha1
kind: Pipeline
metadata:
  name: sample-pipeline
spec:
  pipeline_id: "pipeline-001"
  dlq: "dlq-stream"
  sources:
    type: kafka
    topics:
      - topic_name: "input-topic"
        stream: "input-stream"
        dedup_window: 300000000000  # 5 minutes in nanoseconds
  join:
    type: "stream"
    stream: "joined-stream"
    enabled: true
  sink: "clickhouse"
  config: "pipeline-config-json"
```

### Pipeline Status

The operator tracks the status of each component:

```yaml
status:
  ingestor_operator_status: "started"
  join_operator_status: "started"
  sink_operator_status: "started"
```

## Building and Releasing

### Build Commands

```bash
make build                    # Build the operator binary
make docker-build            # Build Docker image
make manifests               # Generate CRDs and RBAC
make generate                # Generate deepcopy methods
```

### Image Management

1. **Build with custom tag:**
   ```bash
   make docker-build IMG=ghcr.io/your-org/glassflow-etl-k8s-operator:v1.0.0
   ```

2. **Push to registry:**
   ```bash
   make docker-push IMG=ghcr.io/your-org/glassflow-etl-k8s-operator:v1.0.0
   ```

3. **Create stable release:**
   ```bash
   # Tag your release
   git tag v1.0.0
   git push origin v1.0.0
   
   # Build and push release image
   make docker-build docker-push IMG=ghcr.io/your-org/glassflow-etl-k8s-operator:v1.0.0
   ```

### Helm Chart

The operator includes a Helm chart for easy deployment:

```bash
# Package the chart
helm package chart/

# Install from local chart
helm install glassflow-etl-operator ./glassflow-etl-k8s-operator-0.0.1.tgz
```

## CI/CD Pipeline

The project uses a consolidated CI/CD pipeline that runs on every push and pull request:

### Main CI/CD Workflow (`ci-cd.yml`)

**Triggers**: Push to main branch, Pull requests

**Jobs**:
1. **Lint** - Code quality checks with golangci-lint
2. **Test** - Unit tests with NATS integration
3. **E2E Test** - End-to-end tests with Kind cluster
4. **Build** - Docker image building and pushing
5. **Security Scan** - Vulnerability scanning with Trivy
6. **Validate Manifests** - Kustomize and CRD validation
7. **Prepare Deployment** - Build installer artifacts
8. **Notify** - Success/failure notifications

### Specialized Workflows

- **`security-scan.yml`** - Scheduled security scans (weekly)
- **`multi-platform-build.yml`** - Multi-architecture builds
- **`release.yml`** - Release coordination and versioning
- **`release-chart.yml`** - Helm chart releases
- **`release-operator.yml`** - Operator releases

### Local Testing

```bash
# Run the full CI pipeline locally
make test          # Unit tests
make test-e2e      # E2E tests (requires Kind)
make lint          # Code linting
make build         # Build binary
make docker-build  # Build Docker image
```

## Technology Stack

- **Kubebuilder**: Framework for building Kubernetes operators
- **Kustomize**: Kubernetes native configuration management
- **Helm**: Package manager for Kubernetes
- **NATS**: Messaging system for component coordination
- **Controller Runtime**: Kubernetes controller framework

## Testing

### Unit Tests
```bash
make test
```

### E2E Tests
```bash
make test-e2e  # Requires Kind cluster
```

### Linting
```bash
make lint
make lint-fix
```

## Troubleshooting

### Common Issues

1. **NATS Connection Failed:**
   - Ensure NATS is running and accessible
   - Check NATS address configuration in operator deployment

2. **Component Deployment Failed:**
   - Verify component images are accessible
   - Check resource limits and requests

3. **RBAC Errors:**
   - Ensure proper RBAC rules are applied
   - Check service account permissions

### Debug Mode

Enable debug logging:
```bash
kubectl set env deployment/glassflow-etl-k8s-operator-controller-manager \
  --env="LOG_LEVEL=debug" -n glassflow-etl-operator
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Submit a pull request

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Support

- [GitHub Issues](https://github.com/glassflow/glassflow-etl-k8s-operator/issues)
- [GlassFlow Documentation](https://docs.glassflow.dev)
- [GlassFlow Community](https://github.com/glassflow/clickhouse-etl)

