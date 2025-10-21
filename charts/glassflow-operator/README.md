# Glassflow Operator Helm Chart

This Helm chart deploys the Glassflow Operator on Kubernetes.

> [!NOTE] 
> This Chart is meant to be used as a dependency of [glassflow-etl](https://github.com/glassflow/charts/tree/main/charts/glassflow-etl) chart.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+

## Configuration

### Pipelines Namespace Management

The operator supports two modes for managing pipeline namespaces:

#### Auto Mode (Default)
When `global.pipelines.namespace.auto: true` (default), the operator creates a dedicated namespace for each pipeline with the pattern `pipeline-<id>`.

```yaml
global:
  pipelines:
    namespace:
      auto: true  # Create per-pipeline namespaces
```

#### Shared Mode
When `global.pipelines.namespace.auto: false`, all pipelines are deployed to a single shared namespace. The operator will not create or delete this namespace.

```yaml
global:
  pipelines:
    namespace:
      auto: false  # Use shared namespace
      name: glassflow-pipelines  # Target namespace name
      create: true  # Helm will create the namespace if it doesn't exist
```

In shared mode, resource names are prefixed with `gf-pipeline-<id>-` to avoid conflicts between pipelines.
