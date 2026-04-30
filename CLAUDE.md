# operator

GlassFlow's Kubernetes operator — manages the lifecycle of ETL pipelines as Kubernetes custom resources. Watches `Pipeline` CRDs and reconciles the desired state by creating/updating/deleting the underlying Deployments, Services, NATS streams, and other K8s resources.

## Repo layout

```
api/v1alpha1/           # Pipeline CRD types (PipelineSpec, PipelineStatus, etc.)
cmd/main.go             # Operator entry point — sets up controller-runtime manager
internal/
  controller/           # Core reconcile logic (Reconcile loop, predicates, helpers)
  consumers/            # Event consumers (pipeline state changes)
  kafka/                # Kafka topic management
  nats/                 # NATS stream/consumer provisioning
  notifications/        # Pipeline event notifications
  observability/        # OpenTelemetry setup
  pipelinegraph/        # Pipeline dependency graph
  postgres/             # PostgreSQL client
  storage/              # Persistent storage layer
  tracking/             # Usage tracking
  models/               # Internal model structs
  constants/            # Shared constants
  errs/                 # Error types
  utils/                # Shared utilities
pkg/usagestats/         # Usage stats reporting
config/                 # Kustomize manifests (CRDs, RBAC, manager deployment)
charts/                 # Helm chart for the operator
```

## CRD — Pipeline

The `Pipeline` CRD (`api/v1alpha1/pipeline_types.go`) is the central abstraction. Key fields:

- `spec.pipeline_id` — unique pipeline ID (min 5 chars)
- `spec.sources` — source type (`kafka`, `otlp.logs`, `otlp.traces`, `otlp.metrics`) + topic streams
- `spec.join` — stream join configuration
- `spec.sink` — ClickHouse sink config
- `spec.transform` — optional stateless transforms
- `spec.pipeline_resources` — resource requests/limits for pipeline components
- `status` — current pipeline phase (reconciler updates this)

Pipeline config is stored in a K8s Secret `pipeline-config-{pipeline_id}` in the glassflow namespace. The `spec.config` field is a legacy fallback.

## Reconcile flow

```
Pipeline CR created/updated
  → controller.Reconcile()
    → load Pipeline spec
    → provision NATS streams/consumers
    → create/update K8s Deployments for each component (ingestor, join, sink, dedup)
    → sync Kafka topics
    → update Pipeline status
```

Each pipeline component runs the same `glassflow-etl` binary with a different `-role` flag. The operator sets the correct images via `INGESTOR_IMAGE`, `JOIN_IMAGE`, `SINK_IMAGE` env vars at build time.

## Commands

```bash
make build              # Build manager binary (runs manifests + generate + fmt + vet)
make run                # Run controller locally against current kubeconfig context
make test               # Unit tests
make test-integration   # Integration tests (envtest)
make test-e2e           # E2e tests (requires Kind cluster)
make lint               # golangci-lint
make manifests          # Regenerate CRD manifests from types
make generate           # Regenerate DeepCopy methods
make install            # Install CRDs into cluster
make deploy             # Deploy operator to cluster
```

## Key technology choices

| Purpose | Library |
|---------|---------|
| Operator framework | controller-runtime |
| CRD codegen | controller-gen |
| K8s manifests | Kustomize |
| NATS client | nats.go |
| PostgreSQL | pgx v5 |
| Observability | OpenTelemetry + zap |
| Tests | Ginkgo v2 + Gomega |
| Retry | avast/retry-go v4 |

## Local dev

```bash
# Run operator locally (uses current kubeconfig)
make run

# Install CRDs into a local Kind cluster
make install

# Run unit + integration tests
make test-integration
```

The operator is typically run via the CLI in local dev:

```bash
glassflow up    # starts Kind cluster + all services including operator
```

## Git & PR conventions

- Branch naming follows Linear ticket ID: `ETL-XYZ` or `username/ETL-XYZ-description`
- No `Co-Authored-By: Claude` or AI attribution in commits/PRs

## After modifying CRD types

Always run `make manifests generate` after editing `api/v1alpha1/pipeline_types.go` — this regenerates the CRD YAML and DeepCopy methods.

## Domain context

The shared context repo lives at `../glassflow-agent-context/` (sibling directory). Read files from it when:

- **Implementing a feature or ticket** → read `../glassflow-agent-context/workflows/linear-tickets.md` before branching
- **Writing a PR description** → read `../glassflow-agent-context/prompts/pr-description.md`
- **Domain terminology is ambiguous** → read `../glassflow-agent-context/domain/glossary.md`
- **Designing a new component or data flow** → read `../glassflow-agent-context/projects/operator/architecture.md` and `domain/deployment-topology.md`

Don't load these for routine bug fixes or code tasks — read the code directly instead.
