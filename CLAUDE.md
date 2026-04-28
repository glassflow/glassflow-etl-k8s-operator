# glassflow-etl-k8s-operator

Kubernetes operator (Kubebuilder v4) that orchestrates GlassFlow ETL pipeline components — manages CRDs, reconciliation loops, and deployment lifecycle.

## Repo layout

```
cmd/
  main.go               # Operator entry point
internal/
  api/v1alpha1/         # Pipeline CRD type definitions
  controller/           # Reconciliation logic
  pipelinegraph/        # Pipeline component DAG builder
  nats/                 # NATS JetStream integration
  postgres/             # PostgreSQL backend
  storage/              # Storage abstraction layer
  observability/        # OpenTelemetry logging/metrics
  consumers/            # Event consumers
  kafka/                # Kafka integration
  models/               # Shared data structures
  notifications/        # Notification handlers
  tracking/             # Usage tracking
  utils/                # Shared utilities
  errs/                 # Error types
config/
  crd/                  # Generated CRD manifests
  rbac/                 # RBAC roles
  manager/              # Controller manager config
  webhook/              # Webhook config
```

## Commands

```bash
make build              # Build operator binary
make test               # Unit tests
make test-integration   # Integration tests (envtest)
make test-e2e           # E2E tests (requires Kind cluster)
make lint               # golangci-lint
make manifests          # Generate/update CRD manifests
make generate           # Generate DeepCopy methods (run after CRD struct changes)
make deploy IMG=...     # Deploy to Kubernetes cluster
```

## Key technology choices

| Purpose | Library |
|---------|---------|
| Operator framework | Kubebuilder v4 / controller-runtime |
| CRD codegen | controller-gen |
| Integration tests | envtest |
| Messaging | NATS JetStream |
| DB | PostgreSQL |
| Observability | OpenTelemetry |

## Important workflows

**After changing CRD structs in `internal/api/v1alpha1/`:**
```bash
make generate   # Regenerate DeepCopy methods
make manifests  # Regenerate CRD YAML in config/crd/
```
Both must be run together — skipping either causes drift between Go types and deployed CRDs.

**RBAC:** `config/rbac/role.yaml` is generated from controller annotations. Edit the annotations in Go, then run `make manifests` — don't hand-edit the YAML.

## Boundaries with other repos

- Deploys and manages `clickhouse-etl` pipeline containers
- Packaged and deployed via `charts/`
- Monitored/managed via `cli/`

## Git & PR conventions

- Branch naming follows Linear ticket ID: `ETL-XYZ` or `username/ETL-XYZ-description`
- Backend changes reviewed by: Petr, Pablo, Kiran
- No `Co-Authored-By: Claude` or AI attribution in commits/PRs
