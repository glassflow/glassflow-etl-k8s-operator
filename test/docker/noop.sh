#!/bin/sh
# No-op component for e2e tests. Ignores all args and stays alive so that
# StatefulSet ReadyReplicas reaches the expected count, allowing the operator
# create cascade to proceed without real Kafka / ClickHouse connectivity.
exec sleep infinity
