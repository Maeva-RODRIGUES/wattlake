#!/usr/bin/env bash
# Run a dbt command against the Trino of the local cluster, through a
# temporary port-forward that is closed when dbt exits, even on failure.
# Usage: make dbt ARGS="debug"   (or: bash transform/run.sh debug)
set -euo pipefail

CONTEXT="k3d-wattlake"
NAMESPACE="wattlake"
# Not 8080: that port is often used to reach the Airflow web interface.
LOCAL_PORT="${TRINO_LOCAL_PORT:-18081}"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

kubectl --context "$CONTEXT" --namespace "$NAMESPACE" port-forward svc/trino "${LOCAL_PORT}:8080" >/dev/null 2>&1 &
PORT_FORWARD_PID=$!
trap 'kill "$PORT_FORWARD_PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 30); do
    if curl --silent --output /dev/null "http://localhost:${LOCAL_PORT}/v1/info"; then
        break
    fi
    sleep 1
done

TRINO_HOST=localhost TRINO_PORT="$LOCAL_PORT" \
    "${PROJECT_DIR}/.venv/bin/dbt" "$@" --project-dir "$PROJECT_DIR" --profiles-dir "$PROJECT_DIR"
