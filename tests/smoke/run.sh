#!/usr/bin/env bash
# End-to-end smoke test of the stack:
#   1. Trino runs stack.sql (write, read back, Iceberg and S3 assertions).
#   2. Nessie must show the table on branch main, with the insert as latest commit.
# Usage: make smoke-test
set -euo pipefail

CONTEXT="k3d-wattlake"
NAMESPACE="wattlake"
SQL_FILE="$(dirname "$0")/stack.sql"
TABLE="smoke_test.readings"
NESSIE_API="/api/v1/namespaces/${NAMESPACE}/services/nessie:19120/proxy/api/v2"

# UTC start time: the Nessie commit must be newer, so a previous run cannot make this one pass.
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%S)

echo "==> Trino: running ${SQL_FILE}"
# Piped rather than redirected: a confined kubectl (e.g. the snap package)
# cannot read a file redirected to stdin and silently sends nothing.
cat "$SQL_FILE" | kubectl --context "$CONTEXT" --namespace "$NAMESPACE" exec -i deploy/trino-coordinator -- \
    trino --output-format=ALIGNED

echo "==> Nessie: checking branch main"
entries=$(kubectl --context "$CONTEXT" get --raw "${NESSIE_API}/trees/main/entries")
history=$(kubectl --context "$CONTEXT" get --raw "${NESSIE_API}/trees/main/history?max-records=1")

python3 -c '
import json, sys

table, started_at = sys.argv[1], sys.argv[2]
entries, history = json.loads(sys.argv[3]), json.loads(sys.argv[4])

names = {".".join(e["name"]["elements"]) for e in entries["entries"] if e["type"] == "ICEBERG_TABLE"}
if table not in names:
    sys.exit(f"FAIL: table {table} not found on Nessie branch main")
print(f"OK: table {table} registered on branch main")

latest = history["logEntries"][0]["commitMeta"]
message, commit_time = latest["message"], latest["commitTime"]
expected = f"Iceberg append against {table}"
if message != expected:
    sys.exit(f"FAIL: latest commit is [{message}], expected [{expected}]")
# Both timestamps are UTC ISO 8601: comparing their first 19 characters orders them.
if commit_time[:19] < started_at:
    sys.exit(f"FAIL: latest commit at {commit_time} is older than this run ({started_at}Z)")
print(f"OK: latest commit [{expected}] at {commit_time}")
' "$TABLE" "$STARTED_AT" "$entries" "$history"

echo "==> Smoke test passed"
