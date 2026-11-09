#!/usr/bin/env bash
# REST catalog check (see docs/adr/004-catalogue-rest-ingestion.md):
#   1. PyIceberg writes a table through Nessie's Iceberg REST catalog, from a
#      throwaway pod on the cluster network, as Airflow will.
#   2. Trino reads the same table through Nessie's native API.
#   3. Nessie shows the table on branch main, with one commit per write of this
#      run (append, then overwrite). Nessie keeps the table history as commits:
#      the Iceberg metadata it returns only holds the current snapshot.
# Usage: make rest-catalog-test
set -euo pipefail

CONTEXT="k3d-wattlake"
NAMESPACE="wattlake"
SCRIPT="$(dirname "$0")/rest_roundtrip.py"
TABLE="rest_catalog_test.readings"
NESSIE_API="/api/v1/namespaces/${NAMESPACE}/services/nessie:19120/proxy/api/v2"

# Pinned versions: Python of the Airflow image, and the Airflow 3.2.2 constraints
# for every package they list (pyarrow, s3fs, fsspec, botocore...), so that the
# test resolves the same versions as the Airflow image. PyIceberg is not in the
# constraints and is pinned here. s3fs is needed because Nessie makes PyIceberg
# use FsspecFileIO (see ADR 004).
PYTHON_IMAGE="python:3.13.13-slim"
PIP_CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.13.txt"
PIP_PACKAGES="pyiceberg[pyarrow]==0.12.0 pyarrow==24.0.0 s3fs==2026.4.0"

# The pod uses Airflow's SeaweedFS identity, as the ingestion does. Its keys are
# read from the airflow-credentials Secret by the pod itself (secretKeyRef):
# they never leave the cluster. The strategic merge adds them to the container
# that kubectl run generates.
OVERRIDES=$(cat <<'JSON'
{"spec": {"containers": [{"name": "rest-catalog-test", "env": [
  {"name": "S3_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "airflow-credentials", "key": "s3-access-key-id"}}},
  {"name": "S3_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "airflow-credentials", "key": "s3-secret-access-key"}}}
]}]}}
JSON
)

# UTC start time: Nessie commits must be newer, so a previous run cannot make this one pass.
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%S)

echo "==> PyIceberg: writing ${TABLE} through the REST catalog"
# Piped rather than redirected: a confined kubectl (e.g. the snap package)
# cannot read a file redirected to stdin and silently sends nothing.
cat "$SCRIPT" | kubectl --context "$CONTEXT" --namespace "$NAMESPACE" run rest-catalog-test \
    --image="$PYTHON_IMAGE" --restart=Never --rm --stdin --quiet \
    --env="NESSIE_ICEBERG_URI=http://nessie:19120/iceberg" \
    --override-type=strategic --overrides="$OVERRIDES" \
    --command -- sh -c "pip install --quiet --no-cache-dir --root-user-action=ignore --disable-pip-version-check --constraint ${PIP_CONSTRAINTS} ${PIP_PACKAGES} && python -"

echo "==> Trino: reading ${TABLE} through the native Nessie API"
echo "SELECT CASE
    WHEN count(*) = 3 AND sum(mw) = 45580.25 AND count_if(source = 'wind' AND mw = 5000.0) = 1
        THEN format('OK: Trino reads %s rows, %s MW', count(*), sum(mw))
    ELSE fail(format('unexpected content: %s rows, %s MW', count(*), sum(mw)))
END AS trino_check
FROM iceberg.${TABLE};" | kubectl --context "$CONTEXT" --namespace "$NAMESPACE" exec -i deploy/trino-coordinator -- \
    trino --output-format=TSV

echo "==> Nessie: checking branch main"
entries=$(kubectl --context "$CONTEXT" get --raw "${NESSIE_API}/trees/main/entries")
history=$(kubectl --context "$CONTEXT" get --raw "${NESSIE_API}/trees/main/history?max-records=10")

python3 -c '
import json, sys

table, started_at = sys.argv[1], sys.argv[2]
entries, history = json.loads(sys.argv[3]), json.loads(sys.argv[4])

names = {".".join(e["name"]["elements"]) for e in entries["entries"] if e["type"] == "ICEBERG_TABLE"}
if table not in names:
    sys.exit(f"FAIL: table {table} not found on Nessie branch main")
print(f"OK: table {table} registered on branch main")

# Both timestamps are UTC ISO 8601: comparing their first 19 characters orders them.
recent = [e["commitMeta"] for e in history["logEntries"] if e["commitMeta"]["commitTime"][:19] >= started_at]
print(f"Nessie commits on main since {started_at}Z:")
for meta in reversed(recent):
    commit_time, message = meta["commitTime"][:19], meta["message"]
    print(f"    {commit_time}  {message}")
# The append and the overwrite must each be a commit made by this run.
writes = [m for m in recent if m["message"] == f"Update ICEBERG_TABLE {table}"]
if len(writes) < 2:
    sys.exit(f"FAIL: expected 2 table updates (append, overwrite) since {started_at}Z, found {len(writes)}")
print(f"OK: {len(writes)} table updates committed by this run (append, overwrite)")
' "$TABLE" "$STARTED_AT" "$entries" "$history"

echo "==> REST catalog test passed"
