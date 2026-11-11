#!/usr/bin/env bash
# Creates the stack's credentials as Kubernetes Secrets, with random values
# generated on this machine (see docs/adr/006-gestion-des-secrets.md).
# Git only holds the Secret names: the values never leave the cluster.
#
# Secrets that share a value form a group, created all at once or not at all:
#   - none of the group exists: the whole group is created;
#   - all exist: nothing changes, existing values are kept;
#   - some exist: the script stops, as new values would not match the others.
# Groups are independent: a group added later is created on an existing cluster
# without touching the others.
#   1. stack: each S3 key pair is both in its component's Secret and in the
#      SeaweedFS configuration; the airflow role password is both in
#      PostgreSQL's Secret and in Airflow's connection URL.
#   2. nessie-db: the nessie role, read by PostgreSQL and by Nessie.
#   3. streaming: the S3 key pair of the real-time writer, both in its Secret
#      and as an identity added to the SeaweedFS configuration of group 1
#      (by add_s3_identity.py), so that it can be added to an existing cluster.
#
# Usage: make secrets
set -euo pipefail

CONTEXT="k3d-wattlake"
NAMESPACE="wattlake"
STACK_SECRETS=(seaweedfs-s3-config nessie-credentials trino-credentials postgres-credentials airflow-credentials)
NESSIE_DB_SECRETS=(nessie-db-credentials)
STREAMING_SECRETS=(streaming-credentials)
LABEL="app.kubernetes.io/part-of=wattlake"

kubectl_ns() {
    kubectl --context "$CONTEXT" --namespace "$NAMESPACE" "$@"
}

# Prints "create" when no Secret of the group exists, "skip" when all exist,
# and fails when only some exist.
group_action() {
    local group="$1"; shift
    local existing=()
    for name in "$@"; do
        # Command substitution, not a redirection: a confined kubectl (snap)
        # prints nothing when its output is redirected to a file.
        if [ -n "$(kubectl_ns get secret "$name" --ignore-not-found --output name)" ]; then
            existing+=("$name")
        fi
    done
    if [ "${#existing[@]}" -eq "$#" ]; then
        echo "skip"
    elif [ "${#existing[@]}" -eq 0 ]; then
        echo "create"
    else
        echo "Group ${group}: only some Secrets exist (${existing[*]}), new values would not match them." >&2
        echo "Delete the whole group to generate it again (see infra/README.md)." >&2
        return 1
    fi
}

# Both checks run before anything is created.
stack_action=$(group_action stack "${STACK_SECRETS[@]}")
nessie_db_action=$(group_action nessie-db "${NESSIE_DB_SECRETS[@]}")
streaming_action=$(group_action streaming "${STREAMING_SECRETS[@]}")

if [ -z "$(kubectl --context "$CONTEXT" get namespace "$NAMESPACE" --ignore-not-found --output name)" ]; then
    kubectl --context "$CONTEXT" create namespace "$NAMESPACE"
fi

# Hexadecimal values: no character needs escaping in a URL, SQL or JSON.
hex() { openssl rand -hex "$1"; }

# Function bodies are not indented: their heredocs must start at column 0.
create_stack() {
# One S3 identity per component (20-character access key, 40-character secret key).
admin_access=$(hex 10) admin_secret=$(hex 20)
nessie_access=$(hex 10) nessie_secret=$(hex 20)
trino_access=$(hex 10) trino_secret=$(hex 20)
airflow_access=$(hex 10) airflow_secret=$(hex 20)

postgres_password=$(hex 24)
airflow_db_password=$(hex 24)

# Fernet key: 32 random bytes in url-safe base64, as Airflow expects.
fernet_key=$(openssl rand -base64 32 | tr '+/' '-_')

# SeaweedFS S3 identities. admin is kept for operations (it is used by no
# component); each component can only read, write and list the warehouse bucket.
bucket_rw='["Read:warehouse","Write:warehouse","List:warehouse"]'
s3_config=$(cat <<EOF
{"identities":[
{"name":"admin","credentials":[{"accessKey":"${admin_access}","secretKey":"${admin_secret}"}],"actions":["Admin","Read","Write","List","Tagging"]},
{"name":"nessie","credentials":[{"accessKey":"${nessie_access}","secretKey":"${nessie_secret}"}],"actions":${bucket_rw}},
{"name":"trino","credentials":[{"accessKey":"${trino_access}","secretKey":"${trino_secret}"}],"actions":${bucket_rw}},
{"name":"airflow","credentials":[{"accessKey":"${airflow_access}","secretKey":"${airflow_secret}"}],"actions":${bucket_rw}}
]}
EOF
)

# Manifests sent on stdin: the values never appear on a command line.
cat <<EOF | kubectl_ns create --filename -
apiVersion: v1
kind: Secret
metadata:
  name: seaweedfs-s3-config
  labels: {app.kubernetes.io/part-of: wattlake}
type: Opaque
stringData:
  seaweedfs_s3_config: '$(echo "$s3_config" | tr -d '\n')'
  admin-access-key-id: ${admin_access}
  admin-secret-access-key: ${admin_secret}
---
apiVersion: v1
kind: Secret
metadata:
  name: nessie-credentials
  labels: {app.kubernetes.io/part-of: wattlake}
type: Opaque
stringData:
  s3-access-key-id: ${nessie_access}
  s3-secret-access-key: ${nessie_secret}
---
apiVersion: v1
kind: Secret
metadata:
  name: trino-credentials
  labels: {app.kubernetes.io/part-of: wattlake}
type: Opaque
stringData:
  s3-access-key-id: ${trino_access}
  s3-secret-access-key: ${trino_secret}
---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  labels: {app.kubernetes.io/part-of: wattlake}
type: Opaque
stringData:
  postgres-password: ${postgres_password}
  airflow-password: ${airflow_db_password}
---
apiVersion: v1
kind: Secret
metadata:
  name: airflow-credentials
  labels: {app.kubernetes.io/part-of: wattlake}
type: Opaque
stringData:
  # Key names expected by the Airflow chart (*SecretName values).
  connection: postgresql://airflow:${airflow_db_password}@postgres.wattlake.svc.cluster.local:5432/airflow?sslmode=disable
  fernet-key: ${fernet_key}
  api-secret-key: $(hex 32)
  jwt-secret: $(hex 32)
  admin-password: $(hex 12)
  s3-access-key-id: ${airflow_access}
  s3-secret-access-key: ${airflow_secret}
EOF
}

create_nessie_db() {
cat <<EOF | kubectl_ns create --filename -
apiVersion: v1
kind: Secret
metadata:
  name: nessie-db-credentials
  labels: {app.kubernetes.io/part-of: wattlake}
type: Opaque
stringData:
  # Key names read by the Nessie chart (jdbc.secret) and by PostgreSQL's initdb.
  username: nessie
  password: $(hex 24)
EOF
}

create_streaming() {
local access secret
access=$(hex 10) secret=$(hex 20)
# First the SeaweedFS identity (created by group 1, just before): the keys go
# through the environment and stdin, never on a command line. If the Secret
# creation below fails, running the script again replaces the identity.
local config
# Command substitution, not a pipe from kubectl (see group_action).
config=$(kubectl_ns get secret seaweedfs-s3-config --output json)
printf '%s' "$config" \
    | S3_IDENTITY=streaming S3_ACCESS="$access" S3_SECRET="$secret" python3 "$(dirname "$0")/add_s3_identity.py" \
    | kubectl_ns replace --filename -

cat <<EOF | kubectl_ns create --filename -
apiVersion: v1
kind: Secret
metadata:
  name: streaming-credentials
  labels: {app.kubernetes.io/part-of: wattlake}
type: Opaque
stringData:
  s3-access-key-id: ${access}
  s3-secret-access-key: ${secret}
EOF

# SeaweedFS reads its S3 identities at startup only (see infra/seaweedfs/values.yaml).
if [ "$stack_action" = "skip" ]; then
    echo "SeaweedFS identity added: restart the filer to load it:"
    echo "  kubectl --context $CONTEXT --namespace $NAMESPACE rollout restart statefulset/seaweedfs-filer"
fi
}

# stack first: the streaming group adds an identity to its SeaweedFS configuration.
for group in stack nessie_db streaming; do
    action_var="${group}_action"
    if [ "${!action_var}" = "create" ]; then
        "create_${group}"
    else
        echo "Group ${group//_/-}: all Secrets already exist, values kept."
    fi
done

kubectl_ns get secrets --selector "$LABEL"
