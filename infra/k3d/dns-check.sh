#!/usr/bin/env bash
# Check that the cluster still resolves external names (github.com for Argo CD,
# the ODRE API for the ingestion). Read-only: changes nothing.
#
# Each k3d node is a Docker container whose resolver is Docker's embedded DNS.
# Docker reads the host's DNS servers when the container starts and keeps them.
# Two failures follow (see infra/README.md, "Dépannage"):
# - after the laptop changes network, the old servers may no longer answer;
# - a node started while the host had no DNS server (network switching) gets
#   none at all ("NO EXTERNAL NAMESERVERS DEFINED").
# Nodes start independently, so each one is checked: CoreDNS forwards external
# names to the resolver of the node it runs on, and containerd pulls images
# through the resolver of its own node.
#
# Usage: make cluster-dns-check
set -euo pipefail

CLUSTER="wattlake"
# Cluster IP of CoreDNS (kube-dns service): queries go through the same path as a pod's.
CLUSTER_DNS="10.43.0.10"
NAMES=("github.com" "odre.opendatasoft.com")

# With systemd-resolved, /etc/resolv.conf only lists its local stub
# (127.0.0.53): the real upstream servers are in this file, which Docker reads.
HOST_RESOLV="/run/systemd/resolve/resolv.conf"
[ -r "$HOST_RESOLV" ] || HOST_RESOLV="/etc/resolv.conf"

host_servers=$(awk '/^nameserver/ {print $2}' "$HOST_RESOLV" | sort | xargs)
echo "Host DNS servers ($HOST_RESOLV): ${host_servers:-none}"
if [ -z "$host_servers" ]; then
    echo "WARNING: the host has no DNS server. Wait for the network to be up before restarting anything."
fi

# k3d labels its containers with the cluster name and the node role; the load
# balancer container runs no Kubernetes component and is left out.
mapfile -t nodes < <(
    docker ps --filter "label=k3d.cluster=$CLUSTER" --format '{{.Names}} {{.Label "k3d.role"}}' |
        awk '$2 == "server" || $2 == "agent" {print $1}' | sort
)
if [ "${#nodes[@]}" -eq 0 ]; then
    echo "FAILED  no running node for cluster $CLUSTER"
    exit 1
fi

status=0
failed_nodes=()

for node in "${nodes[@]}"; do
    echo "--- $node"
    resolv=$(docker exec "$node" cat /etc/resolv.conf)
    # Docker writes the servers it forwards to in a comment of the node's resolv.conf:
    #   # ExtServers: [host(192.168.86.1) host(fd0f:ee:b0::1)]
    node_servers=$(echo "$resolv" | grep -o 'host([^)]*)' | sed 's/host(\(.*\))/\1/' | sort | xargs || true)

    if [ -z "$node_servers" ] || grep -q 'NO EXTERNAL NAMESERVERS' <<<"$resolv"; then
        echo "FAILED  no external DNS server: the node started while the host had none"
        status=1
        failed_nodes+=("$node")
        continue
    fi
    echo "DNS servers: $node_servers"
    # A mismatch alone is only a warning: the old servers may still answer, and
    # Docker may leave out some host servers (loopback ones, for example). What
    # matters is whether names resolve.
    if [ "$host_servers" != "$node_servers" ]; then
        echo "WARNING: this node still uses the DNS servers of a previous network."
    fi

    node_ok=1
    for name in "${NAMES[@]}"; do
        if docker exec "$node" nslookup "$name" >/dev/null 2>&1; then
            echo "OK      $name resolves through the node resolver"
        else
            echo "FAILED  $name does not resolve through the node resolver"
            node_ok=0
        fi
    done
    if [ "$node_ok" -eq 0 ]; then
        status=1
        failed_nodes+=("$node")
    fi
done

# The path a pod uses: CoreDNS, then the resolver of the node CoreDNS runs on.
echo "--- CoreDNS ($CLUSTER_DNS)"
for name in "${NAMES[@]}"; do
    if docker exec "${nodes[0]}" nslookup "$name" "$CLUSTER_DNS" >/dev/null 2>&1; then
        echo "OK      $name resolves through CoreDNS"
    else
        echo "FAILED  $name does not resolve through CoreDNS"
        status=1
    fi
done

if [ "$status" -ne 0 ]; then
    if [ "${#failed_nodes[@]}" -gt 0 ]; then
        echo "Fix: restart the failing nodes only (about 1 minute, no data lost):"
        echo "  docker restart ${failed_nodes[*]}"
        echo "or the whole cluster: make cluster-restart (2 to 3 minutes)."
    else
        echo "Fix: make cluster-restart (2 to 3 minutes, no data lost)."
    fi
fi
exit "$status"
