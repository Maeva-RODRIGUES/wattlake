#!/usr/bin/env bash
# Check that the cluster still resolves external names (github.com for Argo CD,
# the ODRE API for the ingestion). Read-only: changes nothing.
#
# CoreDNS forwards external names to the node's resolver, Docker's embedded
# DNS, which reads the host's DNS servers when the node container starts and
# keeps them. After the laptop changes network, those servers may no longer
# answer: the fix is "make cluster-restart" (see infra/README.md, "Dépannage").
#
# Usage: make cluster-dns-check
set -euo pipefail

NODE="k3d-wattlake-server-0"
# Cluster IP of CoreDNS (kube-dns service): queries go through the same path as a pod's.
CLUSTER_DNS="10.43.0.10"
NAMES=("github.com" "odre.opendatasoft.com")

# With systemd-resolved, /etc/resolv.conf only lists its local stub
# (127.0.0.53): the real upstream servers are in this file, which Docker reads.
HOST_RESOLV="/run/systemd/resolve/resolv.conf"
[ -r "$HOST_RESOLV" ] || HOST_RESOLV="/etc/resolv.conf"

host_servers=$(awk '/^nameserver/ {print $2}' "$HOST_RESOLV" | sort | xargs)
# Docker writes the servers it forwards to in a comment of the node's resolv.conf:
#   # ExtServers: [host(192.168.86.1) host(fd0f:ee:b0::1)]
node_servers=$(docker exec "$NODE" cat /etc/resolv.conf | grep -o 'host([^)]*)' | sed 's/host(\(.*\))/\1/' | sort | xargs)

echo "Host DNS servers ($HOST_RESOLV): ${host_servers:-none}"
echo "DNS servers used by the cluster nodes: ${node_servers:-none}"

# A mismatch alone is only a warning: the old servers may still answer, and
# Docker may leave out some host servers (loopback ones, for example). What
# matters is whether names resolve.
if [ "$host_servers" != "$node_servers" ]; then
    echo "WARNING: the cluster still uses the DNS servers of a previous network."
fi

status=0

for name in "${NAMES[@]}"; do
    if docker exec "$NODE" nslookup "$name" "$CLUSTER_DNS" >/dev/null 2>&1; then
        echo "OK      $name resolves through CoreDNS"
    else
        echo "FAILED  $name does not resolve through CoreDNS"
        status=1
    fi
done

if [ "$status" -ne 0 ]; then
    echo "Fix: make cluster-restart (2 to 3 minutes, no data lost)."
fi
exit "$status"
