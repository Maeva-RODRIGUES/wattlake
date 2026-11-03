CLUSTER_NAME        := wattlake
KUBE_CONTEXT        := k3d-$(CLUSTER_NAME)
EXPECTED_KUBECONFIG := $(HOME)/.kube/perso.yaml
K3D_CONFIG          := infra/k3d/cluster.yaml

.PHONY: check-kubeconfig cluster-up cluster-down cluster-stop cluster-start

## Abort unless KUBECONFIG points to the personal kubeconfig file.
check-kubeconfig:
	@if [ "$$KUBECONFIG" != "$(EXPECTED_KUBECONFIG)" ]; then \
		echo "KUBECONFIG must be $(EXPECTED_KUBECONFIG), got '$$KUBECONFIG'. Is direnv enabled?"; \
		exit 1; \
	fi

## Create the local k3d cluster (no-op if it already exists).
cluster-up: check-kubeconfig
	@if k3d cluster get $(CLUSTER_NAME) >/dev/null 2>&1; then \
		echo "Cluster $(CLUSTER_NAME) already exists."; \
	else \
		k3d cluster create --config $(K3D_CONFIG); \
	fi
	kubectl --context $(KUBE_CONTEXT) get nodes

## Delete the local k3d cluster.
cluster-down: check-kubeconfig
	k3d cluster delete $(CLUSTER_NAME)

## Stop the cluster containers without deleting them (state is kept).
cluster-stop: check-kubeconfig
	k3d cluster stop $(CLUSTER_NAME)

## Restart a previously stopped cluster.
cluster-start: check-kubeconfig
	k3d cluster start $(CLUSTER_NAME)
