CLUSTER_NAME        := wattlake
KUBE_CONTEXT        := k3d-$(CLUSTER_NAME)
EXPECTED_KUBECONFIG := $(HOME)/.kube/perso.yaml
K3D_CONFIG          := infra/k3d/cluster.yaml
NAMESPACE           := wattlake

SEAWEEDFS_REPO_URL  := https://seaweedfs.github.io/seaweedfs/helm
SEAWEEDFS_CHART     := seaweedfs/seaweedfs
SEAWEEDFS_VERSION   := 4.52.0
SEAWEEDFS_VALUES    := infra/seaweedfs/values.yaml

NESSIE_REPO_URL     := https://charts.projectnessie.org
NESSIE_CHART        := nessie/nessie
NESSIE_VERSION      := 0.109.0
NESSIE_VALUES       := infra/nessie/values.yaml

TRINO_REPO_URL      := https://trinodb.github.io/charts
TRINO_CHART         := trino/trino
TRINO_VERSION       := 1.45.0
TRINO_VALUES        := infra/trino/values.yaml

.PHONY: check-kubeconfig check-helm-env cluster-up cluster-down cluster-stop cluster-start \
	helm-repos seaweedfs-up seaweedfs-down nessie-up nessie-down trino-up trino-down

## Abort unless KUBECONFIG points to the personal kubeconfig file.
check-kubeconfig:
	@if [ "$$KUBECONFIG" != "$(EXPECTED_KUBECONFIG)" ]; then \
		echo "KUBECONFIG must be $(EXPECTED_KUBECONFIG), got '$$KUBECONFIG'. Is direnv enabled?"; \
		exit 1; \
	fi

## Abort unless Helm uses the project-isolated config, cache and data directories.
check-helm-env:
	@for var in HELM_CONFIG_HOME HELM_CACHE_HOME HELM_DATA_HOME; do \
		value=$$(helm env $$var); \
		case "$$value" in \
			*/helm-perso) ;; \
			*) echo "$$var must end with /helm-perso, got '$$value'. Is direnv enabled?"; exit 1 ;; \
		esac; \
	done

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

## Register the Helm chart repositories used by the stack.
helm-repos: check-helm-env
	helm repo add seaweedfs $(SEAWEEDFS_REPO_URL) --force-update
	helm repo add nessie $(NESSIE_REPO_URL) --force-update
	helm repo add trino $(TRINO_REPO_URL) --force-update
	helm repo update seaweedfs nessie trino

## Install or upgrade SeaweedFS (S3 gateway on port 8333, bucket "warehouse").
seaweedfs-up: check-kubeconfig check-helm-env helm-repos
	helm upgrade --install seaweedfs $(SEAWEEDFS_CHART) \
		--version $(SEAWEEDFS_VERSION) \
		--kube-context $(KUBE_CONTEXT) \
		--namespace $(NAMESPACE) --create-namespace \
		--values $(SEAWEEDFS_VALUES) \
		--wait --timeout 5m
	kubectl --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) get pods

## Uninstall SeaweedFS. PVCs are kept: delete them by hand to wipe the data.
seaweedfs-down: check-kubeconfig check-helm-env
	helm uninstall seaweedfs --kube-context $(KUBE_CONTEXT) --namespace $(NAMESPACE)

## Install or upgrade Nessie (API on port 19120, in-memory version store).
nessie-up: check-kubeconfig check-helm-env helm-repos
	helm upgrade --install nessie $(NESSIE_CHART) \
		--version $(NESSIE_VERSION) \
		--kube-context $(KUBE_CONTEXT) \
		--namespace $(NAMESPACE) --create-namespace \
		--values $(NESSIE_VALUES) \
		--wait --timeout 5m
	kubectl --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) get pods

## Uninstall Nessie. With the in-memory store, all commits are lost.
nessie-down: check-kubeconfig check-helm-env
	helm uninstall nessie --kube-context $(KUBE_CONTEXT) --namespace $(NAMESPACE)

## Install or upgrade Trino (1 coordinator + 1 worker, Iceberg catalog on Nessie and SeaweedFS).
trino-up: check-kubeconfig check-helm-env helm-repos
	helm upgrade --install trino $(TRINO_CHART) \
		--version $(TRINO_VERSION) \
		--kube-context $(KUBE_CONTEXT) \
		--namespace $(NAMESPACE) --create-namespace \
		--values $(TRINO_VALUES) \
		--wait --timeout 5m
	kubectl --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) get pods

## Uninstall Trino. Tables stay in Nessie and SeaweedFS.
trino-down: check-kubeconfig check-helm-env
	helm uninstall trino --kube-context $(KUBE_CONTEXT) --namespace $(NAMESPACE)
