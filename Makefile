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

POSTGRES_MANIFESTS  := infra/postgres

AIRFLOW_REPO_URL    := https://airflow.apache.org
AIRFLOW_CHART       := apache-airflow/airflow
AIRFLOW_VERSION     := 1.22.0
AIRFLOW_VALUES      := infra/airflow/values.yaml

INGESTION_DIR       := ingestion
INGESTION_VENV      := $(INGESTION_DIR)/.venv
# Same Python as the apache/airflow:3.2.2 image the DAGs run in.
INGESTION_PYTHON    := 3.13.13

.PHONY: up down check-kubeconfig check-helm-env cluster-up cluster-down cluster-stop cluster-start \
	helm-repos seaweedfs-up seaweedfs-down postgres-up postgres-down nessie-up nessie-down \
	trino-up trino-down airflow-up airflow-down smoke-test screenshots ingestion-test

VENV                := .venv
# Browsers downloaded by Playwright stay inside the project virtual environment.
export PLAYWRIGHT_BROWSERS_PATH := $(CURDIR)/$(VENV)/ms-playwright

## Create the cluster and deploy the whole stack. Sequential sub-makes keep the
## order (cluster, storage, database, catalog, engine, orchestrator) even when make runs with -j.
up: check-kubeconfig check-helm-env
	$(MAKE) cluster-up
	$(MAKE) seaweedfs-up
	$(MAKE) postgres-up
	$(MAKE) nessie-up
	$(MAKE) trino-up
	$(MAKE) airflow-up

## Delete the cluster and all its data. Use cluster-stop to pause instead.
down: cluster-down

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
	helm repo add apache-airflow $(AIRFLOW_REPO_URL) --force-update
	helm repo update seaweedfs nessie trino apache-airflow

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

## Deploy PostgreSQL 17 (official image, port 5432) with the airflow role and database.
postgres-up: check-kubeconfig
	kubectl --context $(KUBE_CONTEXT) create namespace $(NAMESPACE) --dry-run=client --output yaml \
		| kubectl --context $(KUBE_CONTEXT) apply --filename -
	kubectl --context $(KUBE_CONTEXT) apply --kustomize $(POSTGRES_MANIFESTS)
	kubectl --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) rollout status statefulset/postgres --timeout 5m
	kubectl --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) get pods

## Remove PostgreSQL. The PVC is kept: delete it by hand to wipe the data.
postgres-down: check-kubeconfig
	kubectl --context $(KUBE_CONTEXT) delete --kustomize $(POSTGRES_MANIFESTS)

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

## Install or upgrade Airflow (LocalExecutor, metadata in the PostgreSQL from postgres-up).
airflow-up: check-kubeconfig check-helm-env helm-repos
	helm upgrade --install airflow $(AIRFLOW_CHART) \
		--version $(AIRFLOW_VERSION) \
		--kube-context $(KUBE_CONTEXT) \
		--namespace $(NAMESPACE) --create-namespace \
		--values $(AIRFLOW_VALUES) \
		--wait --timeout 10m
	kubectl --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) get pods

## Uninstall Airflow. Its metadata stays in PostgreSQL.
airflow-down: check-kubeconfig check-helm-env
	helm uninstall airflow --kube-context $(KUBE_CONTEXT) --namespace $(NAMESPACE)

## End-to-end check: Trino writes an Iceberg table to SeaweedFS and Nessie records the commit.
smoke-test: check-kubeconfig
	bash tests/smoke/run.sh

## Project virtual environment with Playwright and its headless Chromium (never installed globally).
$(VENV)/bin/playwright: tools/screenshots/requirements.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --requirement tools/screenshots/requirements.txt
	$(VENV)/bin/playwright install --only-shell chromium
	touch $@

## Run the smoke test, then capture the Nessie and Trino web interfaces into docs/images/.
screenshots: check-kubeconfig $(VENV)/bin/playwright
	$(VENV)/bin/python tools/screenshots/capture.py

## Ingestion virtual environment, built by uv with the Python of the Airflow image.
## Rebuilt from scratch whenever pyproject.toml changes.
$(INGESTION_VENV)/bin/pytest: $(INGESTION_DIR)/pyproject.toml
	uv venv --clear --python $(INGESTION_PYTHON) $(INGESTION_VENV)
	uv pip install --python $(INGESTION_VENV)/bin/python --editable "$(INGESTION_DIR)[test]"
	touch $@

## Run the ingestion unit tests (no network access: HTTP calls are mocked).
ingestion-test: $(INGESTION_VENV)/bin/pytest
	cd $(INGESTION_DIR) && .venv/bin/pytest
