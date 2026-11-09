CLUSTER_NAME        := wattlake
KUBE_CONTEXT        := k3d-$(CLUSTER_NAME)
EXPECTED_KUBECONFIG := $(HOME)/.kube/perso.yaml
K3D_CONFIG          := infra/k3d/cluster.yaml
NAMESPACE           := wattlake

# Argo CD: the only component installed by hand. It deploys all the others
# (SeaweedFS, PostgreSQL, Nessie, Trino, Airflow) from infra/argocd/apps/,
# read from the main branch of the GitHub repository.
ARGOCD_NAMESPACE    := argocd
ARGOCD_REPO_URL     := https://argoproj.github.io/argo-helm
ARGOCD_CHART        := argo/argo-cd
# 10.11.1 (2026-10-23, Argo CD v3.5.6): latest release published at least two weeks before pinning.
ARGOCD_VERSION      := 10.11.1
ARGOCD_VALUES       := infra/argocd/values.yaml
ARGOCD_ROOT_APP     := infra/argocd/root.yaml

# Custom Airflow image (ingestion/Dockerfile). The CI builds and publishes it
# as ghcr.io/<owner>/wattlake-airflow:<commit SHA>; the deployed tag is set in
# infra/airflow/values.yaml. These defaults only name local test builds.
AIRFLOW_IMAGE_REPO  ?= wattlake-airflow
AIRFLOW_IMAGE_TAG   ?= local
AIRFLOW_IMAGE       := $(AIRFLOW_IMAGE_REPO):$(AIRFLOW_IMAGE_TAG)

INGESTION_DIR       := ingestion
INGESTION_VENV      := $(INGESTION_DIR)/.venv
# Same Python as the apache/airflow:3.2.2 image the DAGs run in.
INGESTION_PYTHON    := 3.13.13
# Same constraints as ingestion/Dockerfile: the tests resolve the image's versions.
AIRFLOW_CONSTRAINTS := https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.13.txt

TRANSFORM_DIR       := transform
TRANSFORM_VENV      := $(TRANSFORM_DIR)/.venv
# Same Python as the Airflow image, which runs dbt.
TRANSFORM_PYTHON    := 3.13.13

# Linter and formatter of every Python file (configuration: ruff.toml).
# 0.17.2 (2026-10-22): latest release published at least two weeks before pinning.
RUFF_VERSION        := 0.17.2
PYTHON_SOURCES      := ingestion tests/catalog tools

.PHONY: up down check-kubeconfig check-helm-env cluster-up cluster-down cluster-stop cluster-start \
	argocd-up argocd-apps airflow-image smoke-test rest-catalog-test screenshots ingestion-test \
	transform-lock dbt lint dbt-parse

VENV                := .venv
# Browsers downloaded by Playwright stay inside the project virtual environment.
export PLAYWRIGHT_BROWSERS_PATH := $(CURDIR)/$(VENV)/ms-playwright

## Create the cluster, install Argo CD, then hand it the stack. Sequential
## sub-makes keep the order even when make runs with -j.
up: check-kubeconfig check-helm-env
	$(MAKE) cluster-up
	$(MAKE) argocd-up
	$(MAKE) argocd-apps

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

## Install or upgrade Argo CD (web interface on port 80 of argocd-server, see README).
argocd-up: check-kubeconfig check-helm-env
	helm repo add argo $(ARGOCD_REPO_URL) --force-update
	helm repo update argo
	helm upgrade --install argocd $(ARGOCD_CHART) \
		--version $(ARGOCD_VERSION) \
		--kube-context $(KUBE_CONTEXT) \
		--namespace $(ARGOCD_NAMESPACE) --create-namespace \
		--values $(ARGOCD_VALUES) \
		--wait --timeout 5m
	kubectl --context $(KUBE_CONTEXT) --namespace $(ARGOCD_NAMESPACE) get pods

## Hand the stack to Argo CD: the root Application deploys one Application per
## component, read from infra/argocd/apps/ on the main branch.
argocd-apps: check-kubeconfig
	kubectl --context $(KUBE_CONTEXT) apply --filename $(ARGOCD_ROOT_APP)
	kubectl --context $(KUBE_CONTEXT) --namespace $(ARGOCD_NAMESPACE) get applications

## Build the custom Airflow image (official image, ingestion package, dbt in its
## own virtual environment, dbt project, DAGs). Context: the repository root.
## The CI runs it with the ghcr.io name and the commit SHA as tag.
airflow-image:
	docker build --file $(INGESTION_DIR)/Dockerfile --tag $(AIRFLOW_IMAGE) .

## End-to-end check: Trino writes an Iceberg table to SeaweedFS and Nessie records the commit.
smoke-test: check-kubeconfig
	bash tests/smoke/run.sh

## REST catalog check: PyIceberg writes a table through Nessie's Iceberg REST catalog
## from a throwaway pod, then Trino reads it through the native API (see ADR 004).
rest-catalog-test: check-kubeconfig
	bash tests/catalog/run.sh

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
	uv pip install --python $(INGESTION_VENV)/bin/python --constraints $(AIRFLOW_CONSTRAINTS) \
		--editable "$(INGESTION_DIR)[test]"
	touch $@

## Run the ingestion unit tests (no network access: HTTP calls are mocked).
ingestion-test: $(INGESTION_VENV)/bin/pytest
	cd $(INGESTION_DIR) && .venv/bin/pytest

## Pin every dbt dependency, transitive ones included, from transform/requirements.in.
## Releases newer than 14 days are ignored, so that every pin is at least two weeks old.
transform-lock:
	uv pip compile $(TRANSFORM_DIR)/requirements.in --python-version 3.13 \
		--exclude-newer $$(date -u -d '14 days ago' +%Y-%m-%d) \
		--output-file $(TRANSFORM_DIR)/requirements.txt

## dbt virtual environment, installed exactly from the pinned requirements.txt.
$(TRANSFORM_VENV)/bin/dbt: $(TRANSFORM_DIR)/requirements.txt
	uv venv --clear --python $(TRANSFORM_PYTHON) $(TRANSFORM_VENV)
	uv pip sync --python $(TRANSFORM_VENV)/bin/python $(TRANSFORM_DIR)/requirements.txt
	touch $@

## Check the style of every Python file, without changing anything (run in CI).
## To fix: uvx ruff@$(RUFF_VERSION) check --fix ... then uvx ruff@$(RUFF_VERSION) format ...
lint:
	uvx ruff@$(RUFF_VERSION) check $(PYTHON_SOURCES)
	uvx ruff@$(RUFF_VERSION) format --check $(PYTHON_SOURCES)

## Validate the dbt project without any database (run in CI): YAML, Jinja, refs
## and test definitions. dbt build and the dbt unit tests need Trino: they run
## in the cluster (make dbt ARGS="build", or the dbt_transformations DAG).
dbt-parse: $(TRANSFORM_VENV)/bin/dbt
	cd $(TRANSFORM_DIR) && .venv/bin/dbt deps --profiles-dir . --log-path /tmp/dbt-parse/logs
	cd $(TRANSFORM_DIR) && .venv/bin/dbt parse --profiles-dir . \
		--target-path /tmp/dbt-parse/target --log-path /tmp/dbt-parse/logs

## Run a dbt command against the cluster's Trino, e.g. make dbt ARGS="debug".
dbt: check-kubeconfig $(TRANSFORM_VENV)/bin/dbt
	bash $(TRANSFORM_DIR)/run.sh $(ARGS)
