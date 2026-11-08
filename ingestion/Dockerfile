# Airflow image for WattLake: the official image plus the ingestion code, the
# dbt project and the DAGs. Built by "make airflow-image" from the repository
# root (build context "."), loaded into k3d by "make airflow-image-import".
# Files sent to the build: see Dockerfile.dockerignore next to this file.

# Tag for readability, digest for reproducibility: a tag can be rebuilt upstream,
# the digest always designates the image that was tested.
FROM apache/airflow:3.3.2-python3.13@sha256:e9982ad3f49a60418622e1baf80c34b0daf3fa5091a336cb5ee61f0cdd189371

# Links the image published on ghcr.io to its repository.
LABEL org.opencontainers.image.source="https://github.com/Maeva-RODRIGUES/wattlake"

# Constraints of the base image's Airflow version: every package they list
# (pyarrow, s3fs, fsspec, httpx...) gets the version Airflow was released with.
# The ingestion package declares its own pinned dependencies (pyproject.toml),
# PyIceberg and s3fs included; the constraints apply to them and to theirs.
ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.13.txt

# 1. Ingestion package, in Airflow's own environment.
COPY --chown=airflow:0 ingestion/pyproject.toml /opt/wattlake/ingestion/pyproject.toml
COPY --chown=airflow:0 ingestion/src /opt/wattlake/ingestion/src

# apache-airflow is listed again so that pip can never upgrade it while
# resolving the other packages. The ingestion package is installed as a regular
# (non-editable) package: Python imports the copy in site-packages, and the
# sources in /opt/wattlake/ingestion (a few KB) are only the build input.
RUN pip install --no-cache-dir \
        --constraint "${AIRFLOW_CONSTRAINTS}" \
        "apache-airflow==3.2.2" \
        /opt/wattlake/ingestion

# 2. dbt, in a virtual environment of its own. Its pinned dependencies
# (transform/requirements.txt) differ from the Airflow constraints for 26
# shared packages: installing it next to Airflow would break one or the other.
USER root
RUN mkdir -p /opt/dbt && chown airflow:0 /opt/dbt
USER airflow
COPY --chown=airflow:0 transform/requirements.txt /opt/dbt/requirements.txt
# The base image sets PIP_USER=true for Airflow's environment; it must be off
# inside a virtual environment.
RUN python -m venv /opt/dbt/venv \
    && PIP_USER=false /opt/dbt/venv/bin/pip install --no-cache-dir --requirement /opt/dbt/requirements.txt

# 3. dbt project, with its packages (dbt_utils) installed at build time rather
# than downloaded at every run.
COPY --chown=airflow:0 transform/dbt_project.yml transform/profiles.yml transform/packages.yml transform/package-lock.yml /opt/wattlake/transform/
COPY --chown=airflow:0 transform/models /opt/wattlake/transform/models
COPY --chown=airflow:0 transform/macros /opt/wattlake/transform/macros
COPY --chown=airflow:0 transform/tests /opt/wattlake/transform/tests
RUN /opt/dbt/venv/bin/dbt deps \
        --project-dir /opt/wattlake/transform --profiles-dir /opt/wattlake/transform \
        --log-path /tmp/dbt/logs

# 4. DAGs.
COPY --chown=airflow:0 ingestion/dags /opt/airflow/dags
