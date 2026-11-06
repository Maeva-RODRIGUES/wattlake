# Airflow image for WattLake: the official image plus the ingestion code and its
# dependencies. Built by "make airflow-image", loaded into k3d by "make airflow-image-import".
# Build context: the ingestion/ directory.

# Tag for readability, digest for reproducibility: a tag can be rebuilt upstream,
# the digest always designates the image that was tested.
FROM apache/airflow:3.2.2-python3.13@sha256:bef4d135ec7b64658cf64089973edd62cab8fd72203f280c7e0952213c17d912

# Constraints of the base image's Airflow version: every package they list
# (pyarrow, s3fs, fsspec, httpx...) gets the version Airflow was released with.
# The ingestion package declares its own pinned dependencies (pyproject.toml),
# PyIceberg and s3fs included; the constraints apply to them and to theirs.
ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.13.txt

COPY --chown=airflow:0 pyproject.toml /opt/wattlake/ingestion/pyproject.toml
COPY --chown=airflow:0 src /opt/wattlake/ingestion/src

# apache-airflow is listed again so that pip can never upgrade it while
# resolving the other packages. The ingestion package is installed as a regular
# (non-editable) package: Python imports the copy in site-packages, and the
# sources in /opt/wattlake/ingestion (a few KB) are only the build input.
RUN pip install --no-cache-dir \
        --constraint "${AIRFLOW_CONSTRAINTS}" \
        "apache-airflow==3.2.2" \
        /opt/wattlake/ingestion

COPY --chown=airflow:0 dags /opt/airflow/dags
