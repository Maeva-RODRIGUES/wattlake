# Real-time image: Python, the pinned dependencies and the wattlake_streaming
# package (producer and writer). Built by "make streaming-image" from the repository
# root (build context "."), published by the CI to ghcr.io.
# Files sent to the build: see Dockerfile.dockerignore next to this file.

# Tag for readability, digest for reproducibility: a tag can be rebuilt upstream,
# the digest always designates the image that was tested.
FROM python:3.13.17-slim@sha256:d98cc29d3266e535dd9da1099688d9cb3ca4753813571e071763d17725460962

# Links the image published on ghcr.io to its repository.
LABEL org.opencontainers.image.source="https://github.com/Maeva-RODRIGUES/wattlake"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /opt/wattlake/streaming

# Every package and its hash are pinned (make streaming-lock): pip refuses
# anything else. confluent-kafka ships librdkafka in its wheel, no apt needed.
COPY streaming/requirements.txt .
RUN pip install --require-hashes --requirement requirements.txt

# The package runs from its sources: no build step, so no unpinned build backend.
COPY streaming/src ./src
ENV PYTHONPATH=/opt/wattlake/streaming/src

# Unprivileged user, with a numeric id so that Kubernetes can check runAsNonRoot.
USER 10001:10001

CMD ["python", "-m", "wattlake_streaming.producer"]
