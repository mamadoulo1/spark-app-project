# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (layer-cache friendly)
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ARG APP_VERSION=1.0.0
ARG BUILD_DATE
ARG VCS_REF

LABEL maintainer="Data Engineering Team" \
      version="${APP_VERSION}" \
      build-date="${BUILD_DATE}" \
      vcs-ref="${VCS_REF}" \
      description="Enterprise PySpark ETL Framework"

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_FORMAT=json \
    ENV=prod

# Install Java (required by PySpark) + tini for proper signal handling
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Create non-root user for security
RUN groupadd -r spark && useradd -r -g spark -d /app spark

WORKDIR /app

# Copy source code
COPY --chown=spark:spark src/ ./src/
COPY --chown=spark:spark configs/ ./configs/

USER spark

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "src.jobs.etl_job"]
