# Trailbeam: OTLP/HTTP ingest, JSON API and web screen on one port, SQLite on a volume.
#
#   docker build -t trailbeam .
#   docker run -p 127.0.0.1:4318:4318 -v trailbeam-data:/data trailbeam
#
# Then open http://127.0.0.1:4318/ and point the Trailbeam packages (or the Collector) at
# http://127.0.0.1:4318/v1/traces. Build locally; no image is published.

ARG PYTHON_VERSION=3.12

# The shared UI's shell (ui/), built into the Python package. Node is needed only here, not at runtime.
FROM node:22-slim AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

FROM python:${PYTHON_VERSION}-slim AS build
WORKDIR /src
COPY python/pyproject.toml python/README.md ./
COPY python/src ./src
COPY --from=ui /src/python/src/trailbeam/server/ui ./src/trailbeam/server/ui
RUN pip wheel --no-cache-dir --wheel-dir /wheels ".[server]"

FROM python:${PYTHON_VERSION}-slim
LABEL org.opencontainers.image.title="trailbeam" \
      org.opencontainers.image.description="Trailbeam: per-message timelines for Amazon SQS from Trailbeam OpenTelemetry spans" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/nick-lennox/trailbeam"
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels "trailbeam[server]" && rm -rf /wheels \
    && useradd --system --uid 10001 --home-dir /data --no-create-home trailbeam \
    && mkdir /data && chown trailbeam:trailbeam /data
ENV PYTHONUNBUFFERED=1 \
    TRAILBEAM_DB=/data/trailbeam.sqlite \
    TRAILBEAM_HOST=0.0.0.0 \
    TRAILBEAM_PORT=4318
USER trailbeam
VOLUME ["/data"]
EXPOSE 4318
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/api/health' % os.environ.get('TRAILBEAM_PORT', '4318'), timeout=4)"]
ENTRYPOINT ["trailbeam"]
