# Trailbeam: OTLP/HTTP ingest, JSON API and web screen on one port, SQLite on a volume.
#
#   docker build -t trailbeam .
#   docker run -p 127.0.0.1:4318:4318 -v trailbeam-data:/data trailbeam
#
# Then open http://127.0.0.1:4318/ and point the Trailbeam packages (or the Collector) at
# http://127.0.0.1:4318/v1/traces. Published as ghcr.io/nick-lennox/trailbeam (.github/workflows/release.yaml).
#
# For Postgres (TRAILBEAM_STORE_URL), add its driver: docker build --build-arg TRAILBEAM_EXTRAS=server,postgres
# (deploy/replicas/compose.yaml does). The default image has SQLite only.
#
# Every image has boto3 (the aws extra) for AWS reads, whatever TRAILBEAM_EXTRAS says. They stay off until an admin
# turns them on in Settings, and boto3 is not imported until then. The client library already brings botocore, so
# boto3 adds about 3.4 MB to a 312 MB image (measured for QV-589; README, "AWS reads").

ARG PYTHON_VERSION=3.12
ARG TRAILBEAM_EXTRAS=server

# The shared UI's shell (ui/), built into the server package. Node is needed only here, not at runtime.
FROM node:22-slim AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

# Two distributions: trailbeam (the client library, python/) and trailbeam-server (Trailbeam itself, server/,
# with the UI). The extras of trailbeam pull in trailbeam-server, so the image installs "trailbeam[EXTRAS]" as
# the README's pip install does.
FROM python:${PYTHON_VERSION}-slim AS build
ARG TRAILBEAM_EXTRAS
WORKDIR /src
COPY python/pyproject.toml python/README.md ./python/
COPY python/src ./python/src
COPY server/pyproject.toml server/README.md ./server/
COPY server/src ./server/src
COPY --from=ui /src/server/src/trailbeam/server/ui ./server/src/trailbeam/server/ui
RUN pip wheel --no-cache-dir --wheel-dir /wheels "./python[aws,${TRAILBEAM_EXTRAS}]" ./server

FROM python:${PYTHON_VERSION}-slim
ARG TRAILBEAM_EXTRAS
LABEL org.opencontainers.image.title="trailbeam" \
      org.opencontainers.image.description="Trailbeam: per-message timelines for Amazon SQS from Trailbeam OpenTelemetry spans" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/nick-lennox/trailbeam"
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels "trailbeam[aws,${TRAILBEAM_EXTRAS}]" && rm -rf /wheels \
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
