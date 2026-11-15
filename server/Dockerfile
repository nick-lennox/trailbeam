# Trailbeam store: OTLP/HTTP ingest, JSON API and web screen on one port, SQLite on a volume.
#
#   docker build -t trailbeam-store server
#   docker run -p 127.0.0.1:4318:4318 -v trailbeam-data:/data trailbeam-store
#
# Then open http://127.0.0.1:4318/ and point the Trailbeam packages (or the Collector) at
# http://127.0.0.1:4318/v1/traces. Build locally; no image is published.

ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim AS build
WORKDIR /src
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

FROM python:${PYTHON_VERSION}-slim
LABEL org.opencontainers.image.title="trailbeam-store" \
      org.opencontainers.image.description="Trailbeam store: per-message timelines for Amazon SQS from Trailbeam OpenTelemetry spans" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/nick-lennox/trailbeam"
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels \
    && useradd --system --uid 10001 --home-dir /data --no-create-home trailbeam \
    && mkdir /data && chown trailbeam:trailbeam /data
ENV PYTHONUNBUFFERED=1 \
    TRAILBEAM_STORE_DB=/data/trailbeam.sqlite \
    TRAILBEAM_STORE_HOST=0.0.0.0 \
    TRAILBEAM_STORE_PORT=4318
USER trailbeam
VOLUME ["/data"]
EXPOSE 4318
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/api/health' % os.environ.get('TRAILBEAM_STORE_PORT', '4318'), timeout=4)"]
ENTRYPOINT ["trailbeam-store"]
