# The OpenTelemetry Collector (contrib) with collector/qv2-collector.yaml from this repository. Built from the
# repository root by demo/compose.yaml; copying the file in, rather than bind-mounting it, works on every
# Docker setup. Local only, nothing is published.
FROM otel/opentelemetry-collector-contrib:0.165.0
COPY collector/qv2-collector.yaml /etc/qv2/qv2-collector.yaml
CMD ["--config", "/etc/qv2/qv2-collector.yaml"]
