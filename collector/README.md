# collector

A ready-made [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) configuration for Trailbeam.
It needs the **contrib** distribution (`otelcol-contrib`), because it uses the `filter` and
`tail_sampling` processors. Tested with otelcol-contrib 0.165.0.

| File | What it does |
|---|---|
| [`trailbeam-collector.yaml`](trailbeam-collector.yaml) | Receives OTLP from your applications and sends **every** span with instrumentation scope `trailbeam` to the Trailbeam, unsampled. Sends nothing else to Trailbeam. |
| [`backend.yaml`](backend.yaml) | Optional overlay. Also sends traces to your own tracing backend: every SQS span (Trailbeam and stock AWS SDK) unsampled, and everything else tail-sampled. |

```sh
# Trailbeam only
otelcol-contrib --config trailbeam-collector.yaml

# Trailbeam, plus your own backend
TRAILBEAM_BACKEND_ENDPOINT=http://jaeger:4318 otelcol-contrib --config trailbeam-collector.yaml --config backend.yaml
```

Point the Trailbeam packages at the Collector with `TRAILBEAM_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces`
(the default). The packages use their own always-on tracer provider, so the application's head sampling
never drops Trailbeam spans. This configuration makes sure the Collector doesn't drop them either.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `TRAILBEAM_COLLECTOR_HTTP_ENDPOINT` | `localhost:4318` | OTLP/HTTP listen address. Use `0.0.0.0:4318` in a container. |
| `TRAILBEAM_COLLECTOR_GRPC_ENDPOINT` | `localhost:4317` | OTLP/gRPC listen address. |
| `TRAILBEAM_ENDPOINT` | `http://localhost:4319` | Base URL of Trailbeam's OTLP/HTTP receiver. The exporter appends `/v1/traces`. |
| `TRAILBEAM_BACKEND_ENDPOINT` | none, required by `backend.yaml` | Base OTLP/HTTP URL of your tracing backend. |
| `TRAILBEAM_BACKEND_SAMPLING_PERCENTAGE` | `10` | Share of non-SQS traces kept for the backend. |

Trailbeam listens on 4318 by default, so that the packages can export to it directly without a
Collector. When the Collector and Trailbeam run on the same host, start Trailbeam on 4319
(`trailbeam --port 4319`, see [Run Trailbeam](https://docs.trailbeam.app/setup/run)).

## Why it is built this way

- **Trailbeam only gets Trailbeam spans.** It reads only spans with scope `trailbeam` (span contract section 1), so
  the `filter/trailbeam_only` processor drops everything else before export.
- **Every SQS span is kept, by configuration alone.** In `backend.yaml` the traces are split into two
  pipelines: one keeps SQS spans and sends them unsampled, the other drops SQS spans and tail-samples
  what is left. This is the setup QV-11 measured in recheck B1, where the Collector kept 467 of 467 SQS
  spans from Python, 231 of 231 from Node.js and 354 of 354 from Java while sampling everything else at
  1%. The match expression for stock SQS spans is the one QV-11 used, plus the current semantic
  convention `messaging.system.name == "aws.sqs"`.
- **Retries:** if Trailbeam is down, the exporter keeps retrying (`max_elapsed_time: 0s`) until its
  in-memory queue of 5,000 batches is full. After that, new spans are dropped, and they are lost if the
  Collector restarts. A persistent queue (`file_storage` extension) would prevent that, but it is not
  configured in 0.1.
- **Trade-off of the split:** in your backend, a sampled-out trace still shows its SQS spans, without
  their parent HTTP or database spans. That is the cost of keeping every SQS span.

## Tests

[`tests/test_collector.py`](tests/test_collector.py) starts the real Collector between two fake OTLP
endpoints, sends one trace holding Trailbeam spans, stock SQS spans in the forms botocore, the Node.js AWS SDK
instrumentation and Java emit, and other spans, and checks that:

- Trailbeam endpoint receives exactly the Trailbeam spans;
- with `backend.yaml` and 0% sampling, the backend receives every Trailbeam and stock SQS span and none of
  the others, even after the tail-sampling decision wait;
- with 100% sampling, the backend receives every span.

```sh
pip install pytest opentelemetry-proto
TRAILBEAM_OTELCOL_BIN=/path/to/otelcol-contrib pytest collector/tests
```

Without an `otelcol-contrib` binary the tests are skipped. CI downloads 0.165.0.
