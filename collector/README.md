# collector

A ready-made [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) configuration for qv2.
It needs the **contrib** distribution (`otelcol-contrib`), because it uses the `filter` and
`tail_sampling` processors. Tested with otelcol-contrib 0.165.0.

| File | What it does |
|---|---|
| [`qv2-collector.yaml`](qv2-collector.yaml) | Receives OTLP from your applications and sends **every** span with instrumentation scope `qv2` to the qv2 store, unsampled. Sends nothing else to the store. |
| [`backend.yaml`](backend.yaml) | Optional overlay. Also sends traces to your own tracing backend: every SQS span (qv2 and stock AWS SDK) unsampled, and everything else tail-sampled. |

```sh
# qv2 store only
otelcol-contrib --config qv2-collector.yaml

# qv2 store, plus your own backend
QV2_BACKEND_ENDPOINT=http://jaeger:4318 otelcol-contrib --config qv2-collector.yaml --config backend.yaml
```

Point the qv2 packages at the Collector with `QV2_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces`
(the default). The packages use their own always-on tracer provider, so the application's head sampling
never drops qv2 spans. This configuration makes sure the Collector doesn't drop them either.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `QV2_COLLECTOR_HTTP_ENDPOINT` | `localhost:4318` | OTLP/HTTP listen address. Use `0.0.0.0:4318` in a container. |
| `QV2_COLLECTOR_GRPC_ENDPOINT` | `localhost:4317` | OTLP/gRPC listen address. |
| `QV2_STORE_ENDPOINT` | `http://localhost:4319` | Base URL of the qv2 store's OTLP/HTTP receiver. The exporter appends `/v1/traces`. |
| `QV2_BACKEND_ENDPOINT` | none, required by `backend.yaml` | Base OTLP/HTTP URL of your tracing backend. |
| `QV2_BACKEND_SAMPLING_PERCENTAGE` | `10` | Share of non-SQS traces kept for the backend. |

The store listens on 4318 by default, so that the packages can export to it directly without a
Collector. When the Collector and the store run on the same host, start the store on 4319
(`qv2-store --port 4319`, see [`../server/`](../server/)).

## Why it is built this way

- **The store only gets qv2 spans.** It reads only spans with scope `qv2` (span contract section 1), so
  the `filter/qv2_only` processor drops everything else before export.
- **Every SQS span is kept, by configuration alone.** In `backend.yaml` the traces are split into two
  pipelines: one keeps SQS spans and sends them unsampled, the other drops SQS spans and tail-samples
  what is left. This is the setup QV-11 measured in recheck B1, where the Collector kept 467 of 467 SQS
  spans from Python, 231 of 231 from Node.js and 354 of 354 from Java while sampling everything else at
  1%. The match expression for stock SQS spans is the one QV-11 used, plus the current semantic
  convention `messaging.system.name == "aws.sqs"`.
- **Retries:** if the store is down, the exporter keeps retrying (`max_elapsed_time: 0s`) until its
  in-memory queue of 5,000 batches is full. After that, new spans are dropped, and they are lost if the
  Collector restarts. A persistent queue (`file_storage` extension) would prevent that, but it is not
  configured in 0.1.
- **Trade-off of the split:** in your backend, a sampled-out trace still shows its SQS spans, without
  their parent HTTP or database spans. That is the cost of keeping every SQS span.

## Tests

[`tests/test_collector.py`](tests/test_collector.py) starts the real Collector between two fake OTLP
endpoints, sends one trace holding qv2 spans, stock SQS spans in the forms botocore, the Node.js AWS SDK
instrumentation and Java emit, and other spans, and checks that:

- the store endpoint receives exactly the qv2 spans;
- with `backend.yaml` and 0% sampling, the backend receives every qv2 and stock SQS span and none of
  the others, even after the tail-sampling decision wait;
- with 100% sampling, the backend receives every span.

```sh
pip install pytest opentelemetry-proto
QV2_OTELCOL_BIN=/path/to/otelcol-contrib pytest collector/tests
```

Without an `otelcol-contrib` binary the tests are skipped. CI downloads 0.165.0.
