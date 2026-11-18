# collector

A ready-made [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) configuration for Trailbeam.
It needs the **contrib** distribution (`otelcol-contrib`), because it uses the `filter` and
`tail_sampling` processors. Tested with otelcol-contrib 0.165.0.

| File | What it does |
|---|---|
| [`trailbeam-collector.yaml`](trailbeam-collector.yaml) | Receives OTLP from your applications and sends **every** span with instrumentation scope `trailbeam` to the Trailbeam, unsampled. Sends nothing else to Trailbeam. |
| [`backend.yaml`](backend.yaml) | Optional overlay. Also sends traces to your own tracing backend: every SQS span (Trailbeam and stock AWS SDK) unsampled, and everything else tail-sampled. |
| [`persistent-queue.yaml`](persistent-queue.yaml) | Optional overlay. Keeps the spans for Trailbeam in a queue on disk, so that they survive an outage, a slow Trailbeam and a Collector restart. Use it for high volume ([High volume](#high-volume)). |

```sh
# Trailbeam only
otelcol-contrib --config trailbeam-collector.yaml

# Trailbeam, plus your own backend
TRAILBEAM_BACKEND_ENDPOINT=http://jaeger:4318 otelcol-contrib --config trailbeam-collector.yaml --config backend.yaml

# Trailbeam, with a queue on disk (add backend.yaml too if you use it)
otelcol-contrib --config trailbeam-collector.yaml --config persistent-queue.yaml
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
| `TRAILBEAM_COLLECTOR_QUEUE_DIR` | `/var/lib/otelcol/trailbeam` | `persistent-queue.yaml`: the queue's directory. Created if missing. Must survive a restart: a volume in a container. |
| `TRAILBEAM_COLLECTOR_QUEUE_SPANS` | `2000000` | `persistent-queue.yaml`: spans the queue holds, about 1.1 KB of disk each. |

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
  Collector restarts. `persistent-queue.yaml` prevents both ([High volume](#high-volume)).
- **Trade-off of the split:** in your backend, a sampled-out trace still shows its SQS spans, without
  their parent HTTP or database spans. That is the cost of keeping every SQS span.

## High volume

The Trailbeam packages hold spans in memory until they are exported: 16,384 spans by default
(`TRAILBEAM_MAX_QUEUE_SIZE`), about 16 seconds of 1,000 spans/s. If Trailbeam is down or slower than your
traffic for longer than that, the packages drop spans. They count every drop (the
`trailbeam.sdk.spans.dropped` metric, a warning once a minute, `export_stats()` / `exportStats()`; see the
[Python](../python/README.md#dropped-spans) and [Node.js](../node/README.md#dropped-spans) READMEs), but
the messages still have gaps in their timelines.

For high volume, or wherever a Trailbeam restart or upgrade must not lose spans, send the spans to a
Collector next to the application, with `persistent-queue.yaml`:

```sh
TRAILBEAM_ENDPOINT=http://trailbeam:4318 \
TRAILBEAM_COLLECTOR_QUEUE_DIR=/var/lib/otelcol/trailbeam \
  otelcol-contrib --config trailbeam-collector.yaml --config persistent-queue.yaml
```

and point the applications at it (`TRAILBEAM_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces`, the
default). What changes:

- **The application is answered once a span is on disk.** The Collector writes it to its queue (with
  `fsync`) and answers at once, however far behind Trailbeam is. The packages' queue stays nearly
  empty.
- **Spans leave the queue only when Trailbeam has accepted them.** The exporter retries for as long as
  it takes. A Collector that crashes or restarts carries on from the queue on disk.
- **When the queue is full, the Collector holds the application's export until there is room**
  (`block_on_overflow`) instead of dropping the spans itself. An export that waits longer than the
  package's export timeout (10 s) fails; the package drops that batch and counts it as `export_failed`,
  and its queue fills up meanwhile. A full queue on disk is the signal to make it bigger.
- **No batch processor.** The overlay removes the `batch` processor from the Trailbeam pipeline, because
  it holds spans in memory before the queue, where a crash loses them. The exporter makes batches from
  the queue instead (512 to 2,048 spans, or what has arrived after 1 s).

Size the queue for the longest outage you want to ride out: spans per second × seconds. The default
of 2,000,000 spans is about 33 minutes at 1,000 spans/s and about 2 GB of disk. Watch
`otelcol_exporter_queue_size` against `otelcol_exporter_queue_capacity` on the Collector's own metrics
(`:8888/metrics`); with `sizer: items` both count spans, although their description says batches.

The queue is one per Collector, in `TRAILBEAM_COLLECTOR_QUEUE_DIR`. Two Collectors must not share a
directory. In a container or on Kubernetes, put the directory on a volume that outlives the container
(a `PersistentVolumeClaim` with a StatefulSet, or a `hostPath` with a DaemonSet). Otherwise the queue
only survives an outage of Trailbeam, not a restart of the Collector.

Measured with otelcol-contrib 0.165.0 on one machine (QV-75): Trailbeam spans take about 1.06 KB each on
disk (180k spans: 190 MB). With `fsync` on, the Collector accepted 3,000 spans/s with Trailbeam down at
an export p99 of 89 ms and no errors. With the Collector in front of a Trailbeam server that took in
only about 110 spans/s, a Python application emitting 1,000 spans/s for 2 minutes dropped none of its
120,064 spans; the Collector had sent 13,312 of them on and held the other 106,752 on disk. Sent
directly to the same server (starting from an empty database), the same load dropped 51%.

## Tests

[`tests/test_collector.py`](tests/test_collector.py) starts the real Collector between two fake OTLP
endpoints, sends one trace holding Trailbeam spans, stock SQS spans in the forms botocore, the Node.js AWS SDK
instrumentation and Java emit, and other spans, and checks that:

- Trailbeam endpoint receives exactly the Trailbeam spans;
- with `backend.yaml` and 0% sampling, the backend receives every Trailbeam and stock SQS span and none of
  the others, even after the tail-sampling decision wait;
- with 100% sampling, the backend receives every span;
- with `persistent-queue.yaml`, spans accepted while Trailbeam is down survive a killed Collector and
  all arrive, once each, when a new Collector starts on the same directory and Trailbeam is back.

```sh
pip install pytest opentelemetry-proto
TRAILBEAM_OTELCOL_BIN=/path/to/otelcol-contrib pytest collector/tests
```

Without an `otelcol-contrib` binary the tests are skipped. CI downloads 0.165.0.
