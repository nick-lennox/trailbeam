# Trailbeam (Python)

The `trailbeam` package has two parts: hooks for boto3 SQS clients, and a wrapper for AWS Lambda handlers
that consume SQS. Both emit the spans defined in
[`contract/span-contract.md`](../contract/span-contract.md). Python 3.10 or later. Pre-release, not
published.

## Install

From your application's environment, with a clone of this repository:

```sh
pip install ./trailbeam/python
```

To work on the package itself, run `pip install -e 'python[test]'` from the repository root.

Dependencies: `opentelemetry-api`, `opentelemetry-sdk` and `opentelemetry-exporter-otlp-proto-http`
1.27 or later, `opentelemetry-instrumentation-botocore` 0.48b0 or later, and `botocore` 1.34 or
later.

## Add the hooks to boto3 code

1. Call `install` once per process, before or after you create SQS clients:

   ```python
   import trailbeam.boto
   trailbeam.boto.install()
   ```

2. Set `TRAILBEAM_RECEIPT_HASH_KEY` to the same secret in every service that uses the same queues.
3. Set `TRAILBEAM_OTLP_TRACES_ENDPOINT` to Trailbeam or the Collector, if it is not on
   `http://localhost:4318/v1/traces`.

The hooks cover `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`,
`DeleteMessageBatch`, `ChangeMessageVisibility` and `ChangeMessageVisibilityBatch`. They make no SQS
calls of their own. On `ReceiveMessage` they add the system attributes Trailbeam needs to the request
(contract section 4.2).

`install` works with `opentelemetry-instrumentation-botocore`:

- If your application already called `BotocoreInstrumentor().instrument(...)`, `install` keeps that
  instrumentation and its tracer provider. It runs the Trailbeam hooks first, then your own hooks.
- Otherwise `install` instruments botocore with the Trailbeam tracer provider. The stock botocore spans are
  then exported to `TRAILBEAM_OTLP_TRACES_ENDPOINT` as well. Trailbeam ignores them.

Each Trailbeam span is a child of the stock botocore span for the same call. Each span covers its SQS call:
it starts just before the request is sent and ends when the response arrives, read from
`time.time_ns()` (contract section 2). Each span also carries two ticks of one per-process call-order
counter, `trailbeam.order.issued` and `trailbeam.order.returned`, and `trailbeam.order.scope`, which names the counter
(contract section 2a). Every thread in the process shares the counter. A forked child gets a new
scope.

### `trailbeam.boto` reference

| Call | Effect |
|---|---|
| `install(receipt_hash_key=None, *, span_exporter=None, span_processor=None)` | Installs the hooks and returns the Trailbeam `TracerProvider`. `receipt_hash_key` takes precedence over `TRAILBEAM_RECEIPT_HASH_KEY`. `span_exporter` or `span_processor` replaces the default OTLP/HTTP export. A second call replaces the first installation. |
| `uninstall()` | Removes the hooks, then flushes and shuts down the Trailbeam tracer provider. |
| `trailbeam.export_stats()` | `{"exported", "queued", "dropped", "dropped_by_reason"}` for this process ([Dropped spans](#dropped-spans)). |

## Wrap a Lambda handler

1. Decorate the handler:

   ```python
   import trailbeam.aws_lambda

   @trailbeam.aws_lambda.wrap
   def handler(event, context):
       ...
   ```

2. If the event source mapping does not have `ReportBatchItemFailures` in `FunctionResponseTypes`,
   use `@trailbeam.aws_lambda.wrap(report_batch_item_failures=False)`. Lambda then ignores
   `batchItemFailures`, and so does Trailbeam.
3. Set `TRAILBEAM_RECEIPT_HASH_KEY` and `TRAILBEAM_OTLP_TRACES_ENDPOINT` as for the hooks.

What the wrapper does on each invocation with SQS records:

1. It emits one `trailbeam receive` per record and flushes them, before the handler runs. A timeout or
   out-of-memory kill therefore cannot lose them. The span is zero-length at the moment the wrapper
   got the event, with `trailbeam.start_observed = false`, because Lambda made the `ReceiveMessage` call.
   It carries `trailbeam.order.returned` and no `trailbeam.order.issued`.
2. It calls the handler.
3. It emits one `trailbeam outcome` per record and flushes them. The value is `success`, `failed_reported`,
   `failed_invalid_response` or `failed_exception` (contract section 4.4). For `failed_exception`,
   `trailbeam.error_type` is the exception's class name, never its message. The span carries one tick as
   both `trailbeam.order.issued` and `trailbeam.order.returned`.

A timeout or out-of-memory kill leaves a receive with no outcome. The wrapper cannot detect that from
inside the process. Trailbeam infers it. `BaseException` subclasses that are not `Exception`, such as
`SystemExit`, are not caught and produce no outcome.

Events without SQS records go to the handler unchanged. `messaging.client.id` is
`AWS_LAMBDA_LOG_STREAM_NAME`.

### `trailbeam.aws_lambda.wrap` reference

| Argument | Default | Meaning |
|---|---|---|
| `receipt_hash_key` | `None` | Takes precedence over `TRAILBEAM_RECEIPT_HASH_KEY`. |
| `span_exporter`, `span_processor` | `None` | Replace the default OTLP/HTTP export. |
| `flush_timeout_millis` | `2000` | Longest wait for each of the two flushes, in milliseconds. |
| `report_batch_item_failures` | `True` | Match the event source mapping's `FunctionResponseTypes`. |

The wrapped function has a `trailbeam_tracer_provider` attribute.

## Configuration

| Setting | Default |
|---|---|
| `TRAILBEAM_RECEIPT_HASH_KEY`, or the `receipt_hash_key` argument | The published default key `trailbeam-default-receipt-hash-key`, with one warning per process. A key shorter than 16 bytes is used, with one warning. |
| `TRAILBEAM_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` |
| `OTEL_SERVICE_NAME` | `service.name` = `unknown_service` |
| `OTEL_RESOURCE_ATTRIBUTES` | `service.instance.id`, which becomes `messaging.client.id`. Default: a random UUID v4 per tracer provider. |
| `TRAILBEAM_MAX_QUEUE_SIZE` | `16384` spans waiting for export, per tracer provider ([Dropped spans](#dropped-spans)). The other `OTEL_BSP_*` variables apply as usual. |

### Dropped spans

Spans wait in an in-memory queue until they are exported, in batches of 512. If Trailbeam (or the
Collector) is down or slower than your traffic, the queue fills up, and from then on new spans are
dropped. The messages they belong to then have gaps in their timelines. Drops are never silent:

- **Log line.** The `trailbeam` logger warns at the first drop, then at most once a minute with the
  number dropped since the last line and the reason, and once more at shutdown:
  `trailbeam: dropped 800 spans in the last 60 s (800 queue full); 812 dropped since start. ...`
  The count is in the message, so the line can't be lost to duplicate filtering.
- **Metric.** `trailbeam.sdk.spans.dropped` (unit `{span}`), a counter on the global OpenTelemetry
  meter provider, with `error.type` = `queue_full`, `export_failed` or `shutdown`. It is a no-op unless
  your application sets a meter provider. Alert on it.
- **In code.** `trailbeam.export_stats()` returns `{"exported", "queued", "dropped", "dropped_by_reason"}`
  for this process.

`queue_full` means the queue was full. `export_failed` means an export failed after the OTLP exporter's
own retries (the batch is not tried again). `shutdown` means spans that were still queued when the
provider shut down, or that ended after it shut down. Spans of a `span_processor` you pass in are not
counted.

The queue holds `TRAILBEAM_MAX_QUEUE_SIZE` spans, 16,384 by default (the SDK's default is 2,048). That is
about 16 seconds of 1,000 spans/s, and about 50 MB of memory when full, more with captured bodies. A
bigger queue only rides out a longer outage. For high volume, or to keep spans through a restart, send
them through an OpenTelemetry Collector with a queue on disk
([High volume](../collector/README.md#high-volume)).


### Message contents (opt-in)

Contract section 15. Body size is always recorded. The rest is off until you turn it on:

```python
trailbeam.boto.install(
    tenant={"message_attribute": "tenant", "body_path": "customer.id"},  # or TRAILBEAM_TENANT_ATTRIBUTE / _BODY_PATH
    capture={
        "body_queues": ["orders"],        # bodies of these queues, redacted, or "*"  (TRAILBEAM_CAPTURE_BODY_QUEUES)
        "error_queues": ["orders"],       # Lambda exception messages, redacted       (TRAILBEAM_CAPTURE_ERROR_QUEUES)
        "redact_keys": ["customer.name"], # added to the default keys                 (TRAILBEAM_REDACT_KEYS)
        "max_body_bytes": 16384,          #                                           (TRAILBEAM_CAPTURE_MAX_BYTES)
    },
)
```

`trailbeam.aws_lambda.wrap(tenant=..., capture=...)` takes the same options. Redaction runs in this
process before export. A body that fails redaction, fails the MD5 check or is over the limit is not
exported, and the span says why in `trailbeam.body.capture_status`. The package never makes an SQS
call of its own. A self-hosted Trailbeam stores the body only if the queue is also in its own
`TRAILBEAM_STORE_BODIES` ([Message bodies](../README.md#message-bodies)).

## Tests

```sh
cd python && pytest -q
```

The hook tests use a local SQS emulator and never AWS: they remove every `AWS_*` variable and set
dummy credentials. By default they start moto's server in-process. To use ElasticMQ or LocalStack,
set `TRAILBEAM_TEST_SQS_ENDPOINT`, for example to `http://localhost:9324`.

The Postgres store's tests, and the storage conformance suite on Postgres, run when
`TRAILBEAM_TEST_POSTGRES_URL` names a database the tests may create schemas in, for example
`postgresql://postgres:postgres@localhost:5432/postgres` (`pip install -e ".[test,postgres]"`).
