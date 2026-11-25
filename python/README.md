# Trailbeam (Python)

The `trailbeam` package has three parts: hooks for boto3 SQS clients, a wrapper for AWS Lambda handlers
that consume SQS, and hooks for pika, the RabbitMQ client. The SQS parts emit the spans defined in
[`contract/span-contract.md`](../contract/span-contract.md), the pika hooks those of
[`contract/span-contract-amqp.md`](../contract/span-contract-amqp.md). Python 3.10 or later.

## Install

```sh
pip install trailbeam
```

To install from a clone instead: `pip install ./trailbeam/python`.

To work on the package itself, run `pip install -e 'python[test]'` from the repository root.

Dependencies: `opentelemetry-api`, `opentelemetry-sdk` and `opentelemetry-exporter-otlp-proto-http`
1.28 or later, `opentelemetry-instrumentation-botocore` 0.49b0 or later, and `botocore` 1.34 or
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
| `install(receipt_hash_key=None, *, span_exporter=None, span_processor=None, tenant=None, capture=None, sns=None)` | Installs the hooks and returns the Trailbeam `TracerProvider`. `receipt_hash_key` takes precedence over `TRAILBEAM_RECEIPT_HASH_KEY`. `span_exporter` or `span_processor` replaces the default OTLP/HTTP export. A second call replaces the first installation. |
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

## Add the hooks to pika (RabbitMQ)

```sh
pip install "trailbeam[pika]"
```

Call `install` once per process, before you open connections:

```python
import trailbeam.pika
trailbeam.pika.install()
```

The hooks sit on `pika.channel.Channel`, which every pika connection type uses (`BlockingConnection`, pika 1.5's
thread-safe `Connection`, `SelectConnection`, and the asyncio, Tornado and Twisted adapters). pika 1.3 or later.
They emit the spans of contract 4.0 (schema version 4):

| What the application or the broker does | Span |
|---|---|
| `basic_publish`; with `confirm_delivery`, the span ends at the broker's confirm (or its nack, or the channel closing first) | `trailbeam publish` |
| the broker returns a `mandatory` publish no queue took (`basic.return`) | `trailbeam return` |
| a delivery to a `basic_consume` consumer, before your callback runs; a `basic_get` that returned a message | `trailbeam deliver` |
| `basic_ack`, `basic_nack`, `basic_reject`: one span per delivery covered (`multiple=True` covers several) | `trailbeam ack`, `trailbeam nack`, `trailbeam reject` |
| `basic_cancel`, or the broker cancelling a consumer (its queue was deleted, or a consumer timeout) | `trailbeam cancel` |
| the channel or its connection closes, or the connection is lost, with deliveries not settled: one span per delivery | `trailbeam channel_closed` |

The hooks make no broker calls of their own and don't change messages. The receive count comes from the quorum
queue's `x-acquired-count` header (a classic queue's redelivery has none), the dead-letter move from `x-death`.
A message is identified by its `message_id` property: a publish without one is counted but gets no timeline, unless
you turn on `set_message_id`. RabbitMQ streams are not covered.

Limits: a channel opened before `install` is not followed, and a consumer of the server-named queue `""` gets no
spans (pass the queue's name). With `auto_ack=True` a delivery is settled by the broker as it is sent, so it gets a
`trailbeam deliver` and nothing more.

### `trailbeam.pika` reference

| Call | Effect |
|---|---|
| `install(*, span_exporter=None, span_processor=None, tenant=None, capture=None, set_message_id=None)` | Installs the hooks and returns the Trailbeam `TracerProvider`. `tenant` and `capture` are as for `trailbeam.boto` ([Message contents](#message-contents-opt-in)): the tenant comes from the message header `tenant["message_attribute"]` names, then the body path; `body_queues` lists queues, and exchanges for publishes. Only text bodies are captured (`not_text` otherwise). `set_message_id=True`, or `TRAILBEAM_AMQP_SET_MESSAGE_ID=on`, gives a publish without a `message_id` a random UUID (on a copy of its properties), marked `trailbeam.amqp.message_id.source = package`. A second call replaces the first installation. |
| `uninstall()` | Removes the hooks, then flushes and shuts down the Trailbeam tracer provider. |

## Configuration

| Setting | Default |
|---|---|
| `TRAILBEAM_RECEIPT_HASH_KEY`, or the `receipt_hash_key` argument | The published default key `trailbeam-default-receipt-hash-key`, with one warning per process. A key shorter than 16 bytes is used, with one warning. |
| `TRAILBEAM_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` |
| `OTEL_SERVICE_NAME` | `service.name` = `unknown_service` |
| `OTEL_RESOURCE_ATTRIBUTES` | `service.instance.id`, which becomes `messaging.client.id`. Default: a random UUID v4 per tracer provider. |
| `OTEL_RESOURCE_ATTRIBUTES=service.version=1.4.2` | No version. With it, Trailbeam shows each consumer's version on the timeline and on the lists, which can be filtered by `version`, so you can see whether a deploy changed things. |
| `TRAILBEAM_MAX_QUEUE_SIZE` | `16384` spans waiting for export, per tracer provider ([Dropped spans](#dropped-spans)). The other `OTEL_BSP_*` variables apply as usual. |
| `TRAILBEAM_SHUTDOWN_TIMEOUT_MS` | `2000`: the longest that shutdown (`uninstall()`, or interpreter exit) waits for queued spans to be exported ([When the server is down, or the process ends](#when-the-server-is-down-or-the-process-ends)). |

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
own retries (the batch is not tried again): the server was unreachable, too slow, or refused the spans.
`shutdown` means spans that were still queued when shutdown gave up (after `TRAILBEAM_SHUTDOWN_TIMEOUT_MS`),
or that ended after it shut down. Spans of a `span_processor` you pass in are not
counted.

### When the server is down, or the process ends

Spans are exported from a background thread, so a down, slow or misconfigured Trailbeam server never
raises in your code and never slows your SQS calls. At the end of the process, Trailbeam waits at most
`TRAILBEAM_SHUTDOWN_TIMEOUT_MS` (2 s) for the queue to be exported, so a down or slow server delays your
exit by at most that much. In each case:

| Case | What happens to the spans |
|---|---|
| A short script that ends normally (falls off the end, or calls `sys.exit()`) | Exported when the interpreter exits. No `uninstall()` call is needed. |
| `os._exit()`, a signal with no handler (`SIGTERM`, `SIGKILL`), or a crash of the interpreter | Spans still queued are lost without a log line: Python skips its exit hooks then. At most the last 5 s of spans (`OTEL_BSP_SCHEDULE_DELAY`) are queued. Call `trailbeam.boto.uninstall()` in your `SIGTERM` handler, or let your server framework turn `SIGTERM` into a normal exit (Gunicorn and Uvicorn do). |
| The server is unreachable from the start | Your code runs as normal. Each batch fails after the exporter's retries (up to 10 s, in the background) and is dropped as `export_failed`; at exit, what is still queued is dropped as `shutdown` after 2 s. Both are counted and logged. |
| The server goes away mid-run | Spans exported before that are kept. Later batches are dropped as `export_failed`, and at exit what is left as `shutdown`, as above. Once the server is back, new spans are exported again; dropped ones are not resent. |
| A wrong or missing token | The server answers `401` and the batch is dropped as `export_failed` at once (no retry). The OTLP exporter logs `Failed to export spans batch code: 401`, and the Trailbeam line names `OTEL_EXPORTER_OTLP_HEADERS`. |
| A slow server | Your SQS calls are not slowed. A request that takes longer than the exporter's timeout (10 s, `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT`) is dropped as `export_failed`. At exit, what has not been exported after 2 s is dropped as `shutdown`. |
| More spans than the server takes | The queue fills up and new spans are dropped as `queue_full` ([Dropped spans](#dropped-spans)). |

The Lambda wrapper's flushes keep to `flush_timeout_millis` in the same way.

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

### SNS fan-out

Contract section 21. `install` also hooks boto3 `sns` clients: each `publish` and `publish_batch` to a topic
records a `trailbeam publish` span with the SNS message ID, and a receive of the message on a subscribed queue is
joined to it. Two ways the join works:

- **Raw delivery on**: the hooks add one message attribute, `trailbeam.publish_id`, to each published message, on a
  copy of your request. They skip a message that already has 10 attributes (with raw delivery on, SNS would drop it),
  would pass 256 KiB, or uses `MessageStructure="json"`, and the span says why in `trailbeam.sns.publish_id_skipped`.
- **Raw delivery off**: the SQS body is SNS's envelope. On every queue, the receive reads three of its fields:
  `MessageId`, `TopicArn` and `Timestamp`. It does this even when body capture is off for the queue. It reads
  nothing else in the body: not `Message`, `Subject` or `MessageAttributes`, and the body itself is never
  exported. No option is needed.

```python
trailbeam.boto.install(
    sns={"inject": False},  # don't add trailbeam.publish_id to publishes (or TRAILBEAM_SNS_INJECT=off)
)
```

`trailbeam.aws_lambda.wrap(sns=...)` takes the same options for its receives.


### EventBridge PutEvents

Contract section 25. `install()` also hooks boto3 `events` clients: each `PutEvents` call records one
`trailbeam put_event` span per entry, in entry order, whether EventBridge accepted the entry, failed it
(`FailedEntryCount`), or the whole call failed (throttled, for example). No option is needed, and the request is never
changed.

- **Recorded from your call**: the event bus (`EventBusName` as given, and its name; `default` when none), the entry's
  index, whether it was accepted (`ok`) or `failed` with its error code (`ErrorCode`, or the call's error), the event ID
  EventBridge returned, the entry's `Source` and `DetailType` (1 to 256 characters each), whether you set `Time`, and
  the size of `Detail` in bytes.
- **Never recorded**: `Detail` itself (the event's payload), `Resources`, `TraceHeader`, the value of `Time`, the error
  message EventBridge returned, and `EndpointId`.

Trailbeam joins the entry to the SQS messages its rules delivered, by the event's `id`, which every receive already
records (next section). The message's timeline then starts with "Put on EventBridge bus *name*", observed, with the
process that called `PutEvents`, the other SQS messages the event reached, and the EventBridge delivery time (SQS's
`SentTimestamp` minus the event's `time`). If you set `Time` on the entry, that time is yours, not EventBridge's, and the
timeline says so.

### S3 uploads

Contract section 27. `install()` also hooks boto3 `s3` clients: each `PutObject`, `CompleteMultipartUpload` or
`CopyObject` call records one `trailbeam s3_upload` span, ok or failed. The request is never changed.

- **Recorded from your call**: the bucket, the operation, whether it succeeded (`ok`) or `failed` with its error code, the
  returned `ETag` and `VersionId`, and, only when body capture is on for the bucket (its name or `*` in `capture={"body_queues": [...]}`), the object key,
  redacted like a body.
- **Never recorded**: the object's contents, its user metadata (`Metadata`), `CopySource`, tags, checksums, encryption
  keys, and the response's `Location`.

Trailbeam joins the upload to the SQS messages its bucket notification sent, by bucket, object key and event time. A
receive records the key only on a queue whose body is captured (next section), so the join needs body capture on both
sides: for the bucket in the uploading process, and for the queue in the receiving one. Where either is off, the
message keeps its inferred "S3 *event*" step. Joined, the timeline starts with "Uploaded to S3 bucket *name*",
observed, with the process that uploaded, the other SQS messages the notification reached, and the S3 delivery time
(SQS's `SentTimestamp` minus the record's `eventTime`).

```python
trailbeam.boto.install(capture={"body_queues": ["photos", "photo-uploads"]})  # the bucket, and the queue
```

### EventBridge and S3 notifications

Contract section 23. A message that an EventBridge rule or an S3 bucket notification sent has no send in your code. When
its body is an EventBridge event, or an S3 event notification (or S3's `s3:TestEvent` message), the receive records what
the event says about where it came from, on every queue: the event's `id`, `source`, `detail-type`, account, region and
time, or the S3 event name, bucket, region, time, notification configuration ID and `sequencer`. Nothing else in the body
is read or exported. The S3 object key is recorded only on a queue whose body is captured (`capture={"body_queues": [...]}`), redacted like the
body. Trailbeam shows these as an inferred first step, "Arrived via an EventBridge rule" or "S3 *event*", unless a
recorded `PutEvents` entry joins the message (previous section). There is no option; `trailbeam.aws_lambda.wrap` reads
the same.

## Tests

```sh
cd python && pytest -q
```

The hook tests use a local SQS emulator and never AWS: they remove every `AWS_*` variable and set
dummy credentials. By default they start moto's server in-process. To use ElasticMQ or LocalStack,
set `TRAILBEAM_TEST_SQS_ENDPOINT`, for example to `http://localhost:9324`.

The pika hooks' tests against the contract's vectors need no broker. Their tests against RabbitMQ
(`tests/test_pika_rabbitmq.py`) run when `TRAILBEAM_TEST_AMQP_URL` names one, for example
`docker run -d -p 127.0.0.1:5672:5672 rabbitmq:4` and `amqp://guest:guest@127.0.0.1:5672/%2F`. Each test declares
queues of its own and deletes them.

The Postgres store's tests, and the storage conformance suite on Postgres, run when
`TRAILBEAM_TEST_POSTGRES_URL` names a database the tests may create schemas in, for example
`postgresql://postgres:postgres@localhost:5432/postgres` (`pip install -e ".[test,postgres]"`).
