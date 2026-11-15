# Trailbeam (Python)

The `trailbeam` package has two parts: hooks for boto3 SQS clients, and a wrapper for AWS Lambda handlers
that consume SQS. Both emit the spans defined in
[`docs/span-contract.md`](../docs/span-contract.md). Python 3.10 or later. Pre-release, not
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
3. Set `TRAILBEAM_OTLP_TRACES_ENDPOINT` to the store or the Collector, if it is not on
   `http://localhost:4318/v1/traces`.

The hooks cover `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`,
`DeleteMessageBatch`, `ChangeMessageVisibility` and `ChangeMessageVisibilityBatch`. They make no SQS
calls of their own. On `ReceiveMessage` they add the system attributes Trailbeam needs to the request
(contract section 4.2).

`install` works with `opentelemetry-instrumentation-botocore`:

- If your application already called `BotocoreInstrumentor().instrument(...)`, `install` keeps that
  instrumentation and its tracer provider. It runs the Trailbeam hooks first, then your own hooks.
- Otherwise `install` instruments botocore with the Trailbeam tracer provider. The stock botocore spans are
  then exported to `TRAILBEAM_OTLP_TRACES_ENDPOINT` as well. The store ignores them.

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
inside the process. The store infers it. `BaseException` subclasses that are not `Exception`, such as
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

## Tests

```sh
cd python && pytest -q
```

The hook tests use a local SQS emulator and never AWS: they remove every `AWS_*` variable and set
dummy credentials. By default they start moto's server in-process. To use ElasticMQ or LocalStack,
set `TRAILBEAM_TEST_SQS_ENDPOINT`, for example to `http://localhost:9324`.
