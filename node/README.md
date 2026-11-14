# @qv2/sdk (Node.js)

OpenTelemetry hooks for AWS SDK v3 SQS clients, and a wrapper for Lambda handlers that consume
SQS. They emit the spans defined in [`docs/span-contract.md`](../docs/span-contract.md):
`qv2 send`, `qv2 receive`, `qv2 delete`, `qv2 change_visibility` and `qv2 outcome`, one per
message, with a keyed hash of each receipt handle and never the handle itself or the message body.

`"private": true` stays in `package.json` until a release decision; 0.1 publishes nothing to npm.

## Install from the repository

Plain CommonJS with type declarations; there is no build step. Node.js 20 or later.

```sh
git clone https://github.com/nick-lennox/qv2
npm install ./qv2/node          # from your application's directory
```

or `npm pack ./qv2/node` and install the tarball.

## Client hooks

```js
const { SQSClient } = require('@aws-sdk/client-sqs');
const qv2 = require('@qv2/sdk');

qv2.install({ receiptHashKey: process.env.QV2_RECEIPT_HASH_KEY }); // optional; defaults shown below
const sqs = qv2.instrument(new SQSClient({}));
```

`instrument` adds a middleware to that client. It:

- asks `ReceiveMessage` for the system attributes `ApproximateReceiveCount`, `SentTimestamp`,
  `ApproximateFirstReceiveTimestamp`, `AWSTraceHeader` and `DeadLetterQueueSourceArn` (on a copy of the input; nothing the
  caller asked for is removed, and no message attributes are requested);
- emits spans after each response, so settle spans carry `qv2.result` (`ok` / `failed`) and
  `qv2.error_code`; a settle call that throws still gets one `failed` span per receipt handle;
- gives each span the call's interval: it starts just before the request is sent and ends when the
  response arrives (contract v2, section 2), read from `Date.now()`;
- gives each span the call's order inside the process (schema version 3): `qv2.order.issued` and
  `qv2.order.returned` are ticks of one counter shared by every instrumented client, taken when the
  request was sent and when the response (or error) arrived, with `qv2.order.scope` naming the
  counter (each worker thread has its own). A call whose `returned` is lower than another's
  `issued`, in the same scope, returned before the other was sent;
- never changes the result or error the caller sees.

If the application uses `@opentelemetry/instrumentation-aws-sdk`, each qv2 span is a child of the
stock span for the same call and shares its trace ID. Without it, qv2 spans are children of the
active span, if any. qv2 uses its own always-on tracer provider, so the application's sampling
never drops qv2 spans, and it does not register anything globally. For parent spans to be found,
the application needs an OpenTelemetry context manager (any `NodeSDK` setup registers one).

## Lambda wrapper

```js
const qv2 = require('@qv2/sdk');

exports.handler = qv2.wrapHandler(async (event, context) => {
  // ...
  return { batchItemFailures: [] };
});
```

For each SQS record, the wrapper emits a `qv2 receive` and exports it **before** your handler
runs, so a timeout or out-of-memory kill cannot lose it. Lambda made the `ReceiveMessage` call, not
the wrapper, so that span only says the receive happened no later than the handler start: it is
zero-length and marked `qv2.start_observed = false`, and it carries `qv2.order.returned` but no
`qv2.order.issued`. After the handler returns or throws, it emits one `qv2 outcome` per record,
with `qv2.order.issued` = `qv2.order.returned` (one tick, after the handler):

| `qv2.outcome` | When |
|---|---|
| `success` | the handler returned and the record is not in `batchItemFailures` |
| `failed_reported` | the record's ID is in `batchItemFailures` |
| `failed_invalid_response` | `batchItemFailures` has an entry that is not a record ID in the batch (Lambda then fails the whole batch) |
| `failed_exception` | the handler threw; `qv2.error_type` is the error's class name, never its message |

A timeout or out-of-memory kill gets no outcome; the store infers it from the receive. Pass
`reportBatchItemFailures: false` if the event source mapping does not have `ReportBatchItemFailures`
turned on, since Lambda then ignores `batchItemFailures`. Async, sync and callback-style handlers
are accepted; the wrapper is always an async handler. Events without SQS records pass straight
through. `messaging.client.id` is `AWS_LAMBDA_LOG_STREAM_NAME`, which names one execution
environment. Each flush waits at most `flushTimeoutMs` (default 2000), so an unreachable collector
cannot use up the function's time.

## Configuration

| Variable | Default | |
|---|---|---|
| `QV2_RECEIPT_HASH_KEY` | `qv2-default-receipt-hash-key`, with one warning | The same key on every service that uses the same queues. A secret: never commit it. |
| `QV2_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` | OTLP/HTTP (protobuf). |
| `OTEL_SERVICE_NAME` | `unknown_service` | `service.name` |
| `OTEL_RESOURCE_ATTRIBUTES` | | `service.instance.id` here becomes `messaging.client.id`; otherwise a random UUID per process. |

Call `await qv2.forceFlush()` before a short-lived process exits, or `await qv2.uninstall()`.

## Tests

```sh
npm install
npm test                                            # unit tests, no network
QV2_TEST_SQS_ENDPOINT=http://localhost:9324 npm test  # plus end to end against ElasticMQ
```

The receipt-hash tests use the shared vectors in
[`docs/span-contract-vectors.json`](../docs/span-contract-vectors.json), and the wrapper tests replay
the recorded QV-9 Lambda events in [`python/tests/fixtures`](../python/tests/fixtures); the Python
tests use both too.
