# @qv2/sdk (Node.js)

OpenTelemetry hooks for AWS SDK v3 SQS clients. They emit the spans defined in
[`docs/span-contract.md`](../docs/span-contract.md): `qv2 send`, `qv2 receive`, `qv2 delete` and
`qv2 change_visibility`, one per message, with a keyed hash of each receipt handle and never the
handle itself or the message body.

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
  `ApproximateFirstReceiveTimestamp` and `AWSTraceHeader` (on a copy of the input; nothing the
  caller asked for is removed, and no message attributes are requested);
- emits spans after each response, so settle spans carry `qv2.result` (`ok` / `failed`) and
  `qv2.error_code`; a settle call that throws still gets one `failed` span per receipt handle;
- never changes the result or error the caller sees.

If the application uses `@opentelemetry/instrumentation-aws-sdk`, each qv2 span is a child of the
stock span for the same call and shares its trace ID. Without it, qv2 spans are children of the
active span, if any. qv2 uses its own always-on tracer provider, so the application's sampling
never drops qv2 spans, and it does not register anything globally. For parent spans to be found,
the application needs an OpenTelemetry context manager (any `NodeSDK` setup registers one).

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
[`docs/span-contract-vectors.json`](../docs/span-contract-vectors.json), which the Python tests
check too.
