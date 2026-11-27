# trailbeam (Node.js)

OpenTelemetry hooks for AWS SDK v3 SQS clients, and a wrapper for Lambda handlers that consume
SQS. They emit the spans defined in [`contract/span-contract.md`](../contract/span-contract.md):
`trailbeam send`, `trailbeam receive`, `trailbeam delete`, `trailbeam change_visibility` and `trailbeam outcome`, one per
message, with a keyed hash of each receipt handle and never the handle itself or the message body.

## Install

Plain CommonJS with type declarations; there is no build step. Node.js 20 or later.
AWS SDK v3 `@aws-sdk/client-sqs` 3.572.0 or later: older clients drop `MessageSystemAttributeNames`, so
the timeline would have no send time, receive count or dead-letter source ([supported versions](../README.md#supported-versions)).

```sh
npm install trailbeam
```

To install from a clone instead, run `npm install ./trailbeam/node` from your application's directory.

## Run the server: `npx trailbeam start`

Needs version 0.4.0 or later of this package: 0.3.0 has no `trailbeam` command. The Python equivalent is
[`trailbeam start`](../README.md#trailbeam-start-a-token-and-key-made-for-you); for a team server, see the
[team install](../README.md#team-install-docker-compose-with-postgres).

To try Trailbeam from a Node.js project you don't need Python: with [Docker](https://docs.docker.com/get-docker/)
installed and running,

```sh
npx trailbeam start
```

runs the Trailbeam server from its container image, `ghcr.io/nick-lennox/trailbeam` with the tag of this
package's version, and keeps its data in the Docker volume `trailbeam-data`. The first run creates an access
token and a receipt-hash key there. It then prints the web screen's address (`http://127.0.0.1:4318/`), the
token, and the environment variables to set in your services, the same output as the Python
`trailbeam start`. Later runs reuse the token and key, and don't print them: they print the command that
shows each one, `docker run --rm -v trailbeam-data:/data --entrypoint cat ghcr.io/nick-lennox/trailbeam:X.Y.Z
/data/token` (and `/data/receipt-hash-key`), and the export lines run it with `$(...)`. If `TRAILBEAM_RECEIPT_HASH_KEY` is set, that key is used instead of
the one in the volume, and the export lines say `$TRAILBEAM_RECEIPT_HASH_KEY`. Stop it with Ctrl-C.

Before it starts the server it prints one line to stderr naming the image it runs: the reference, the local
image ID and the registry digest, for example `trailbeam: running ghcr.io/nick-lennox/trailbeam:0.3.0 (image
sha256:…, ghcr.io/nick-lennox/trailbeam@sha256:…)`. It pulls the image only when it isn't on this machine.
`TRAILBEAM_IMAGE=REF` runs another image instead (a local build, a mirror), and the line then says so.

It takes the same flags as the Python command (`npx trailbeam start --help` lists them), and the same
`TRAILBEAM_` environment variables. The flags that name something on your machine work like this:

- `--host` and `--port`: the address and port Docker publishes (default `127.0.0.1:4318`). Use
  `--host 0.0.0.0` to let other machines reach it.
- `--data-dir DIR`: keep the data in that directory instead of the volume. The container then runs as
  you, so the files are yours, readable only by you. `--db FILE` does the same with the SQLite file (with the
  token and key in `~/.trailbeam`, as in the Python command, unless `--data-dir` is set).
- `--config FILE` is mounted read-only. `--token-file FILE` is read by `npx` and given to the server as
  `TRAILBEAM_TOKEN`.
- `--store-url` is not available: the image has SQLite only. For Postgres, use the
  [team install](../README.md#team-install-docker-compose-with-postgres).

If Docker is missing, not running, or this user can't use it, it says so in one line and exits.

The container has the label `trailbeam.start=1` (`docker ps --filter label=trailbeam.start` lists them). If
the process that started `npx trailbeam start` goes away (a supervisor that stops only the `npx` process,
which npm doesn't pass on), the server is stopped too, within about a second. If the port is already in use,
it says so; when a Docker container publishes the port, it names the container (saying when it is a
Trailbeam server from an earlier run) and gives the `docker stop` command.
Nothing of this is loaded by `require('trailbeam')`, and it adds no dependencies.

## Client hooks

```js
const { SQSClient } = require('@aws-sdk/client-sqs');
const trailbeam = require('trailbeam');

trailbeam.install({ receiptHashKey: process.env.TRAILBEAM_RECEIPT_HASH_KEY }); // optional; defaults shown below
const sqs = trailbeam.instrument(new SQSClient({}));
```

`instrument` adds a middleware to that client. It:

- asks `ReceiveMessage` for the system attributes `ApproximateReceiveCount`, `SentTimestamp`,
  `ApproximateFirstReceiveTimestamp`, `AWSTraceHeader` and `DeadLetterQueueSourceArn` (on a copy of the input; nothing the
  caller asked for is removed, and no message attributes are requested);
- emits spans after each response, so settle spans carry `trailbeam.result` (`ok` / `failed`) and
  `trailbeam.error_code`; a settle call that throws still gets one `failed` span per receipt handle;
- gives each span the call's interval: it starts just before the request is sent and ends when the
  response arrives (contract section 2), read from `Date.now()` in whole milliseconds;
- gives each span the call's order inside the process (schema version 3): `trailbeam.order.issued` and
  `trailbeam.order.returned` are ticks of one counter shared by every instrumented client, taken when the
  request was sent and when the response (or error) arrived, with `trailbeam.order.scope` naming the
  counter (each worker thread has its own). A call whose `returned` is lower than another's
  `issued`, in the same scope, returned before the other was sent. The counter is shared by every
  async task in the realm, so calls from different tasks can be ordered too; why that is sound is in
  [contract section 2a](../contract/span-contract.md#2a-call-order-inside-one-process-schema-version-3);
- never changes the result or error the caller sees.

If the application uses `@opentelemetry/instrumentation-aws-sdk`, each Trailbeam span is a child of the
stock span for the same call and shares its trace ID. Without it, Trailbeam spans are children of the
active span, if any. Trailbeam uses its own always-on tracer provider, so the application's sampling
never drops Trailbeam spans, and it does not register anything globally. For parent spans to be found,
the application needs an OpenTelemetry context manager (any `NodeSDK` setup registers one).

When the event loop empties, the package exports the spans still queued, so a script that ends on
its own loses nothing. `process.exit()` and signals skip that step: call `await trailbeam.uninstall()`
before `process.exit()`. Spans still queued at exit are lost, and the package prints how many to stderr.

## Lambda wrapper

```js
const trailbeam = require('trailbeam');

exports.handler = trailbeam.wrapHandler(async (event, context) => {
  // ...
  return { batchItemFailures: [] };
});
```

For each SQS record, the wrapper emits a `trailbeam receive` and exports it **before** your handler
runs, so a timeout or out-of-memory kill cannot lose it. Lambda made the `ReceiveMessage` call, not
the wrapper, so that span only says the receive happened no later than the handler start: it is
zero-length and marked `trailbeam.start_observed = false`, and it carries `trailbeam.order.returned` but no
`trailbeam.order.issued`. After the handler returns or throws, it emits one `trailbeam outcome` per record,
with `trailbeam.order.issued` = `trailbeam.order.returned` (one tick, after the handler):

| `trailbeam.outcome` | When |
|---|---|
| `success` | the handler returned and the record is not in `batchItemFailures` |
| `failed_reported` | the record's ID is in `batchItemFailures` |
| `failed_invalid_response` | `batchItemFailures` has an entry that is not a record ID in the batch (Lambda then fails the whole batch) |
| `failed_exception` | the handler threw; `trailbeam.error_type` is the error's class name, never its message |

A timeout or out-of-memory kill gets no outcome; Trailbeam infers it from the receive. Pass
`reportBatchItemFailures: false` if the event source mapping does not have `ReportBatchItemFailures`
turned on, since Lambda then ignores `batchItemFailures`. Async, sync and callback-style handlers
are accepted; the wrapper is always an async handler. Events without SQS records pass straight
through. `messaging.client.id` is `AWS_LAMBDA_LOG_STREAM_NAME`, which names one execution
environment, or the wrapper's `service.instance.id` if that variable is not set. Each flush waits at most `flushTimeoutMs` (default 2000), so an unreachable collector
cannot use up the function's time.

## Add the hooks to amqplib (RabbitMQ)

```sh
npm install trailbeam amqplib
```

Call `install` once per process, before you open connections:

```js
const trailbeam = require('trailbeam');
trailbeam.amqplib.install();
```

The hooks sit on amqplib's channel classes, which both its promise API (`require('amqplib')`) and its callback API
(`require('amqplib/callback_api')`) use, with or without connection recovery. amqplib 0.10 or later is an optional
peer dependency: the package loads it only when `trailbeam.amqplib.install()` is called. They emit the spans of
contract 4.1 (schema version 4), the same spans as the Python package's pika hooks:

| What the application or the broker does | Span |
|---|---|
| `publish`, `sendToQueue`; on a confirm channel, the span ends at the broker's confirm (or its nack, or the channel closing first) | `trailbeam publish` |
| the broker returns a `mandatory` publish no queue took (`basic.return`) | `trailbeam return` |
| a delivery to a `consume` consumer, before your callback runs; a `get` that returned a message | `trailbeam deliver` |
| `ack`, `ackAll`, `nack`, `nackAll`, `reject`: one span per delivery covered (`allUpTo` covers several) | `trailbeam ack`, `trailbeam nack`, `trailbeam reject` |
| `cancel`, or the broker cancelling a consumer (its queue was deleted, or a consumer timeout) | `trailbeam cancel` |
| the channel or its connection closes, or the connection is lost, with deliveries not settled: one span per delivery | `trailbeam channel_closed` |

The hooks make no broker calls of their own and don't change messages; your confirm callbacks, `waitForConfirms`
and `return` listeners work as before. The receive count comes from the quorum queue's `x-acquired-count` header (a
classic queue's redelivery has none), the dead-letter move from `x-death`. A message is identified by its `messageId`:
a publish without one is counted but gets no timeline, unless you turn on `setMessageId`. RabbitMQ streams are not
covered.

Limits: a consumer of the server-named queue `''` gets no spans (pass the queue's name). With `noAck: true` a delivery
is settled by the broker as it is sent, so it gets a `trailbeam deliver` and nothing more. `server.address` is the
host the socket was opened to; amqplib keeps no copy of the URL. The Node.js package does not set `thread.name`.
amqplib 0.10.5 and older can't connect to RabbitMQ 4.1 or later at all (the broker refuses their `frame_max` of 4096),
so with a current broker use 0.10.6 or later; CI tests 0.10.9 and the newest.

| Call | Effect |
|---|---|
| `amqplib.install({ spanExporter, spanProcessor, tenant, capture, setMessageId, receiptHashKey, exportRoutingKeys, amqplibDir })` | Installs the hooks and returns their `BasicTracerProvider`. `tenant` and `capture` are as for `install` ([Message contents](#message-contents-opt-in)): the tenant comes from the message header `tenant.messageAttribute` names, then the body path; `bodyQueues` lists queues, and exchanges for publishes. Only text bodies are captured (`not_text` otherwise). `setMessageId: true`, or `TRAILBEAM_AMQP_SET_MESSAGE_ID=on`, gives a publish without a `messageId` a random UUID (on a copy of its options), marked `trailbeam.amqp.message_id.source = package`. `receiptHashKey` takes precedence over `TRAILBEAM_RECEIPT_HASH_KEY` for the routing-key hash. `exportRoutingKeys: true`, or `TRAILBEAM_AMQP_EXPORT_ROUTING_KEYS=on`, exports routing keys verbatim ([What the RabbitMQ hooks export](#what-the-rabbitmq-hooks-export)). `amqplibDir`: the amqplib folder to hook, if `require('amqplib')` from the package or the working directory finds another copy. A second call replaces the first installation. |
| `amqplib.uninstall()` | Removes the hooks, then flushes and shuts down their provider. Returns a promise. |
| `amqplib.forceFlush()` | Exports the spans that are waiting. Returns a promise. |

### What the RabbitMQ hooks export

Every field the hooks put on a span (contract [A16](../contract/span-contract-amqp.md#a16-what-the-rabbitmq-hooks-export)).
Nothing else leaves the process.

| Field | Exported |
|---|---|
| Queue and exchange names, vhost, broker host and port, channel and consumer tags, delivery tag, `redelivered`, `mandatory`, `deliveryMode` (or `persistent`), confirm mode | always |
| Routing key | **as a keyed hash** (`trailbeam.amqp.routing_key.hmac`), unless body capture is on for that queue or exchange (`capture: { bodyQueues: [...] }`) or `exportRoutingKeys: true` (or `TRAILBEAM_AMQP_EXPORT_ROUTING_KEYS=on`) is set: then verbatim |
| `messageId` | **always, as the application set it**, with control characters (such as a trailing newline) removed |
| `timestamp` | when set |
| Reply code and text of a return or a close | always |
| `x-acquired-count`, `x-delivery-count`, and the newest `x-death` entry (queue, exchange, reason, count, time) and `x-first-death-*` | when the message has them |
| Tenant | only with the tenant option (`tenant.messageAttribute`, or `TRAILBEAM_TENANT_ATTRIBUTE` / `_BODY_PATH`) |
| Body size | always |
| Body, redacted | only on queues and exchanges in the body list |

Never exported: `correlationId`, `replyTo`, `userId`, `appId`, `type`, `contentType`, `contentEncoding`, `expiration`, `priority`, `clusterId`, any other header, and the body unless you capture it.

**The message ID leaves the process.** It is how Trailbeam identifies a message, so it is exported whatever the
capture settings. Your application chooses it: if your IDs hold order numbers, email addresses or other business
data, that data reaches Trailbeam. Use opaque IDs (a UUID), or let `setMessageId` give one to publishes without an ID.

**Routing keys stay in the process by default.** Topic routing keys often hold IDs or email addresses
(`user.alice@example.com.signup`), so with body capture off a span carries only an HMAC of the key, made with
`TRAILBEAM_RECEIPT_HASH_KEY` (or `receiptHashKey`). Trailbeam doesn't join or group by routing key, so nothing else changes; the timeline shows the key as
"hashed". Equal hashes mean equal keys. The hash protects the key only if the key is secret: with the published
default key, anyone can hash a guess and compare. Set your own key.

## Configuration

| Variable | Default | |
|---|---|---|
| `TRAILBEAM_RECEIPT_HASH_KEY` | `trailbeam-default-receipt-hash-key`, with one warning | The same key on every service that uses the same queues. A secret: never commit it. |
| `TRAILBEAM_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` | OTLP/HTTP (protobuf). |
| `OTEL_SERVICE_NAME` | `unknown_service` | `service.name` |
| `OTEL_RESOURCE_ATTRIBUTES` | random UUID v4 | `service.instance.id` here becomes `messaging.client.id`. Without it, each `install` call and each `wrapHandler` call generates a random UUID v4. |
| `OTEL_RESOURCE_ATTRIBUTES=service.version=1.4.2` | no version | With it, Trailbeam shows each consumer's version on the timeline and on the lists, which can be filtered by `version`, so you can see whether a deploy changed things. |
| `TRAILBEAM_MAX_QUEUE_SIZE` | `16384` | Spans waiting for export, per tracer provider ([Dropped spans](#dropped-spans)). The other `OTEL_BSP_*` variables apply as usual. |
| `TRAILBEAM_SHUTDOWN_TIMEOUT_MS` | `2000` | The longest that `uninstall()`, and the export when the event loop empties, wait for queued spans ([When the server is down, or the process ends](#when-the-server-is-down-or-the-process-ends)). |

### Dropped spans

Spans wait in an in-memory queue until they are exported, in batches of 512. If Trailbeam (or the
Collector) is down or slower than your traffic, the queue fills up, and from then on new spans are
dropped. The messages they belong to then have gaps in their timelines. Drops are never silent:

- **Log line.** `process.emitWarning` (code `TRAILBEAM_SPANS_DROPPED`) warns at the first drop, then at most once a minute with the
  number dropped since the last line and the reason, and once more at shutdown:
  `trailbeam: dropped 800 spans in the last 60 s (800 queue full); 812 dropped since start. ...`
  The count is in the message, so the line can't be lost to duplicate filtering.
- **Metric.** `trailbeam.sdk.spans.dropped` (unit `{span}`), a counter on the global OpenTelemetry
  meter provider, with `error.type` = `queue_full`, `export_failed` or `shutdown`. It is a no-op unless
  your application sets a meter provider. Alert on it.
- **In code.** `trailbeam.exportStats()` returns `{ exported, queued, dropped, droppedByReason }`
  for this process.

`queue_full` means the queue was full. `export_failed` means an export failed after the OTLP exporter's
own retries, or had not finished after `OTEL_BSP_EXPORT_TIMEOUT` (30 s); the batch is not tried again: the
server was unreachable, too slow, or refused the spans. `shutdown` means spans that were still queued when
shutdown gave up (after `TRAILBEAM_SHUTDOWN_TIMEOUT_MS`), or that ended after it shut down. Spans of a `spanProcessor` you pass in are not
counted.

### When the server is down, or the process ends

Spans are exported in the background, so a down, slow or misconfigured Trailbeam server never throws or
rejects in your code and never slows your SQS calls. At the end of the process, Trailbeam waits at most
`TRAILBEAM_SHUTDOWN_TIMEOUT_MS` (2 s) for the queue to be exported, so a down or slow server delays your
exit by about that much. An export that was already under way keeps its own timeout (10 s,
`OTEL_EXPORTER_OTLP_TIMEOUT`). In each case:

| Case | What happens to the spans |
|---|---|
| A short script that ends on its own (the event loop empties) | Exported then. No `uninstall()` call is needed. |
| `process.exit()`, or a signal with no handler (`SIGTERM`, `SIGINT`) | Spans still queued are lost, and the package prints how many to stderr. Call `await trailbeam.uninstall()` before `process.exit()` and in your signal handlers. |
| The server is unreachable from the start | Your code runs as normal. Each batch fails after the exporter's retries and is dropped as `export_failed`; at exit, the last batch gives up after 2 s. Both are counted and reported. |
| The server goes away mid-run | Spans exported before that are kept. Later batches are dropped as `export_failed`, and at exit what is left as `shutdown`. Once the server is back, new spans are exported again; dropped ones are not resent. |
| A wrong or missing token | The server answers `401` and the batch is dropped as `export_failed` at once (no retry). The warning names `OTEL_EXPORTER_OTLP_HEADERS`; set an OpenTelemetry diag logger to see the 401 itself. |
| A slow server | Your SQS calls are not slowed. A request that takes longer than the exporter's timeout (10 s) is dropped as `export_failed`. At exit, what has not been exported after 2 s is dropped. |
| More spans than the server takes | The queue fills up and new spans are dropped as `queue_full` ([Dropped spans](#dropped-spans)). |

The queue holds `TRAILBEAM_MAX_QUEUE_SIZE` spans, 16,384 by default (the SDK's default is 2,048). That is
about 16 seconds of 1,000 spans/s, and about 50 MB of memory when full, more with captured bodies. A
bigger queue only rides out a longer outage. For high volume, or to keep spans through a restart, send
them through an OpenTelemetry Collector with a queue on disk
([High volume](../collector/README.md#high-volume)).

### Message contents (opt-in)

Contract section 15. Body size is always recorded. The rest is off until you turn it on:

```js
trailbeam.install({
  tenant: { messageAttribute: 'tenant', bodyPath: 'customer.id' },   // or TRAILBEAM_TENANT_ATTRIBUTE / _BODY_PATH
  capture: {
    bodyQueues: ['orders'],      // bodies of these queues, redacted, or '*'   (TRAILBEAM_CAPTURE_BODY_QUEUES)
    errorQueues: ['orders'],     // Lambda exception messages, redacted        (TRAILBEAM_CAPTURE_ERROR_QUEUES)
    redactKeys: ['customer.name'], // added to the default keys                (TRAILBEAM_REDACT_KEYS)
    maxBodyBytes: 16384,         //                                            (TRAILBEAM_CAPTURE_MAX_BYTES)
  },
});
```

`wrapHandler(fn, { tenant, capture })` takes the same options. Redaction runs in this process before
export. A body that fails redaction, fails the MD5 check or is over the limit is not exported, and
the span says why in `trailbeam.body.capture_status`. The package never makes an SQS call of its own.
A self-hosted Trailbeam stores the body only if the queue is also in its own `TRAILBEAM_STORE_BODIES`
([Message bodies](../README.md#message-bodies)).

### SNS fan-out

Contract section 21. Pass an `SNSClient` to `instrument` too: each `Publish` and `PublishBatch` to a topic records
a `trailbeam publish` span with the SNS message ID, and a receive of the message on a subscribed queue is joined to
it. Two ways the join works:

- **Raw delivery on**: the middleware adds one message attribute, `trailbeam.publish_id`, to each published message,
  on a copy of your input. It skips a message that already has 10 attributes (with raw delivery on, SNS would drop
  it), would pass 256 KiB, or uses `MessageStructure: 'json'`, and the span says why in
  `trailbeam.sns.publish_id_skipped`.
- **Raw delivery off**: the SQS body is SNS's envelope. On every queue, the receive reads three of its fields:
  `MessageId`, `TopicArn` and `Timestamp`. It does this even when body capture is off for the queue. It reads
  nothing else in the body: not `Message`, `Subject` or `MessageAttributes`, and the body itself is never
  exported. No option is needed.

```js
const { SNSClient } = require('@aws-sdk/client-sns');
trailbeam.install({
  sns: { inject: false },  // don't add trailbeam.publish_id to publishes (or TRAILBEAM_SNS_INJECT=off)
});
const sns = trailbeam.instrument(new SNSClient({}));
```

`wrapHandler(fn, { sns })` takes the same options for its receives.

### EventBridge PutEvents

Contract section 25. Pass an `EventBridgeClient` to `instrument` too: each `PutEvents` call records one
`trailbeam put_event` span per entry, in entry order, whether EventBridge accepted the entry, failed it, or the whole
call failed. Trailbeam joins an accepted entry to the receives of the SQS messages its event reached, by the event ID.

- **Recorded**: the event bus (its name, and the `EventBusName` you passed), the entry's index, whether it was accepted
  (`ok`) or `failed` with its error code, the event ID EventBridge returned, the entry's `Source` and `DetailType`
  (when 1 to 256 characters), whether you set `Time` (not its value), and the size of `Detail` in bytes.
- **Never recorded**: `Detail` itself, `Resources`, `TraceHeader`, the value of `Time`, the error message EventBridge
  returns for a failed entry, and the request's `EndpointId`.

The request is never changed, and there is no option.

```js
const { EventBridgeClient } = require('@aws-sdk/client-eventbridge');
const events = trailbeam.instrument(new EventBridgeClient({}));
```


### S3 uploads

Contract section 27. Pass an `S3Client` to `instrument` too: each `PutObject`, `CompleteMultipartUpload` or
`CopyObject` call records one `trailbeam s3_upload` span, ok or failed. The request is never changed.

- **Recorded**: the bucket, the operation, whether it succeeded (`ok`) or `failed` with its error code, the
  returned `ETag` and `VersionId`, and, only when body capture is on for the bucket (its name or `*` in `capture.bodyQueues`), the object key,
  redacted like a body.
- **Never recorded**: the object's contents, its user metadata (`Metadata`), `CopySource`, tags, checksums, encryption
  keys, and the response's `Location`.

Trailbeam joins the upload to the SQS messages its bucket notification sent, by bucket, object key and event time,
so only where body capture is on for the bucket in the uploading process and for the queue in the receiving one.
Otherwise the message keeps its inferred "S3 *event*" step. On a captured queue the receive also records the record's
`eTag` and `versionId`, so a write of the same key by a process without Trailbeam (the console, `aws s3 cp`) a moment
later is not taken for your upload. The one case it can't tell apart: a bucket without versioning, and a write of the
same bytes (the same ETag).

```js
const { S3Client } = require('@aws-sdk/client-s3');
trailbeam.install({ capture: { bodyQueues: ['photos', 'photo-uploads'] } }); // the bucket, and the queue
const s3 = trailbeam.instrument(new S3Client({}));
```

### EventBridge and S3 notifications

Contract section 23. A message that an EventBridge rule or an S3 bucket notification sent has no send in your code. When
its body is an EventBridge event, or an S3 event notification (or S3's `s3:TestEvent` message), the receive records what
the event says about where it came from, on every queue: the event's `id`, `source`, `detail-type`, account, region and
time, or the S3 event name, bucket, region, time, notification configuration ID and `sequencer`. Nothing else in the body
is read or exported. The S3 object key is recorded only on a queue whose body is captured (`capture: { bodyQueues: [...] }`), redacted like the
body. Trailbeam shows these as an inferred first step, "Arrived via an EventBridge rule" or "S3 *event*". There is no
option; `wrapHandler` reads the same.

The Node.js package does not set `thread.name` (contract section 3).

## API reference

| Export | Effect |
|---|---|
| `install({ receiptHashKey, spanExporter, spanProcessor, tenant, capture, sns })` | Creates the client hooks for this realm and returns the Trailbeam `BasicTracerProvider`. A second call replaces the first and shuts down its provider. All options are optional. |
| `instrument(client)` | Adds the middleware `trailbeamSqsMiddleware` to an AWS SDK v3 `SQSClient`, `SNSClient`, `EventBridgeClient` or `S3Client` (and `trailbeamS3Middleware`, which acts only on an `S3Client`) and returns the client. Calls `install()` first if needed. Throws `TypeError` if the argument has no middleware stack. |
| `uninstall()` | Turns the hooks off on every instrumented client, then flushes and shuts down the provider. Returns a promise. |
| `forceFlush()` | Exports the client-hook spans that are waiting. Returns a promise. |
| `wrapHandler(handler, { receiptHashKey, spanExporter, spanProcessor, flushTimeoutMs = 2000, reportBatchItemFailures = true })` | Returns an async Lambda handler with a `trailbeamTracerProvider` property. Throws `TypeError` if `handler` is not a function. |
| `exportStats()` | `{ exported, queued, dropped, droppedByReason }` for this process ([Dropped spans](#dropped-spans)). |
| `hashReceiptHandle(key, receiptHandle)`, `keyId(key)` | The two values of contract section 7. |
| `version`, `SCHEMA_VERSION` | The package version and `3`. |

Types are in [`index.d.ts`](index.d.ts).

## Tests

```sh
npm install
npm test                                            # unit tests, no network
TRAILBEAM_TEST_SQS_ENDPOINT=http://localhost:9324 npm test  # plus end to end against ElasticMQ
```

The receipt-hash tests use the shared vectors in
[`contract/span-contract-vectors.json`](../contract/span-contract-vectors.json), and the wrapper tests replay
the recorded QV-9 Lambda events in [`python/tests/fixtures`](../python/tests/fixtures); the Python
tests use both too.

The amqplib hooks' tests run the contract's RabbitMQ vectors ([`contract/span-contract-amqp-vectors.json`](../contract/span-contract-amqp-vectors.json))
on amqplib's own classes with no broker. Against a real one:

```sh
docker run -d --name rabbitmq -p 5672:5672 -e RABBITMQ_DEFAULT_USER=trailbeam -e RABBITMQ_DEFAULT_PASS=trailbeam rabbitmq:4
TRAILBEAM_TEST_AMQP_URL='amqp://trailbeam:trailbeam@127.0.0.1:5672/%2F' npm test
```

With `TRAILBEAM_TEST_SERVER_PYTHON` set to a Python that has the trailbeam package (`pip install -e ../python -e ../server`),
those tests also pass every span through the server's ingest and check each message's timeline.
