# How Trailbeam works

This page explains the Trailbeam 0.1 architecture end to end. It covers the components, the data that moves
between them, what Trailbeam records and what it infers, and how it fails. The exact span attributes
are in [`span-contract.md`](span-contract.md). The JSON API is in [`api.md`](api.md). Terms
are defined in the [glossary](glossary.md).

## Components

```
 your process                                   optional                 trailbeam (one container)   
+-------------------------------+         +-------------------+        +----------------------------+
| app code                      |         | OpenTelemetry     |        | POST /v1/traces  (ingest)  |
|   | SQS SDK call              |  OTLP/  | Collector         |  OTLP/ | SQLite file, table spans   |
|   v                           |  HTTP   | trailbeam-collector.yaml|  HTTP  | GET /api/...     (JSON)    |
| trailbeam hook or Lambda wrapper ---+-------->| keeps scope "trailbeam" |------->| GET /, /message  (HTML)    |
|   own TracerProvider,         |         | drops the rest    |        |                            |
|   always-on sampler           |         +-------------------+        +----------------------------+
+-------------------------------+
        |
        v   the SDK sends the request to Amazon SQS, unchanged except for one ReceiveMessage parameter
```

| Component | Code | What it does |
|---|---|---|
| Python client hooks | [`python/src/trailbeam/boto.py`](../python/src/trailbeam/boto.py), `trailbeam.boto.install()` | Hooks botocore through `opentelemetry-instrumentation-botocore`. Emits one span per message for 7 SQS actions. |
| Python Lambda wrapper | [`python/src/trailbeam/aws_lambda.py`](../python/src/trailbeam/aws_lambda.py), `@trailbeam.aws_lambda.wrap` | Emits one `trailbeam receive` per SQS record before the handler runs, and one `trailbeam outcome` per record after it. |
| Node.js client hooks | [`node/src/sqs.js`](../node/src/sqs.js), `trailbeam.instrument(client)` | Adds a middleware to an AWS SDK v3 `SQSClient`. Emits the same spans as the Python hooks. |
| Node.js Lambda wrapper | [`node/src/lambda.js`](../node/src/lambda.js), `trailbeam.wrapHandler(fn)` | Same behaviour as the Python wrapper. |
| Collector configuration | [`collector/trailbeam-collector.yaml`](../collector/trailbeam-collector.yaml) | Optional. Forwards every span with instrumentation scope `trailbeam` to Trailbeam, unsampled. |
| Trailbeam | [`python/src/trailbeam/server/`](../python/src/trailbeam/server/), command `trailbeam` | One Python process. Decodes OTLP/HTTP, stores each Trailbeam span as one SQLite row, joins deletes to receives, and builds timelines when you read them. |
| Web screen | [`python/src/trailbeam/server/web.py`](../python/src/trailbeam/server/web.py) | Server-rendered HTML built from the JSON API's responses. No JavaScript. |

The 7 SQS actions are `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`,
`DeleteMessageBatch`, `ChangeMessageVisibility` and `ChangeMessageVisibilityBatch`. The hooks ignore
every other SQS action.

## What Trailbeam does and does not do to your queues and data

- Trailbeam makes no SQS calls of its own. The packages only observe the calls your code already makes. The
  Trailbeam makes no AWS calls and needs no AWS credentials.
- Trailbeam never calls `ReceiveMessage` on your queues, including the dead-letter queue. A receive changes a
  message's state: its receive count, its visibility timeout and its receipt handle.
- The client hooks change one thing in your requests. On `ReceiveMessage`, they add the system
  attributes `ApproximateReceiveCount`, `SentTimestamp`, `ApproximateFirstReceiveTimestamp`,
  `AWSTraceHeader` and `DeadLetterQueueSourceArn` to `MessageSystemAttributeNames`. They keep every
  name you asked for. If you asked for `All`, they add nothing. If you use only the deprecated
  `AttributeNames`, they add the names there.
- The hooks need no IAM permissions beyond the ones your code already uses, because they make no
  calls. This repository defines no IAM role or policy for Trailbeam.
- Trailbeam does not record message bodies or message attributes. There is no option to capture them in
  0.1.
- Trailbeam does not export receipt handles. It exports a keyed hash of each handle (HMAC-SHA256, first 32
  hex characters). See [Receipt-handle hash](span-contract.md#7-receipt-handle-hash).
- On a handler exception, the Lambda wrapper records the exception's class name, never its message.
- Trailbeam does record: message IDs, queue names and queue URLs (a URL contains the AWS account ID), the
  `AWSTraceHeader` value verbatim, `service.name`, process IDs, Python thread names, Lambda function
  names and invocation IDs.

## Data flow, from SendMessage to the timeline

This section follows one message through the system.

### 1. The hook records each SQS call as an interval

For each tracked call, the hook does this:

1. Before the SDK sends the request, the hook takes a call-order tick (`trailbeam.order.issued`) and then
   reads the wall clock (the span start).
2. The SDK sends the request. SQS acts on it at some moment that Trailbeam cannot see.
3. When the response or error arrives, the hook reads the wall clock (the span end) and takes a second
   tick (`trailbeam.order.returned`).
4. The hook emits one span per message in the call. Every span of one call gets the same start, end
   and ticks.

The interval contains the moment SQS acted. The ticks come from one counter per process (Python) or
per realm (Node.js). Two calls in one process are ordered when one call's `returned` tick is lower
than the other call's `issued` tick.

Clocks: Python reads `time.time_ns()`. Node.js reads `Date.now()`, which has millisecond resolution.

### 2. The span carries the SQS facts for that message

| Span | Emitted when | Key attributes |
|---|---|---|
| `trailbeam send` | SQS accepted the message in `SendMessage` or `SendMessageBatch` | `messaging.message.id` from the response |
| `trailbeam receive` | `ReceiveMessage` returned the message, or a Lambda event holds it as a record | message ID, `trailbeam.receive_count`, receipt-handle hash, `trailbeam.sent_timestamp_ms`, `trailbeam.first_receive_timestamp_ms`, `trailbeam.dead_letter_queue_source_arn` when SQS sets it |
| `trailbeam delete` | after a `DeleteMessage` or `DeleteMessageBatch` response or error, one span per receipt handle | receipt-handle hash, `trailbeam.result` (`ok` or `failed`), `trailbeam.error_code` |
| `trailbeam change_visibility` | after a `ChangeMessageVisibility` or `ChangeMessageVisibilityBatch` response or error, one span per receipt handle | receipt-handle hash, `trailbeam.visibility_timeout`, `trailbeam.result`, `trailbeam.error_code` |
| `trailbeam outcome` | after a wrapped Lambda handler returns or throws, one span per record | `trailbeam.outcome`, `trailbeam.error_type` |

A delete or visibility change takes a receipt handle, not a message ID. The hooks keep an in-process
map from receipt handle to message ID. The map holds at most 100,000 entries, each for at most 12
hours. When the handle is in the map, the settle span also carries `messaging.message.id`.

The Lambda wrapper cannot see Lambda's `ReceiveMessage` call. Its `trailbeam receive` is zero-length at the
moment the wrapper got the event, with `trailbeam.start_observed = false`. Trailbeam reads that time as an
upper bound only. The wrapper flushes the receive spans before the handler runs, and waits at most
2000 ms by default for that flush.

### 3. Export

Each package creates its own `TracerProvider` with an always-on sampler. It does not register the
provider globally. Your application's sampling therefore never drops Trailbeam spans. The provider exports
OTLP/HTTP protobuf to `TRAILBEAM_OTLP_TRACES_ENDPOINT`, default `http://localhost:4318/v1/traces`, through
the OpenTelemetry SDK's `BatchSpanProcessor`.

In Python, if your application has not instrumented botocore, `trailbeam.boto.install()` instruments it with
the Trailbeam provider. The stock botocore spans then go to the same endpoint. Trailbeam counts them as
`ignored`. If your application instruments botocore first, Trailbeam keeps your provider for the stock
spans.

You can export to Trailbeam directly or through the Collector. The Collector configuration keeps every
span with instrumentation scope `trailbeam` and drops every other span before Trailbeam.

### 4. Ingest

Trailbeam accepts `POST /v1/traces` with `application/x-protobuf` or `application/json`, and
optionally `Content-Encoding: gzip` or `deflate`. For each span, Trailbeam does this:

1. If the instrumentation scope is not `trailbeam`, it counts the span as `ignored` and drops it.
2. If `trailbeam.schema_version` is not 2 or 3, or a required attribute is missing, it counts the span as
   `rejected`. It reports the reason in the OTLP `partial_success` field of the response.
3. It classifies the span by `trailbeam.kind` and `messaging.operation.name`. It never parses span names.
4. It inserts one row into the SQLite table `spans`. A span with a trace ID and span ID already stored
   is a duplicate (for example an exporter retry), and Trailbeam skips it.

### 5. The receipt-hash join

Each row has a `mid` column: the message the row belongs to.

- Send, receive and outcome rows: `mid` is the span's own `messaging.message.id`.
- Delete and visibility-change rows: Trailbeam looks for a `trailbeam receive` with the same
  `trailbeam.receipt_handle.hmac` and `trailbeam.receipt_handle.key_id`. If it finds one, `mid` is that receive's
  message ID, and `receive_row` points at that receive. This tells Trailbeam which delivery the
  handle came from.
- If no receive matches yet, `mid` is the settle span's own message ID, if it has one.

Spans arrive in any order, so the join runs in both directions. A new settle row looks for its
receive. A new receive row claims settle rows that arrived before it. Hashes made with different keys
never match.

### 6. Building a timeline

Trailbeam builds a timeline each time you request one (`GET /api/messages/{id}` or the message page).
It does not store timelines. It reads every row whose `mid` is the message ID and then does this:

1. It makes one observed event per row.
2. It proves order between pairs of observed events. Event A comes before event B only when one of
   four observed facts, called sources, proves it:

   | Source | Evidence |
   |---|---|
   | `clock` | A's span ended before B's span started, on the hosts' clocks. |
   | `process_order` | Same `trailbeam.order.scope`, and A's `returned` tick is lower than B's `issued` tick. |
   | `receive_count` | Two receives on one queue, and A has the lower `ApproximateReceiveCount`. |
   | `sqs_timestamps` | A is the send, B is the receive with receive count 1 on that queue, and B's `SentTimestamp` is lower than its `ApproximateFirstReceiveTimestamp`. Equal values prove nothing. |

   Order is transitive. If the sources contradict each other (a cycle), Trailbeam drops the edges on
   the cycle and adds an `order_conflict` notice.
3. It adds inferred events and the observed dead-letter queue move (next section).
4. It lists the events in an order that agrees with every proven edge. It cuts the list into groups
   wherever everything before the cut is proven before everything after it. A group of more than one
   event is "concurrent, order unknown".
5. It computes the summary: `state`, `deliveries`, `successful_deliveries`, `first_seen` and the
   other fields in [Summary fields](api.md#summary-fields).

Trailbeam never orders events by what caused what. "A receive comes after its send" is not a source.
If no source orders a send and its receive, they share a concurrent group. This is common when a
consumer waits in a long poll: its `ReceiveMessage` started before the message was sent.

## What is recorded and what is inferred

| Timeline event | How Trailbeam knows | Evidence |
|---|---|---|
| `sent` | recorded | a `trailbeam send` span |
| `received` | recorded | a `trailbeam receive` span |
| `deleted` | recorded | a `trailbeam delete` span. `status` is `final` only if SQS accepted it and the handle came from the message's latest receive. |
| `visibility_changed` | recorded | a `trailbeam change_visibility` span |
| `lambda_outcome` | recorded | a `trailbeam outcome` span |
| `moved_to_dead_letter_queue` | reported by SQS, `inferred: false` | a receive whose `DeadLetterQueueSourceArn` names another queue. It has no span and no time of its own. |
| `visibility_timeout_expired` | inferred | a receive on a queue follows an earlier receive on that queue with a lower receive count |
| `deliveries_not_observed` | inferred | the receive count jumps by more than 1, or the first observed receive on the source queue has a receive count above 1 |
| `deleted_by_lambda` | inferred | a `trailbeam outcome` with value `success`. Lambda's event source mapping deletes the message, and Trailbeam does not see that call. |
| `lambda_invocation_ended_without_outcome` | inferred | a Lambda receive with no `trailbeam outcome` for its invocation after `--lambda-timeout` seconds (default 900) |

An inferred event has no span and no time. It has a `window`, a `reason` sentence and anchors: the
observed events it was inferred from. Trailbeam places it next to its anchors and orders it against
other events only through them.

Trailbeam never infers a move to a dead-letter queue from the message ID alone. If you configure the
queue's `maxReceiveCount` (`--max-receive-count`), and the receive count on the source queue has
reached it, Trailbeam says "not seen on the DLQ yet". It shows no move until a receive from the
dead-letter queue reports one.

## Failure cases

| What happens | Effect on the timeline | What Trailbeam shows |
|---|---|---|
| A process exits or is killed before its `BatchSpanProcessor` exports | Those spans are lost. | Missing events. A lost receive can produce `deliveries_not_observed`. |
| A Lambda invocation times out or runs out of memory | The receive span was flushed before the handler ran. No outcome span exists. | `lambda_invocation_ended_without_outcome` after `--lambda-timeout`. Until then, if it is the latest delivery, `state` is `processing`. |
| A producer or consumer has no Trailbeam hooks | No spans from it. | `send_not_observed` notice, or `deliveries_not_observed`. |
| Services use different receipt-hash keys | Deletes cannot join to receives. | `key_mismatch` notice, `deleted.status` `unmatched_handle`, `unjoined_settles` in `/api/health`. |
| No key is set | The packages use the published default key and log one warning. | `default_key` notice on every message page. |
| A host clock is offset from another host's | `clock` edges between those hosts can be wrong. Trailbeam does not correct clock offset in 0.1. | Possibly a wrong order, or an `order_conflict` notice when sources disagree. |
| A delete uses an old receipt handle | Amazon SQS can accept it without deleting the message. | `deleted.status` `stale_handle`, and a later receive. ElasticMQ rejects such a delete (`failed`, `ReceiptHandleIsInvalid`). |
| Trailbeam is down, export goes through the Collector | The Collector retries until its queue of 5,000 batches is full, then drops new spans. Its queue is in memory. | Missing events after the queue fills, or after a Collector restart. |
| Trailbeam is down, export goes directly to it | The SDK exporter retries and then drops the batch. | Missing events. |
| The same span arrives twice | Trailbeam skips the duplicate. | No effect. `ingest.duplicates` in `/api/health` counts it. |
| Two queues in different accounts or regions share a name | Trailbeam groups by queue name. | Their messages are merged under one name. |

## Limits of 0.1

- One Trailbeam process with one SQLite connection behind a lock.
- No authentication. Bind to `127.0.0.1` unless the network is trusted.
- No retention. Data stays until you delete the SQLite file.
- List endpoints build a full timeline for each listed message. They are for screen-sized `limit`
  values, not for exports.
- Moves back from a dead-letter queue (redrive) show no event.
- Whether Lambda SQS records carry `DeadLetterQueueSourceArn` is not verified.
