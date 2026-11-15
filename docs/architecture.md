# How qv2 works

This page explains the qv2 0.1 architecture end to end. It covers the components, the data that moves
between them, what the store records and what it infers, and how it fails. The exact span attributes
are in [`span-contract.md`](span-contract.md). The JSON API is in [`store-api.md`](store-api.md). Terms
are defined in the [glossary](glossary.md).

## Components

```
 your process                                   optional                 qv2 store (one container)
+-------------------------------+         +-------------------+        +----------------------------+
| app code                      |         | OpenTelemetry     |        | POST /v1/traces  (ingest)  |
|   | SQS SDK call              |  OTLP/  | Collector         |  OTLP/ | SQLite file, table spans   |
|   v                           |  HTTP   | qv2-collector.yaml|  HTTP  | GET /api/...     (JSON)    |
| qv2 hook or Lambda wrapper ---+-------->| keeps scope "qv2" |------->| GET /, /message  (HTML)    |
|   own TracerProvider,         |         | drops the rest    |        |                            |
|   always-on sampler           |         +-------------------+        +----------------------------+
+-------------------------------+
        |
        v   the SDK sends the request to Amazon SQS, unchanged except for one ReceiveMessage parameter
```

| Component | Code | What it does |
|---|---|---|
| Python client hooks | [`python/src/qv2/boto.py`](../python/src/qv2/boto.py), `qv2.boto.install()` | Hooks botocore through `opentelemetry-instrumentation-botocore`. Emits one span per message for 7 SQS actions. |
| Python Lambda wrapper | [`python/src/qv2/aws_lambda.py`](../python/src/qv2/aws_lambda.py), `@qv2.aws_lambda.wrap` | Emits one `qv2 receive` per SQS record before the handler runs, and one `qv2 outcome` per record after it. |
| Node.js client hooks | [`node/src/sqs.js`](../node/src/sqs.js), `qv2.instrument(client)` | Adds a middleware to an AWS SDK v3 `SQSClient`. Emits the same spans as the Python hooks. |
| Node.js Lambda wrapper | [`node/src/lambda.js`](../node/src/lambda.js), `qv2.wrapHandler(fn)` | Same behaviour as the Python wrapper. |
| Collector configuration | [`collector/qv2-collector.yaml`](../collector/qv2-collector.yaml) | Optional. Forwards every span with instrumentation scope `qv2` to the store, unsampled. |
| Store | [`server/src/qv2_store/`](../server/src/qv2_store/), command `qv2-store` | One Python process. Decodes OTLP/HTTP, stores each qv2 span as one SQLite row, joins deletes to receives, and builds timelines when you read them. |
| Web screen | [`server/src/qv2_store/web.py`](../server/src/qv2_store/web.py) | Server-rendered HTML built from the JSON API's responses. No JavaScript. |

The 7 SQS actions are `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`,
`DeleteMessageBatch`, `ChangeMessageVisibility` and `ChangeMessageVisibilityBatch`. The hooks ignore
every other SQS action.

## What qv2 does and does not do to your queues and data

- qv2 makes no SQS calls of its own. The packages only observe the calls your code already makes. The
  store makes no AWS calls and needs no AWS credentials.
- qv2 never calls `ReceiveMessage` on your queues, including the dead-letter queue. A receive changes a
  message's state: its receive count, its visibility timeout and its receipt handle.
- The client hooks change one thing in your requests. On `ReceiveMessage`, they add the system
  attributes `ApproximateReceiveCount`, `SentTimestamp`, `ApproximateFirstReceiveTimestamp`,
  `AWSTraceHeader` and `DeadLetterQueueSourceArn` to `MessageSystemAttributeNames`. They keep every
  name you asked for. If you asked for `All`, they add nothing. If you use only the deprecated
  `AttributeNames`, they add the names there.
- The hooks need no IAM permissions beyond the ones your code already uses, because they make no
  calls. This repository defines no IAM role or policy for qv2.
- qv2 does not record message bodies or message attributes. There is no option to capture them in
  0.1.
- qv2 does not export receipt handles. It exports a keyed hash of each handle (HMAC-SHA256, first 32
  hex characters). See [Receipt-handle hash](span-contract.md#7-receipt-handle-hash).
- On a handler exception, the Lambda wrapper records the exception's class name, never its message.
- qv2 does record: message IDs, queue names and queue URLs (a URL contains the AWS account ID), the
  `AWSTraceHeader` value verbatim, `service.name`, process IDs, Python thread names, Lambda function
  names and invocation IDs.

## Data flow, from SendMessage to the timeline

This section follows one message through the system.

### 1. The hook records each SQS call as an interval

For each tracked call, the hook does this:

1. Before the SDK sends the request, the hook takes a call-order tick (`qv2.order.issued`) and then
   reads the wall clock (the span start).
2. The SDK sends the request. SQS acts on it at some moment that qv2 cannot see.
3. When the response or error arrives, the hook reads the wall clock (the span end) and takes a second
   tick (`qv2.order.returned`).
4. The hook emits one span per message in the call. Every span of one call gets the same start, end
   and ticks.

The interval contains the moment SQS acted. The ticks come from one counter per process (Python) or
per realm (Node.js). Two calls in one process are ordered when one call's `returned` tick is lower
than the other call's `issued` tick.

Clocks: Python reads `time.time_ns()`. Node.js reads `Date.now()`, which has millisecond resolution.

### 2. The span carries the SQS facts for that message

| Span | Emitted when | Key attributes |
|---|---|---|
| `qv2 send` | SQS accepted the message in `SendMessage` or `SendMessageBatch` | `messaging.message.id` from the response |
| `qv2 receive` | `ReceiveMessage` returned the message, or a Lambda event holds it as a record | message ID, `qv2.receive_count`, receipt-handle hash, `qv2.sent_timestamp_ms`, `qv2.first_receive_timestamp_ms`, `qv2.dead_letter_queue_source_arn` when SQS sets it |
| `qv2 delete` | after a `DeleteMessage` or `DeleteMessageBatch` response or error, one span per receipt handle | receipt-handle hash, `qv2.result` (`ok` or `failed`), `qv2.error_code` |
| `qv2 change_visibility` | after a `ChangeMessageVisibility` or `ChangeMessageVisibilityBatch` response or error, one span per receipt handle | receipt-handle hash, `qv2.visibility_timeout`, `qv2.result`, `qv2.error_code` |
| `qv2 outcome` | after a wrapped Lambda handler returns or throws, one span per record | `qv2.outcome`, `qv2.error_type` |

A delete or visibility change takes a receipt handle, not a message ID. The hooks keep an in-process
map from receipt handle to message ID. The map holds at most 100,000 entries, each for at most 12
hours. When the handle is in the map, the settle span also carries `messaging.message.id`.

The Lambda wrapper cannot see Lambda's `ReceiveMessage` call. Its `qv2 receive` is zero-length at the
moment the wrapper got the event, with `qv2.start_observed = false`. The store reads that time as an
upper bound only. The wrapper flushes the receive spans before the handler runs, and waits at most
2000 ms by default for that flush.

### 3. Export

Each package creates its own `TracerProvider` with an always-on sampler. It does not register the
provider globally. Your application's sampling therefore never drops qv2 spans. The provider exports
OTLP/HTTP protobuf to `QV2_OTLP_TRACES_ENDPOINT`, default `http://localhost:4318/v1/traces`, through
the OpenTelemetry SDK's `BatchSpanProcessor`.

In Python, if your application has not instrumented botocore, `qv2.boto.install()` instruments it with
the qv2 provider. The stock botocore spans then go to the same endpoint. The store counts them as
`ignored`. If your application instruments botocore first, qv2 keeps your provider for the stock
spans.

You can export to the store directly or through the Collector. The Collector configuration keeps every
span with instrumentation scope `qv2` and drops every other span before the store.

### 4. Ingest

The store accepts `POST /v1/traces` with `application/x-protobuf` or `application/json`, and
optionally `Content-Encoding: gzip` or `deflate`. For each span, the store does this:

1. If the instrumentation scope is not `qv2`, it counts the span as `ignored` and drops it.
2. If `qv2.schema_version` is not 2 or 3, or a required attribute is missing, it counts the span as
   `rejected`. It reports the reason in the OTLP `partial_success` field of the response.
3. It classifies the span by `qv2.kind` and `messaging.operation.name`. It never parses span names.
4. It inserts one row into the SQLite table `spans`. A span with a trace ID and span ID already stored
   is a duplicate (for example an exporter retry), and the store skips it.

### 5. The receipt-hash join

Each row has a `mid` column: the message the row belongs to.

- Send, receive and outcome rows: `mid` is the span's own `messaging.message.id`.
- Delete and visibility-change rows: the store looks for a `qv2 receive` with the same
  `qv2.receipt_handle.hmac` and `qv2.receipt_handle.key_id`. If it finds one, `mid` is that receive's
  message ID, and `receive_row` points at that receive. This tells the store which delivery the
  handle came from.
- If no receive matches yet, `mid` is the settle span's own message ID, if it has one.

Spans arrive in any order, so the join runs in both directions. A new settle row looks for its
receive. A new receive row claims settle rows that arrived before it. Hashes made with different keys
never match.

### 6. Building a timeline

The store builds a timeline each time you request one (`GET /api/messages/{id}` or the message page).
It does not store timelines. It reads every row whose `mid` is the message ID and then does this:

1. It makes one observed event per row.
2. It proves order between pairs of observed events. Event A comes before event B only when one of
   four observed facts, called sources, proves it:

   | Source | Evidence |
   |---|---|
   | `clock` | A's span ended before B's span started, on the hosts' clocks. |
   | `process_order` | Same `qv2.order.scope`, and A's `returned` tick is lower than B's `issued` tick. |
   | `receive_count` | Two receives on one queue, and A has the lower `ApproximateReceiveCount`. |
   | `sqs_timestamps` | A is the send, B is the receive with receive count 1 on that queue, and B's `SentTimestamp` is lower than its `ApproximateFirstReceiveTimestamp`. Equal values prove nothing. |

   Order is transitive. If the sources contradict each other (a cycle), the store drops the edges on
   the cycle and adds an `order_conflict` notice.
3. It adds inferred events and the observed dead-letter queue move (next section).
4. It lists the events in an order that agrees with every proven edge. It cuts the list into groups
   wherever everything before the cut is proven before everything after it. A group of more than one
   event is "concurrent, order unknown".
5. It computes the summary: `state`, `deliveries`, `successful_deliveries`, `first_seen` and the
   other fields in [Summary fields](store-api.md#summary-fields).

The store never orders events by what caused what. "A receive comes after its send" is not a source.
If no source orders a send and its receive, they share a concurrent group. This is common when a
consumer waits in a long poll: its `ReceiveMessage` started before the message was sent.

## What is recorded and what is inferred

| Timeline event | How the store knows | Evidence |
|---|---|---|
| `sent` | recorded | a `qv2 send` span |
| `received` | recorded | a `qv2 receive` span |
| `deleted` | recorded | a `qv2 delete` span. `status` is `final` only if SQS accepted it and the handle came from the message's latest receive. |
| `visibility_changed` | recorded | a `qv2 change_visibility` span |
| `lambda_outcome` | recorded | a `qv2 outcome` span |
| `moved_to_dead_letter_queue` | reported by SQS, `inferred: false` | a receive whose `DeadLetterQueueSourceArn` names another queue. It has no span and no time of its own. |
| `visibility_timeout_expired` | inferred | a receive on a queue follows an earlier receive on that queue with a lower receive count |
| `deliveries_not_observed` | inferred | the receive count jumps by more than 1, or the first observed receive on the source queue has a receive count above 1 |
| `deleted_by_lambda` | inferred | a `qv2 outcome` with value `success`. Lambda's event source mapping deletes the message, and qv2 does not see that call. |
| `lambda_invocation_ended_without_outcome` | inferred | a Lambda receive with no `qv2 outcome` for its invocation after `--lambda-timeout` seconds (default 900) |

An inferred event has no span and no time. It has a `window`, a `reason` sentence and anchors: the
observed events it was inferred from. The store places it next to its anchors and orders it against
other events only through them.

The store never infers a move to a dead-letter queue from the message ID alone. If you configure the
queue's `maxReceiveCount` (`--max-receive-count`), and the receive count on the source queue has
reached it, the store says "not seen on the DLQ yet". It shows no move until a receive from the
dead-letter queue reports one.

## Failure cases

| What happens | Effect on the timeline | What the store shows |
|---|---|---|
| A process exits or is killed before its `BatchSpanProcessor` exports | Those spans are lost. | Missing events. A lost receive can produce `deliveries_not_observed`. |
| A Lambda invocation times out or runs out of memory | The receive span was flushed before the handler ran. No outcome span exists. | `lambda_invocation_ended_without_outcome` after `--lambda-timeout`. Until then, if it is the latest delivery, `state` is `processing`. |
| A producer or consumer has no qv2 hooks | No spans from it. | `send_not_observed` notice, or `deliveries_not_observed`. |
| Services use different receipt-hash keys | Deletes cannot join to receives. | `key_mismatch` notice, `deleted.status` `unmatched_handle`, `unjoined_settles` in `/api/health`. |
| No key is set | The packages use the published default key and log one warning. | `default_key` notice on every message page. |
| A host clock is offset from another host's | `clock` edges between those hosts can be wrong. The store does not correct clock offset in 0.1. | Possibly a wrong order, or an `order_conflict` notice when sources disagree. |
| A delete uses an old receipt handle | Amazon SQS can accept it without deleting the message. | `deleted.status` `stale_handle`, and a later receive. ElasticMQ rejects such a delete (`failed`, `ReceiptHandleIsInvalid`). |
| The store is down, export goes through the Collector | The Collector retries until its queue of 5,000 batches is full, then drops new spans. Its queue is in memory. | Missing events after the queue fills, or after a Collector restart. |
| The store is down, export goes directly to it | The SDK exporter retries and then drops the batch. | Missing events. |
| The same span arrives twice | The store skips the duplicate. | No effect. `ingest.duplicates` in `/api/health` counts it. |
| Two queues in different accounts or regions share a name | The store groups by queue name. | Their messages are merged under one name. |

## Limits of 0.1

- One store process with one SQLite connection behind a lock.
- No authentication. Bind to `127.0.0.1` unless the network is trusted.
- No retention. Data stays until you delete the SQLite file.
- List endpoints build a full timeline for each listed message. They are for screen-sized `limit`
  values, not for exports.
- Moves back from a dead-letter queue (redrive) show no event.
- Whether Lambda SQS records carry `DeadLetterQueueSourceArn` is not verified.
