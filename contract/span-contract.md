# Trailbeam span contract, schema version 3 (revision 3.1)

This file defines the OpenTelemetry spans that the `trailbeam` Python package and the `trailbeam` Node.js
package emit, and that the Trailbeam reads. Both packages and Trailbeam are built and tested against
it. A change to anything below is a contract change: bump `trailbeam.schema_version`, and update this file,
both packages and Trailbeam in the same change. A revision (3.1) only adds an optional attribute that
older stores ignore and that Trailbeam does not require; it keeps `trailbeam.schema_version = 3` (section 14).

The keywords **must**, **must not**, **should** and **may** are used as in RFC 2119.

## 1. Summary

| Span name | `trailbeam.kind` | `messaging.operation.type` | Emitted by | When |
|---|---|---|---|---|
| `trailbeam send` | `sqs.send` | `send` | client hooks | once per message that SQS accepted in `SendMessage` / `SendMessageBatch` |
| `trailbeam receive` | `sqs.receive` | `receive` | client hooks, Lambda wrapper | once per message returned by `ReceiveMessage`, or once per record in a Lambda SQS event |
| `trailbeam delete` | `sqs.settle` | `settle` | client hooks | once per receipt handle in `DeleteMessage` / `DeleteMessageBatch` |
| `trailbeam change_visibility` | `sqs.settle` | `settle` | client hooks | once per receipt handle in `ChangeMessageVisibility` / `ChangeMessageVisibilityBatch` |
| `trailbeam outcome` | `sqs.outcome` | `process` | Lambda wrapper | once per record, after the handler returns or throws |

"Client hooks" are the boto3 hooks (Python) and the AWS SDK v3 hooks (Node.js). "Lambda wrapper" is
the handler wrapper in each package. In 0.1, `trailbeam outcome` comes only from the Lambda wrappers; for
other consumers, the outcome of processing is the `trailbeam delete` (or its absence).

Trailbeam **must** identify Trailbeam spans by instrumentation scope name `trailbeam` and the attribute
`trailbeam.schema_version`, and classify them by `trailbeam.kind` and `messaging.operation.name`. It **must not**
parse span names; they are for people reading traces. Span names **must not** contain the queue
name or any other variable part. (The QV-9/QV-11 prototypes put the queue name in the delete and
change-visibility span names; that is dropped.)

## 2. Transport and span shape

- Each package creates its **own** `TracerProvider` with an always-on sampler, so Trailbeam spans are never
  sampled away by the application's own sampling. This is what the prototypes did, and what QV-9 and
  QV-11 measured.
- Export is OTLP over HTTP (protobuf) to `TRAILBEAM_OTLP_TRACES_ENDPOINT`, default
  `http://localhost:4318/v1/traces`. Trailbeam accepts OTLP/HTTP on that path. The Collector
  configuration in `collector/` **must** pass every span with scope `trailbeam` through unsampled.
- Instrumentation scope: name `trailbeam`, version = the package version.
- Span kind: `INTERNAL`.
- Parent: in the client hooks, the stock AWS SDK instrumentation span for the same call
  (`opentelemetry-instrumentation-botocore` in Python, `@opentelemetry/instrumentation-aws-sdk` in
  Node.js), so the Trailbeam span shares its trace ID. In the Lambda wrapper, the active span at
  invocation time, or none (a root span).
- Time (changed in schema version 2, see section 12): a span from the client hooks is an
  **interval** that contains the moment SQS acted:
  - start = just before the request is sent: the hook runs before the SDK serializes, signs and
    sends the request (and before any retry), so the start is a lower bound;
  - end = when the response (or the error) arrived back in the hook, an upper bound.

  Every span of one call (each entry of a batch, each message of a `ReceiveMessage`) gets that call's
  interval. Both times are read from the system's wall clock, so spans from processes on one host can
  be compared. Python reads `time.time_ns()` (nanoseconds). Node.js reads `Date.now()` (whole
  milliseconds). Node.js rounding is always down. It can make two intervals look like they touch, but
  it never reverses "A ended before B started".
- Lambda wrapper: Lambda's event source mapping makes the `ReceiveMessage` call, so the wrapper does
  not see when it started or ended. All the wrapper knows is that the receive happened **no later
  than** the moment the wrapper got the event, before the handler runs. A `trailbeam receive` from the
  wrapper is therefore zero-length at that moment and carries `trailbeam.start_observed = false`: its end
  is an upper bound and its start is **not known**. The wrapper **must not** put any other time in
  its start. `trailbeam outcome` is zero-length at the moment the wrapper decided the outcome, after the
  handler returned or threw; that moment is observed, so it has no `trailbeam.start_observed`.
- A consumer **must not** read an order into two spans whose intervals overlap from their times
  alone: SQS may have acted on either first. Another observed fact can still order them (the call
  order in one process, section 2a, or SQS's receive count and timestamps); section 9a lists the
  sources and says how Trailbeam shows what none of them orders.
- One span per message. A batch call produces one Trailbeam span per entry, all children of the one
  stock span.
- Trailbeam reads only Trailbeam spans. Stock SQS spans pass through the Collector but are not read by
  Trailbeam 0.1.

## 2a. Call order inside one process (schema version 3)

The clocks have millisecond resolution, and a consumer often issues its delete in the same
millisecond its receive returned, so the clocks cannot order the two. The process itself can: it
returned from one call before it issued the next. Each package keeps one **call-order counter** per
process and records it on every span:

- `trailbeam.order.scope` names the counter: a random UUID v4 chosen when the package is loaded. Python
  chooses a new one in a forked child (`os.register_at_fork`), whose counter starts again at 1.
  Node.js has one per realm, so each `worker_thread` has its own. Counters with different scopes are
  never compared.
- `trailbeam.order.issued` is a tick of the counter taken in the request hook just before the start time
  is read, so it is no later than the request being sent.
- `trailbeam.order.returned` is a tick taken when the response (or error) arrived, just after the end time
  is read, so it is no earlier than the response.
- `tick()` returns a strictly increasing integer (1, 2, 3, ...), safe to call from any thread.

Every span of one call shares its `issued` and `returned`. If `A.returned < B.issued` with the same
scope, A's call returned before B's was issued, so SQS acted on A before B, whatever the clocks say
and whichever threads made the calls. Calls in flight at the same time (`B.issued < A.returned` and
`A.issued < B.returned`) are not ordered. Trailbeam never compares ticks of different scopes
(section 9a).

**The scope is the whole process (Python) or the whole realm (Node.js), not one thread or async task.**
One counter is shared by every thread of a Python process (behind a lock) and by every async task of a
Node.js realm (which runs on one thread), so a call on one thread can be ordered before a call on another.
That is sound, for the same reason it is sound within one thread. An edge needs A to have *returned*
before B was *issued*. Both ticks come from one counter whose increments happen one at a time (a lock in
Python, the single thread of a realm in Node.js), so the tick order is the real-time order of those two
moments, as a perfect clock's would be. And `returned` is taken after A's response arrived, `issued`
before B's request was sent. Two calls in flight together never get an edge, whichever threads made them.
Measured in QV-32: 96 of 1,302 call-order edges joined calls on different threads or async tasks, all of
them true against the drivers' own records, and 95 of them also proven by the clocks. This scope is a
decision that can be revisited (narrowing it to one thread or task would order fewer steps); Trailbeam
and the packages implement exactly what this section says.

Lambda wrapper: a `trailbeam receive` carries `trailbeam.order.returned` (a tick when the wrapper got the event,
with its end time) and **no** `trailbeam.order.issued`, because Lambda's poller made the call; a
`trailbeam outcome` carries `trailbeam.order.issued` = `trailbeam.order.returned` = one tick when the outcome was
decided, after the handler returned or threw. So a handler's outcome is ordered after its own receive,
and nothing is ordered after a Lambda receive by this source.

## 3. Attributes on every Trailbeam span

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `trailbeam.schema_version` | int | must | `3` |
| `trailbeam.kind` | string | must | `sqs.send`, `sqs.receive`, `sqs.settle` or `sqs.outcome` |
| `trailbeam.source` | string | must | `client` (client hooks) or `lambda` (Lambda wrapper) |
| `messaging.system.name` | string | must | `aws.sqs` |
| `messaging.operation.type` | string | must | from the table in section 1 |
| `messaging.operation.name` | string | must | the SQS API action: `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch`; or `LambdaEvent` for spans from the Lambda wrapper |
| `messaging.destination.name` | string | must | queue name, the last path segment of the queue URL, or the last `:` field of the event source ARN |
| `aws.sqs.queue.url` | string | must | queue URL (section 6) |
| `messaging.client.id` | string | must | consumer or producer identity (section 5) |
| `process.pid` | int | must | OS process ID |
| `thread.name` | string | should (Python) | `threading.current_thread().name`; not set in Node.js |
| `trailbeam.order.scope` | string | must (v3) | the process's call-order counter (section 2a) |
| `trailbeam.order.issued` | int | must (v3, except Lambda receive) | counter tick just before the request was sent (section 2a) |
| `trailbeam.order.returned` | int | must (v3) | counter tick when the response or error arrived (section 2a) |
| `trailbeam.start_observed` | bool | must (Lambda receive) | `false` when the span's start time is not the start of the SQS call (section 2). Omitted otherwise: absent means the start was observed. |

The `messaging.*`, `aws.sqs.queue.url`, `process.pid` and `thread.name` names are OpenTelemetry
semantic conventions; `trailbeam.*` names are ours. `messaging.system.name` replaces the deprecated
`messaging.system`; the stock instrumentations seen in QV-11 still emit `messaging.system` with the
old value `aws_sqs`, which is one more reason Trailbeam reads only Trailbeam spans.

Absent values: an attribute without a value **must** be omitted, never sent as `""`, `0` or `-1`.
(The prototypes sent empty strings and `-1`; that is dropped.)

## 4. Attributes per span

### 4.1 `trailbeam send`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.message.id` | string | must | `MessageId` from the response |
| `trailbeam.batch_entry_id` | string | must for batch | the entry `Id` from `SendMessageBatch` |

Emitted from the response, only for messages SQS accepted. Failed batch entries have no message ID
and produce no `trailbeam send` span in 0.1.

### 4.2 `trailbeam receive`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.message.id` | string | must | `MessageId` (`messageId` in a Lambda record) |
| `trailbeam.receive_count` | int | must | system attribute `ApproximateReceiveCount` |
| `trailbeam.receipt_handle.hmac` | string | must | keyed hash of this delivery's receipt handle (section 7) |
| `trailbeam.receipt_handle.key_id` | string | must | ID of the key used (section 7) |
| `trailbeam.sent_timestamp_ms` | int | should | system attribute `SentTimestamp`, epoch milliseconds |
| `trailbeam.first_receive_timestamp_ms` | int | may | system attribute `ApproximateFirstReceiveTimestamp`, epoch milliseconds |
| `trailbeam.aws_trace_header` | string | may | system attribute `AWSTraceHeader`, verbatim |
| `trailbeam.dead_letter_queue_source_arn` | string | must when SQS returns it (3.1) | system attribute `DeadLetterQueueSourceArn`, verbatim: the ARN of the queue SQS moved the message from. SQS sets it only on a receive from a dead-letter queue (section 9b). |
| `trailbeam.queue.arn` | string | must (Lambda) | the record's `eventSourceARN`, verbatim (section 6) |
| `faas.name` | string | must (Lambda) | function name, from the context, else `AWS_LAMBDA_FUNCTION_NAME` |
| `faas.invocation_id` | string | must (Lambda) | `aws_request_id` / `awsRequestId` |

- **Client hooks** **must** add `ApproximateReceiveCount`, `SentTimestamp`,
  `ApproximateFirstReceiveTimestamp`, `AWSTraceHeader` and (3.1) `DeadLetterQueueSourceArn` to the request's
  `MessageSystemAttributeNames` before the call, unless the caller already asked for them or for
  `All`. They **must not** remove anything the caller asked for, and **must not** request message
  attributes or read the body. (The prototypes also requested a `tenant` message attribute and read
  the body; message contents are out of scope for 0.1.)
- **Lambda wrapper** copies `DeadLetterQueueSourceArn` from the record's `attributes` when it is there.
  Whether Lambda's SQS records carry it is **not verified**; the AWS sample events do not show it.
- **Lambda wrapper** emits one `trailbeam receive` per record **before** the user handler runs, with
  `trailbeam.start_observed = false` (section 2), and flushes it, so a timeout or out-of-memory kill cannot lose it, as the QV-9 and QV-11 prototype wrappers did.
- A `ReceiveMessage` that returns no messages produces no Trailbeam span.

### 4.3 `trailbeam delete` and `trailbeam change_visibility`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `trailbeam.receipt_handle.hmac` | string | must | keyed hash of the receipt handle in the request |
| `trailbeam.receipt_handle.key_id` | string | must | ID of the key used |
| `messaging.message.id` | string | should | the message ID, if this process received the handle (section 8); otherwise omitted |
| `trailbeam.batch_entry_id` | string | must for batch | the entry `Id` |
| `trailbeam.visibility_timeout` | int | must (`change_visibility`) | `VisibilityTimeout` from the request, in seconds. `0` is a real value: it makes the message visible again at once. |
| `trailbeam.result` | string | must | `ok` if SQS returned success for this handle or entry, `failed` otherwise |
| `trailbeam.error_code` | string | must if failed | the SQS error code, such as `ReceiptHandleIsInvalid`, or the SDK error name if there is no code |

Settle spans are emitted **after** the response, so that `trailbeam.result` is known. (The prototypes
emitted them from the request hook, before the result.) If the call throws before any response,
one span per handle is still emitted with `trailbeam.result = failed`.

`trailbeam.result = ok` means SQS accepted the call, **not** that the message was removed. The
[DeleteMessage API reference](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html)
says: "If you use an old `ReceiptHandle`, the request will succeed, but the message might not be
deleted." Trailbeam therefore treats a delete as final only when its `trailbeam.receipt_handle.hmac`
matches the hash of the message's **latest** `trailbeam receive`. A delete that matches an earlier
receive is shown as a delete with a stale receipt handle.

### 4.4 `trailbeam outcome`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.message.id` | string | must | record `messageId` |
| `trailbeam.outcome` | string | must | one of the values below |
| `trailbeam.error_type` | string | must if `failed_exception` | the exception's class name, such as `RuntimeError` or `TypeError` |
| `faas.name` | string | must | function name |
| `faas.invocation_id` | string | must | `aws_request_id` / `awsRequestId` |

`trailbeam.outcome` values, decided per record after the handler returns:

| Value | Meaning |
|---|---|
| `success` | the handler returned normally, and this record was not in a valid `batchItemFailures` list. Lambda's event source mapping then deletes the message; that delete is **not observed**, and Trailbeam marks it as inferred. |
| `failed_reported` | the record's ID was in `batchItemFailures` |
| `failed_invalid_response` | `batchItemFailures` contained an `itemIdentifier` that is not a record ID in this batch; Lambda treats this as a failure of the whole batch, so every record gets this value |
| `failed_exception` | the handler threw; every record gets this value |

The exception **message** is not recorded, only its class name, because messages often contain
message contents. (The prototypes recorded `repr(e)`.) If the function times out or runs out of
memory, no `trailbeam outcome` is emitted; Trailbeam infers it from the `trailbeam receive` with no outcome.
The prototype value `success_deleted_by_lambda` is renamed `success`, because the delete is not seen.

### 4.5 What Trailbeam requires

Trailbeam ([`otlp.py`](../python/src/trailbeam/server/otlp.py)) reads a span only if its instrumentation scope
is `trailbeam`. It counts spans of other scopes as ignored. It rejects a Trailbeam span, and reports the reason in
the OTLP `partial_success` field, when any of these is true:

- `trailbeam.schema_version` is not `2` or `3`;
- `trailbeam.kind` is unknown, or `sqs.settle` comes with a `messaging.operation.name` that is not a delete
  or visibility-change action;
- the trace ID, span ID, start time or end time is missing, or the span ends before it starts;
- a required field for its kind is missing or has the wrong type:

  | Kind | Required attributes |
  |---|---|
  | send | `messaging.destination.name`, `messaging.message.id` |
  | receive | `messaging.destination.name`, `messaging.message.id`, `trailbeam.receive_count`, `trailbeam.receipt_handle.hmac`, `trailbeam.receipt_handle.key_id` |
  | delete, change_visibility | `messaging.destination.name`, `trailbeam.receipt_handle.hmac`, `trailbeam.receipt_handle.key_id`, `trailbeam.result` |
  | outcome | `messaging.destination.name`, `messaging.message.id`, `trailbeam.outcome` |

- `trailbeam.result` is not `ok` or `failed`, or `trailbeam.outcome` is not one of the four values in section 4.4.

Trailbeam reads integer attributes sent as decimal strings as integers. It keeps every attribute of
the span as JSON in the row, and uses only the ones this contract names.

## 5. Consumer identity

`messaging.client.id` identifies the process, or Lambda execution environment, that emitted the
span. Trailbeam uses it to tell consumers apart (duplicate deliveries to different consumers,
which consumer held a message when it got stuck).

- **Client hooks:** the `service.instance.id` resource attribute of the package's tracer provider,
  if set (for example through `OTEL_RESOURCE_ATTRIBUTES`). Otherwise a random UUID v4, which the
  package also sets as `service.instance.id`. The package generates it when it creates the tracer
  provider: on each `install` call, and once for each wrapped Lambda handler.
- **Lambda wrapper:** the `AWS_LAMBDA_LOG_STREAM_NAME` environment variable, which names one
  execution environment. If it is not set, the wrapper's `service.instance.id` as above.

Trailbeam also displays the resource attribute `service.name` (**must** be set; the packages
default it to `OTEL_SERVICE_NAME`, then to `unknown_service`), `process.pid` and `thread.name`.
`thread.name` matters in Python, where several consumer threads share one process: QV-9 and QV-11
ran three processes of four consumers each.

## 6. Queue URL

`aws.sqs.queue.url` is the queue URL:

- **Client hooks:** the `QueueUrl` request parameter, verbatim.
- **Lambda wrapper:** built from the record's `eventSourceARN` (`arn:aws:sqs:<region>:<account>:<name>`)
  as `https://sqs.<region>.amazonaws.com/<account>/<name>`. The wrapper also sets `trailbeam.queue.arn`
  to the ARN.

Trailbeam groups messages by `messaging.destination.name`, not by URL, because one queue can be
reached by more than one URL (an emulator on `localhost` and on a Docker network name, or AWS's
legacy and current endpoint forms). In 0.1, two queues with the same name in different accounts or
regions are therefore merged; this is a known limitation.

## 7. Receipt-handle hash

A receipt handle lets anyone who holds it delete the message or change its visibility while the
handle is valid, so the packages **must not** export receipt handles. They export a keyed hash
instead, which is enough for Trailbeam to join a delete to the receive it belongs to.

```
trailbeam.receipt_handle.hmac   = lower_hex(HMAC-SHA256(key, receipt_handle))[0:32]
trailbeam.receipt_handle.key_id = lower_hex(HMAC-SHA256(key, "trailbeam key id"))[0:8]
```

Both `key` and `receipt_handle` are encoded as UTF-8. 32 hex characters is 128 bits.

**Configuring the key**

- Environment variable `TRAILBEAM_RECEIPT_HASH_KEY`. Its value, as UTF-8 bytes, is the key. Each package
  also accepts the key as an explicit argument to its install or wrap function, which takes
  precedence over the environment variable.
- Every service that receives or settles messages on the same queues **must** use the same key,
  or Trailbeam cannot join their deletes to their receives. `trailbeam.receipt_handle.key_id` lets
  Trailbeam detect a key mismatch and show it, instead of showing missing deletes.
- If no key is configured, the packages use the published default key
  `trailbeam-default-receipt-hash-key` and log one warning per process. Trailbeam shows a notice for
  spans with the default key's ID, `b5e633f7`. The default exists so the local demo works without
  setup; it gives no protection.
- A key shorter than 16 bytes is used, with one warning per process.
- The key is a secret. It **must not** be committed, logged or exported in spans.

Why a keyed hash rather than a plain SHA-256 (what the prototypes used, truncated to 16 hex
characters): a receipt handle is long and random, so a plain hash is not reversible either. The key
adds two things [judgment]: someone who finds a receipt handle elsewhere, such as in an
application log, cannot match it to a Trailbeam record without the key; and hashes from different
deployments cannot be linked to each other. The attribute name changed from
`qv2.receipt_handle.sha256` (the prototypes, under the product's old name) to `trailbeam.receipt_handle.hmac` so prototype data and 0.1 data cannot
be mixed by accident.

**Test vectors** are in [`span-contract-vectors.json`](span-contract-vectors.json). The Python and
Node.js tests in this repository check them with each language's standard library, and each
package's hash function **must** be tested against them.

## 8. Message ID on settle spans

`DeleteMessage` and `ChangeMessageVisibility` take a receipt handle, not a message ID. The client
hooks keep an in-process map from receipt handle to message ID, filled by `ReceiveMessage`
responses, and set `messaging.message.id` on a settle span when the handle is in the map. The map
**must** be safe for concurrent use and **must** be bounded; entries **should** be dropped on a
successful delete and after 12 hours, the longest SQS visibility timeout.

When the handle is not in the map (another process received it, the process restarted, or the
entry was evicted), the span carries only the hash. Trailbeam joins it to a message through the
`trailbeam receive` with the same `trailbeam.receipt_handle.hmac`. Trailbeam **must** support this join; the
map is an optimisation, not a requirement for a correct timeline.

## 9. What Trailbeam infers

These events have no span. Trailbeam shows each one as **inferred**, never as observed:

- **Visibility timeout expired:** a `trailbeam receive` on a queue follows an earlier `trailbeam receive` on the
  same queue with a lower `trailbeam.receive_count`. A delete that SQS accepted for the earlier delivery
  does not prevent it: the later receive shows the delete came too late.
- **Deliveries not observed:** `trailbeam.receive_count` goes up by more than 1 between two receives on one
  queue, or the first receive on the source queue has `trailbeam.receive_count` above 1. A consumer without
  Trailbeam, or lost spans, made the missing receives.
- **Deleted by Lambda:** after `trailbeam outcome = success`.
- **Lambda invocation killed (timeout or out of memory):** a Lambda `trailbeam receive` with no `trailbeam outcome`
  for the same `faas.invocation_id`, once Trailbeam's configured Lambda timeout has passed
  (`--lambda-timeout`, default 900 s, the Lambda maximum). Trailbeam does not know the function's
  real timeout.

The move to a dead-letter queue is **not** inferred (3.1). It is shown only when SQS reports it
(section 9b).

## 9a. Order of steps in Trailbeam

Trailbeam shows step A before step B only when at least one observed fact, a **source**, proves it.
Every ordered edge records its sources; Trailbeam's API and screen show them ([API reference](https://docs.trailbeam.app/reference/api)).

| Source | A before B when |
|---|---|
| `clock` | `A.end < B.start` on the hosts' clocks (section 2). |
| `process_order` | Same `trailbeam.order.scope` and `A.order.returned < B.order.issued` (section 2a). The scope is one process (Python) or realm (Node.js), shared by all its threads and async tasks. |
| `receive_count` | A and B are receives of the message on one queue, and A's `trailbeam.receive_count` is lower: SQS's delivery n came before delivery n + 1. |
| `sqs_timestamps` | A is the send, B is the receive with `trailbeam.receive_count = 1` on the send's queue, and B's `trailbeam.sent_timestamp_ms` is lower than its `trailbeam.first_receive_timestamp_ms`. Both are SQS's clock; Trailbeam never compares them with a host clock. The same millisecond proves nothing. |

Order is transitive (A before B by one source, B before C by another), and the edges show the chain.
Steps that no chain orders are shown as **concurrent**: Trailbeam lists the steps in an order that agrees
with every edge and draws a box labelled "concurrent, order unknown" around each run that can't be cut
into "everything here is before everything after". Trailbeam never re-orders steps by what caused what:
"a receive comes after its send" or "a release comes before its redelivery" are not sources. If the
sources contradict each other (a cycle), Trailbeam shows no order between the steps on the cycle and says
so. Inferred steps (section 9) are placed between the observed steps they were inferred from, and are
ordered against other steps only through those anchors.

The `clock` source relies on every process reading the same clock. It holds for processes on one host
(the local demo) and, to within the hosts' clock offset, across hosts; Trailbeam does not correct for
clock offset between hosts in 0.1 (a known limit). `receive_count` relies on SQS's
`ApproximateReceiveCount`, which SQS calls approximate; `sqs_timestamps` relies on SQS's timestamps
coming from one clock, which is a single host in ElasticMQ and a distributed service on AWS [estimate:
not measured on AWS].

## 9b. The move to a dead-letter queue (3.1)

SQS makes no API call that Trailbeam could observe when it moves a message to a dead-letter queue. It
reports the move afterwards: the next receive of the message, on the dead-letter queue, carries the
same `MessageId` and the system attribute `DeadLetterQueueSourceArn` naming the queue it came from.

- **Observed move:** a `trailbeam receive` with `trailbeam.dead_letter_queue_source_arn` whose last `:` field is
  the name of another queue. Trailbeam shows one event "moved to the dead-letter queue" for each
  pair of source queue and dead-letter queue, observed, with source `sqs_dlq_source`. The earliest
  dead-letter receive that carries the attribute reports it. The event has no time of its own (SQS
  gives none). It is placed after the last receive on the source queue (or the send, if no receive
  there was observed) and before the reporting receive. It is ordered against other events only
  through those two and the proven edges of section 9a.
- **No report, no move.** A receive on another queue without the attribute (an older package, a
  consumer without Trailbeam, or an SQS that does not set it) shows no move. Trailbeam **must not** infer one
  from the message ID alone.
- **Not seen on the DLQ yet:** when a message's highest observed `trailbeam.receive_count` on its source
  queue has reached the queue's `maxReceiveCount` and no receive carrying the attribute is stored,
  Trailbeam says "receive count exceeded `maxReceiveCount`; not seen on the DLQ yet" and shows no
  move. Trailbeam does not know `maxReceiveCount` from spans; it is configured
  ([API reference](https://docs.trailbeam.app/reference/api)). Trailbeam **must not** receive from the dead-letter queue to check: a receive
  changes the message's state (receive count, visibility, receipt handle).
- **Moves back** (redrive) are out of scope for 3.1: a receive on the source queue after the
  dead-letter queue shows no move step.

Verified on ElasticMQ 1.6.15 (QV-31): the dead-letter receive carries `DeadLetterQueueSourceArn`, the
same `MessageId`, the original `SentTimestamp` and `ApproximateFirstReceiveTimestamp`. The attribute
is in botocore's SQS model and in `@aws-sdk/client-sqs`'s `MessageSystemAttributeName`. Not verified:
the same behaviour on AWS SQS, and whether the receive count carries over there.

## 10. Example

A `trailbeam receive` from the Python client hooks, as attributes (not OTLP wire format):

```json
{
  "name": "trailbeam receive",
  "scope": {"name": "trailbeam", "version": "0.1.0.dev0"},
  "resource": {"service.name": "orders-worker", "service.instance.id": "6f1c2a1e-6e9f-4b7e-9a0c-2f3b6b9d1c11"},
  "attributes": {
    "trailbeam.schema_version": 3,
    "trailbeam.kind": "sqs.receive",
    "trailbeam.source": "client",
    "messaging.system.name": "aws.sqs",
    "messaging.operation.type": "receive",
    "messaging.operation.name": "ReceiveMessage",
    "messaging.destination.name": "orders",
    "aws.sqs.queue.url": "http://localhost:9324/000000000000/orders",
    "messaging.client.id": "6f1c2a1e-6e9f-4b7e-9a0c-2f3b6b9d1c11",
    "process.pid": 41233,
    "thread.name": "worker-2",
    "messaging.message.id": "5fea7756-0ea4-451a-a703-a558b933e274",
    "trailbeam.receive_count": 2,
    "trailbeam.receipt_handle.hmac": "b8cf1504a45e5fadf383d469cfa5ae3e",
    "trailbeam.receipt_handle.key_id": "05769f58",
    "trailbeam.sent_timestamp_ms": 1763080041095,
    "trailbeam.first_receive_timestamp_ms": 1763080041120,
    "trailbeam.order.scope": "0b6a52d1-2a59-4f86-9a61-0d4bd1b8b9f4",
    "trailbeam.order.issued": 1841,
    "trailbeam.order.returned": 1846
  },
  "start_time_unix_nano": 1763080041102331000,
  "end_time_unix_nano": 1763080041131877000
}
```

The span's start and end are the interval of the `ReceiveMessage` call that returned the message
(here a long poll that waited 30 ms). A `trailbeam receive` from the Lambda wrapper has the same
attributes with `"trailbeam.source": "lambda"`, `"messaging.operation.name": "LambdaEvent"`,
`"trailbeam.start_observed": false`, the `faas.*` attributes, and equal start and end.

## 11. Where this came from

Source: the hooks in the QV-9 and QV-11 evidence bundles (`pyrun.py`, `node__noderun.js`,
`bin__hooks.py`, `bin__b2node.js`, `lambda__py__src__app.py`, `lambda__node__src__index.js`), and a
tally of the 1,820 spans with scope `qv2.hooks` that the client hooks exported in QV-11
(`raw__otel__ours_all_spans.jsonl`). The prototypes ran under the product's old name, so their names
start with `trailbeam` where this contract's start with `trailbeam`; prototype names below are as recorded.

Kept from the prototypes (with the prefix renamed): span names `trailbeam send`, `trailbeam receive`, `trailbeam outcome`; `trailbeam.kind` values;
`trailbeam.receive_count`, `trailbeam.batch_entry_id`, `trailbeam.visibility_timeout`, `trailbeam.aws_trace_header`;
`messaging.message.id` and `messaging.destination.name`; the handle-to-ID map; the Lambda wrapper's
receive-before-handler order and its outcome rules; a separate always-on tracer provider.

Changed, each described where it is defined above:

| Prototype | Contract v1 |
|---|---|
| `qv2.receipt_handle.sha256`, plain SHA-256, 16 hex | `trailbeam.receipt_handle.hmac`, HMAC-SHA256, 32 hex, plus `trailbeam.receipt_handle.key_id` |
| `trailbeam delete <queue>`, `trailbeam change_visibility <queue>` | `trailbeam delete`, `trailbeam change_visibility` |
| settle spans from the request hook | from the response, with `trailbeam.result` and `trailbeam.error_code` |
| `qv2.operation` | `messaging.operation.name` |
| queue name only | also `aws.sqs.queue.url` |
| `process.pid`, `thread.name`, `service.name` only | also `messaging.client.id` |
| `qv2.sent_timestamp` as a string | `trailbeam.sent_timestamp_ms` as an int |
| `qv2.t_ns` | dropped; span start time is used |
| `qv2.tenant`, `qv2.tenant.source`, `tenant` message attribute | dropped (message contents are out of scope for 0.1); the name `trailbeam.tenant` is reserved |
| `qv2.error = repr(e)` | `trailbeam.error_type`, class name only |
| `success_deleted_by_lambda` | `success` |
| empty strings and `-1` for absent values | attribute omitted |
| Node.js hooks without batch settle or tenant | Node.js and Python emit the same spans |

Semantic-convention names (`messaging.system.name` and its value `aws.sqs`,
`messaging.operation.type` and its values, `messaging.operation.name`, `messaging.client.id`,
`messaging.message.id`, `messaging.destination.name`, `aws.sqs.queue.url`) were checked against
[`model/messaging/registry.yaml`](https://github.com/open-telemetry/semantic-conventions/blob/main/model/messaging/registry.yaml)
and the [AWS attribute registry](https://opentelemetry.io/docs/specs/semconv/registry/attributes/aws/)
on 2026-11-13. All of them are at `development` stability and may still be renamed upstream; if
they are, this contract keeps its names until a schema version bump.

## 12. Changes in schema version 2

Schema version 1 (the 0.1 build up to QV-28) made every span zero-length, stamped when the response
arrived, and Trailbeam sorted by that stamp. SQS acts **before** its response returns, so under
concurrency the stamps of two calls can come in the opposite order to what SQS did: in the QV-20
concurrency check, 4 to 32% of messages showed a receive before its send, or a redelivery before the
`ChangeMessageVisibility` that released it.

| Version 1 | Version 2 |
|---|---|
| `trailbeam.schema_version = 1` | `2`. Trailbeam rejects version 1 spans: their single time is the response time, which cannot be ordered correctly. |
| zero-length span at the response | client hooks: interval from just before the request to the response (section 2) |
| Lambda `trailbeam receive` at the wrapper's start, read as the receive time | zero-length at that moment, `trailbeam.start_observed = false`: an upper bound only |
| Node.js time from `performance.timeOrigin + performance.now()` | `Date.now()`, the same system clock as other processes |
| Trailbeam's order: by time, and a settle placed after its receive | concurrent groups (section 9a); nothing re-ordered by cause |

Unchanged: span names, kinds and every other attribute, the receipt-handle hash and its test
vectors, consumer identity, and Trailbeam's joins.

## 13. Changes in schema version 3

Schema version 2 could order steps only by the clocks, and left most of them concurrent: in the QV-29
recheck, send before receive in 7 of 1,374 pairs and release before redelivery in 0 of 174 on a local
machine (QV-31).

| Version 2 | Version 3 |
|---|---|
| `trailbeam.schema_version = 2` | `3`. Trailbeam still reads version 2 spans; they carry no call order, so only the other sources order them. |
| no call order | `trailbeam.order.scope`, `trailbeam.order.issued`, `trailbeam.order.returned` (section 2a) |
| Trailbeam's order: clock intervals only | four sources, each recorded on the edge it proves (section 9a) |

Unchanged: span names, kinds, times and every other attribute. The client hooks already asked SQS for
`SentTimestamp` and `ApproximateFirstReceiveTimestamp` (section 4.2), and the Lambda wrapper already
copied them from the record. ElasticMQ 1.6.15 fills both (checked for QV-31: the first-receive timestamp
is set at the first receive and kept on later ones).

## 14. Changes in revision 3.1

| 3.0 | 3.1 |
|---|---|
| Trailbeam inferred a move to the dead-letter queue from the message ID on another queue | the move is shown only when SQS reports it (section 9b), source `sqs_dlq_source`; nothing is inferred |
| receive hooks asked for four system attributes | also `DeadLetterQueueSourceArn`, recorded as `trailbeam.dead_letter_queue_source_arn` |

`trailbeam.schema_version` stays `3`: the attribute is optional, a Trailbeam built for contract 3.0 ignores it, and one built for 3.1
reads 3.0 spans (they simply never show a move).
