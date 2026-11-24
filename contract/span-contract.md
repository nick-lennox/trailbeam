# Trailbeam span contract, schema version 3 (revision 3.3)

This file defines the OpenTelemetry spans that the `trailbeam` Python package, the `trailbeam` Node.js
package and the Java jar `trailbeam-aws-sdk-v2` emit, and that the Trailbeam reads. The packages and Trailbeam
are built and tested against it; what is particular to Java is in section 20. A change to anything below is a
contract change: bump `trailbeam.schema_version`, and update this file, every package and Trailbeam in the same
change. A revision (3.1) only adds an optional attribute that
older stores ignore and that Trailbeam does not require, or (3.3) an optional span; it keeps
`trailbeam.schema_version = 3` (sections 14, 16 and 19). Section 17 (the test span of `trailbeam doctor`) is not emitted by the packages and changes nothing they
send.

The keywords **must**, **must not**, **should** and **may** are used as in RFC 2119.

**Schema version 4 (revision 4.0)** adds broker-neutral kinds (`<family>.<operation>`) and the RabbitMQ
(AMQP 0-9-1) spans, in [`span-contract-amqp.md`](span-contract-amqp.md). It changes nothing in the SQS and
SNS spans below: their packages keep sending `trailbeam.schema_version = 3`, and a 4.0 Trailbeam also
accepts `4` on them with the same meaning.

## 1. Summary

| Span name | `trailbeam.kind` | `messaging.operation.type` | Emitted by | When |
|---|---|---|---|---|
| `trailbeam send` | `sqs.send` | `send` | client hooks | once per message that SQS accepted in `SendMessage` / `SendMessageBatch` |
| `trailbeam receive` | `sqs.receive` | `receive` | client hooks, Lambda wrapper | once per message returned by `ReceiveMessage`, or once per record in a Lambda SQS event |
| `trailbeam delete` | `sqs.settle` | `settle` | client hooks | once per receipt handle in `DeleteMessage` / `DeleteMessageBatch` |
| `trailbeam change_visibility` | `sqs.settle` | `settle` | client hooks | once per receipt handle in `ChangeMessageVisibility` / `ChangeMessageVisibilityBatch` |
| `trailbeam outcome` | `sqs.outcome` | `process` | Lambda wrapper | once per record, after the handler returns or throws |
| `trailbeam purge` | `sqs.purge` | `purge` | client hooks | once per `PurgeQueue` call, after the response or error (3.3, section 18) |

"Client hooks" are the boto3 hooks (Python), the AWS SDK v3 hooks (Node.js) and the global
`ExecutionInterceptor` for the AWS SDK for Java v2 (Java, section 20). "Lambda wrapper" is the handler wrapper
in each package. In 0.1, `trailbeam outcome` comes only from the Lambda wrappers; for
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
  Node.js), so the Trailbeam span shares its trace ID. Java reads it from the request's trace header instead,
  and a `ReceiveMessage` in a polling loop has none (section 20.1). In the Lambda wrapper, the active span at
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

**The scope is the whole process (Python), the whole realm (Node.js) or the class loader (Java, section 20.2), not one thread or async task.**
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
and nothing is ordered *before* a Lambda receive by this source: it has no `issued` tick. The clocks
don't order anything before it either, because its start is not observed (`trailbeam.start_observed`
= `false`). Its end time and `returned` tick can still order it before later steps.

## 3. Attributes on every Trailbeam span

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `trailbeam.schema_version` | int | must | `3` |
| `trailbeam.kind` | string | must | `sqs.send`, `sqs.receive`, `sqs.settle`, `sqs.outcome` or (3.3) `sqs.purge` |
| `trailbeam.source` | string | must | `client` (client hooks) or `lambda` (Lambda wrapper) |
| `messaging.system.name` | string | must | `aws.sqs` |
| `messaging.operation.type` | string | must | from the table in section 1 |
| `messaging.operation.name` | string | must | the SQS API action: `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch`, (3.3) `PurgeQueue`; or `LambdaEvent` for spans from the Lambda wrapper |
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
  `ApproximateFirstReceiveTimestamp`, `AWSTraceHeader`, (3.1) `DeadLetterQueueSourceArn` and (3.2)
  `MessageGroupId` and `SequenceNumber` to the request's `MessageSystemAttributeNames` before the call,
  unless the caller already asked for them or for `All`. They **must not** remove anything the caller
  asked for. The only message attribute they may add to `MessageAttributeNames` is the configured tenant
  attribute (3.2, section 15.2). What they read from the body, and when they export it, is section 15.
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
| `trailbeam.error_type` | string | must if `failed_exception` | the exception's class name, such as `RuntimeError` or `TypeError`; in Java the fully qualified name, such as `java.lang.RuntimeException` (section 20.3) |
| `trailbeam.error_message` | string | may (3.2) | the exception's message, redacted, only for queues with error text turned on (section 15.5) |
| `faas.name` | string | must | function name |
| `faas.invocation_id` | string | must | `aws_request_id` / `awsRequestId` |

`trailbeam.outcome` values, decided per record after the handler returns:

| Value | Meaning |
|---|---|
| `success` | the handler returned normally, and this record was not in a valid `batchItemFailures` list. Lambda's event source mapping then deletes the message; that delete is **not observed**, and Trailbeam marks it as inferred. |
| `failed_reported` | the record's ID was in `batchItemFailures` |
| `failed_invalid_response` | `batchItemFailures` contained an `itemIdentifier` that is not a record ID in this batch; Lambda treats this as a failure of the whole batch, so every record gets this value |
| `failed_exception` | the handler threw; every record gets this value |

The exception **message** is not recorded by default, only its class name, because messages often
contain message contents. (The prototypes recorded `repr(e)`.) From 3.2 it is recorded, redacted, for
queues the user turned error text on for (section 15.5). If the function times out or runs out of
memory, no `trailbeam outcome` is emitted; Trailbeam infers it from the `trailbeam receive` with no outcome.
The prototype value `success_deleted_by_lambda` is renamed `success`, because the delete is not seen.

### 4.5 What Trailbeam requires

Trailbeam ([`otlp.py`](../python/src/trailbeam/server/otlp.py)) reads a span only if its instrumentation scope
is `trailbeam`. It counts spans of other scopes as ignored. A span of kind `trailbeam.probe` (the test span of
`trailbeam doctor`) is checked only as section 17 says; no other rule of this section applies to it. It
rejects any other Trailbeam span, and reports the reason in the OTLP `partial_success` field, when any of these is
true:

- `trailbeam.schema_version` is not `2` or `3` (from 4.0: `2`, `3` or `4`, and only `4` on an `amqp.*` span;
  [`span-contract-amqp.md`](span-contract-amqp.md) section A1);
- `trailbeam.kind` is unknown, `sqs.settle` comes with a `messaging.operation.name` that is not a delete
  or visibility-change action, or (3.3) `sqs.purge` comes with one that is not `PurgeQueue`;
- the trace ID, span ID, start time or end time is missing, or the span ends before it starts;
- a required field for its kind is missing or has the wrong type:

  | Kind | Required attributes |
  |---|---|
  | send | `messaging.destination.name`, `messaging.message.id` |
  | receive | `messaging.destination.name`, `messaging.message.id`, `trailbeam.receive_count`, `trailbeam.receipt_handle.hmac`, `trailbeam.receipt_handle.key_id` |
  | delete, change_visibility | `messaging.destination.name`, `trailbeam.receipt_handle.hmac`, `trailbeam.receipt_handle.key_id`, `trailbeam.result` |
  | outcome | `messaging.destination.name`, `messaging.message.id`, `trailbeam.outcome` |
  | purge (3.3) | `messaging.destination.name`, `trailbeam.result` |

- `trailbeam.result` is not `ok` or `failed`, or `trailbeam.outcome` is not one of the four values in section 4.4.

A Trailbeam built before revision 3.3 rejects a `trailbeam purge` as an unknown kind, with that reason; nothing else
in the export is affected (section 18).

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
(section 9b). A `PurgeQueue` call is observed from 3.3 (section 18).

**Not seen at all: the end of the retention period.** SQS deletes a message that is still in the queue when
the queue's `MessageRetentionPeriod` (1 minute to 14 days) ends. No API call is made and no later receive
reports it, so no span can observe it and Trailbeam does not infer it. Such a message keeps its last observed
step, with no delete; Trailbeam's Stuck list says this can be the reason.

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
| `qv2.tenant`, `qv2.tenant.source`, `tenant` message attribute | dropped in 0.1; back in 3.2 as `trailbeam.tenant` and `trailbeam.tenant.source`, from a configured attribute or body path (section 15.2) |
| `qv2.error = repr(e)` | `trailbeam.error_type`, class name only; 3.2 adds an opt-in, redacted `trailbeam.error_message` |
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

## 15. Message contents (revision 3.2)

Revision 3.2 reads message contents for the first time: body size, the tenant, FIFO fields, an
opt-in captured body and opt-in Lambda error text. Every attribute in this section is optional.
Trailbeam builds for 3.0 and 3.1 ignore them, and a 3.2 Trailbeam reads older spans, which just
don't have them.

**The packages make no SQS calls of their own.** They read only the parameters and responses of the
calls the application makes, and the Lambda event. They never call `ReceiveMessage` to fetch a body,
including a "peek" with `VisibilityTimeout = 0`: a receive raises the message's receive count,
changes its visibility and issues a new receipt handle, so it can push the message to the
dead-letter queue early. They never call `ChangeMessageVisibility`, and never fetch an offloaded body
from S3. Each package has a test that fails if the hooks make an SQS request
(`python/tests/test_capture.py`, `node/test/capture.test.js`).

### 15.1 Configuration

Configuration is per process. Each option group is given to `trailbeam.boto.install` /
`trailbeam.aws_lambda.wrap` (Python) or `install` / `wrapHandler` (Node.js). If a group is not given,
the package reads its environment variables instead. A group that is given replaces its environment
variables as a whole.

| Option (Python / Node.js) | Environment variable | Default |
|---|---|---|
| `tenant.message_attribute` / `tenant.messageAttribute` | `TRAILBEAM_TENANT_ATTRIBUTE` | none |
| `tenant.body_path` / `tenant.bodyPath` | `TRAILBEAM_TENANT_BODY_PATH` | none |
| `capture.body_queues` / `capture.bodyQueues` | `TRAILBEAM_CAPTURE_BODY_QUEUES` (comma-separated) | none: no body is captured |
| `capture.error_queues` / `capture.errorQueues` | `TRAILBEAM_CAPTURE_ERROR_QUEUES` | none: no error text is recorded |
| `capture.redact_keys` / `capture.redactKeys` | `TRAILBEAM_REDACT_KEYS` | added to the default keys (15.3) |
| `capture.redact_patterns` / `capture.redactPatterns` | `TRAILBEAM_REDACT_PATTERNS` | all built-in patterns (15.3) |
| `capture.max_body_bytes` / `capture.maxBodyBytes` | `TRAILBEAM_CAPTURE_MAX_BYTES` | 16384; at most 262144 |

The Java jar has no install call: it reads only the environment variables (section 20.4).

A queue list holds queue names (`messaging.destination.name`), or `*` for every queue. Capture is
**opt-in per queue**: a body is exported only for a listed queue. Trailbeam adds a second gate when it
stores spans. That gate belongs to the server, not to this contract:

- The open-source server stores a body only for the queues in `TRAILBEAM_STORE_BODIES` (none by
  default). For other queues it removes `trailbeam.body`, `trailbeam.body.format`,
  `trailbeam.body.redacted_count` and `trailbeam.body.envelope` before writing, keeps
  `trailbeam.body.capture_status`, and marks the span as having had its body dropped.
- The hosted Trailbeam app stores a body only for queues an admin opted in under Settings, and shows
  bodies to owners and admins only.

How both show a body, or the reason there is none, and search bodies is in [body-view.md](body-view.md).

### 15.2 Attributes

On every `trailbeam send` and `trailbeam receive` (client hooks and Lambda wrapper), when the body is
known:

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.message.body.size` | int | must | UTF-8 length of the body in bytes. Message attributes are not counted. |
| `trailbeam.body.capture_status` | string | must | 15.4 |
| `trailbeam.body` | string | when captured | the redacted body (15.3) |
| `trailbeam.body.format` | string | when captured | `json` if the body is a JSON document, else `text` |
| `trailbeam.body.redacted_count` | int | when captured | how many values redaction replaced; `0` is a real value |
| `trailbeam.body.offloaded_size` | int | when the body is a pointer and the size attribute is there | the SQS Extended Client's payload size in bytes, from the message attribute `ExtendedPayloadSize`, else the legacy `SQSLargePayloadSize`: a `StringValue` of 1 to 15 digits. Set only when the body is a pointer (15.4, the class-name test), with or without capture. `messaging.message.body.size` stays the size of the pointer itself. |
| `trailbeam.body.envelope` | string | may | `sns` (a JSON object with `"Type": "Notification"` and `TopicArn`) or `eventbridge` (with `detail-type`, `detail` and `source`): the body is a delivery envelope, not the producer's own payload |
| `trailbeam.tenant` | string | when configured and found | the tenant, at most 256 characters |
| `trailbeam.tenant.source` | string | with `trailbeam.tenant` | `message_attribute` or `body_path` |
| `trailbeam.message_group_id` | string | when SQS has one | `MessageGroupId`: from the request on a send, from the system attribute on a receive |
| `trailbeam.sequence_number` | string | when SQS returns one | `SequenceNumber` (FIFO queues): from the send response, or from the system attribute on a receive. A string, because it is a decimal number of up to 128 bits. |

Where each value comes from:

| | Send (client hooks) | Receive (client hooks) | Lambda wrapper |
|---|---|---|---|
| body | `MessageBody` of the request (`SendMessage`), or of the request entry whose `Id` equals the `Successful[]` entry's `Id` (`SendMessageBatch`) | `Messages[].Body` | `Records[].body` |
| MD5 checked against | `MD5OfMessageBody` of the response or `Successful[]` entry | `MD5OfBody` | `md5OfBody` |
| tenant attribute | `MessageAttributes[name].StringValue` of the request (entry) | `MessageAttributes[name].StringValue` | `messageAttributes[name].stringValue` |
| offloaded size | `MessageAttributes[ExtendedPayloadSize or SQSLargePayloadSize].StringValue` of the request (entry) | the same, of `Messages[].MessageAttributes` | `messageAttributes[...].stringValue` |
| sequence number | `SequenceNumber` of the response or entry | system attribute `SequenceNumber` | `attributes.SequenceNumber` |

A failed batch entry produces no `trailbeam send` (section 4.1), so it has no body either.

**Tenant.** The message attribute is tried first, then the body path. The body path is a dotted path of
object keys (`customer.id`) into a JSON body; the value must be a string or a number, and a number is
written as the language prints it, so `42` becomes `"42"` (use string IDs if they can exceed 2^53).
Reading the tenant from the body parses the body in the process. That does not export the body; only
capture does. For a receive, the hooks add the tenant attribute name to `MessageAttributeNames` unless
the caller already asked for it, `All` or `.*`, and do the same for `ExtendedPayloadSize` and
`SQSLargePayloadSize`. That changes what SQS returns, not the message's state; a message without
those attributes comes back as before.

The Lambda wrapper also sets, on every `trailbeam receive` and `trailbeam outcome`:

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `trailbeam.batch.size` | int | must (3.2) | number of SQS records in the event |
| `trailbeam.batch.index` | int | must (3.2) | 0-based position of this record among them |
| `trailbeam.batch.reported_failure_count` | int | outcome, when the handler returned (3.2) | how many records were in a valid `batchItemFailures` list, `0` when there was none. Omitted when the list was invalid (every record is then `failed_invalid_response`) and when the handler threw. |

These are not `messaging.batch.message_count`: the semantic conventions say not to set it on spans
that operate on a single message, and each of these spans is one record.

### 15.3 Redaction

Redaction runs in the package, before the span is exported. Both packages implement exactly the rules
below, and both are tested against the `capture` vectors in
[`span-contract-vectors.json`](span-contract-vectors.json).

**JSON bodies.** A body is JSON if it parses as one strict JSON document (RFC 8259), with
whitespace allowed around it. The package copies the body and rewrites only what it redacts, so the
output keeps the input's formatting and number text:

- A member whose key matches a **redact key** has its whole value (string, number, object or array)
  replaced by the string `"[REDACTED]"`. A key without a `.` matches the member's name at any depth. A
  key with a `.` matches the full path of object keys from the root (`customer.name`); arrays add no
  segment. Keys compare ASCII-case-insensitively. Each replaced value counts 1.
- Any other string value whose text, after whitespace, starts with `{` or `[` and is itself JSON (an SNS
  envelope's `Message`, for one) is redacted by these same rules. If that changed anything, it is
  written back as a JSON string.
- Any other string value gets the text rules below. A string that changes is written back as a JSON
  string, with no `\u` escapes beyond the ones JSON requires.
- Nesting deeper than 100 levels is a redaction failure.

**S3 pointers are not redacted.** A body that parses as exactly an SQS Extended Client pointer, a
two-item array whose first item is `"software.amazon.payloadoffloading.PayloadS3Pointer"` and whose
second is an object with exactly the members `s3BucketName` and `s3Key`, both strings, is exported
without any redaction: `redacted_count` is `0`. It holds no payload, and the patterns would rewrite
it (`long_number` matched 14,878 of 100,000 uuid4 keys, and every bucket name with a 12-digit
account ID), so the stored pointer would name an S3 object that does not exist (QV-60). It is
written back in one compact form, `["software.amazon.payloadoffloading.PayloadS3Pointer",{"s3BucketName":B,"s3Key":K}]`,
with `B` and `K` encoded as JSON strings as above, so that nothing beyond the bucket and the key
(the losing copy of a duplicate member, say) is exported. Any other shape, an extra member for one,
is redacted as usual. This applies to the whole body only, not to a pointer inside a string.

**Text bodies** (anything that is not JSON), and Lambda error messages, get the text rules:

1. `key=value` and `key: value` pairs for the redact keys without a `.`: the value (to the next space,
   tab, newline, `,`, `;` or `&`, or a whole `"..."` or `'...'`) is replaced by `[REDACTED]`.
2. The built-in patterns, in this order, each match replaced by `[REDACTED]`:

| Pattern | Matches |
|---|---|
| `aws_access_key` | `AKIA` or `ASIA` and 16 upper-case letters or digits |
| `bearer` | `Bearer ` or `bearer ` and the token after it |
| `email` | an email address |
| `long_number` | 9 or more digits, single spaces or hyphens allowed between them: card numbers, phone numbers, US SSNs. It also hides AWS account IDs and long order numbers; turn it off if that is too much. |

The regular expressions are in `_capture.py` and `capture.js`. They use ASCII classes only, so
Python's `re` and JavaScript's `RegExp` match the same strings.

Default redact keys: `password`, `passwd`, `secret`, `token`, `access_token`, `refresh_token`,
`authorization`, `api_key`, `apikey`, `ssn`, `card_number`, `cardnumber`, `cvv`, `cvc`. A configured
key may contain only `A-Z a-z 0-9 _ . -`; a package refuses anything else at install time, as it does
an unknown pattern name.

**Fail closed.** If redaction throws, or its result cannot be encoded as UTF-8 (a lone surrogate), the
body is not exported (`redaction_failed`). Redaction is pattern matching. It removes what the keys and
patterns describe, and nothing else; that is why capture is opt-in per queue.

### 15.4 `trailbeam.body.capture_status`

Set by a 3.2 package on every send and receive whose body it saw. Checked in this order:

| Value | Meaning |
|---|---|
| `off` | the queue is not in the body queue list |
| `md5_mismatch` | the MD5 of the body's UTF-8 bytes does not equal SQS's MD5 for the message, so the body the package holds is not the one SQS stored. Not exported. If there is no MD5 to check against, the check is skipped. |
| `redaction_failed` | 15.3. Not exported. |
| `exceeds_capture_limit` | the redacted body is longer than the capture limit in UTF-8 bytes. Not exported, not truncated. |
| `offloaded_to_s3` | the body is an SQS Extended Client pointer: a JSON array of two items whose first is `"software.amazon.payloadoffloading.PayloadS3Pointer"`. The pointer is exported as the body, unredacted if it has exactly the shape in 15.3; the payload in S3 is never fetched. Its size is `trailbeam.body.offloaded_size` (15.2). |
| `captured` | exported in `trailbeam.body` |

Trailbeam **should** work out two more reasons itself, for a message with no captured body: **producer not
instrumented** (no `trailbeam send`, and no receive captured it) and **package too old** (spans from
before 3.2, with no status). It shows the reason instead of an empty body.

**Which copy Trailbeam keeps.** With capture on, a body can arrive from the send and from each receive.
Trailbeam keeps one body per message ID, preferring the send's, because the producer's copy is the
payload before any delivery envelope. The receive copies are the fallback for producers that don't run
a Trailbeam package. The packages don't try to avoid the duplicate: telling a receive that its producer
was instrumented would mean adding a marker to the message, and the packages never change messages.

The default limit of 16 KiB, and the hard limit of 256 KiB, are a judgment, not measured. A body
travels as one span attribute in an OTLP export batch. The OpenTelemetry SDKs set no attribute length
limit by default, but large spans mean large export requests, and many collectors and Trailbeam's
ingest bound the request size [estimate: not measured]. SQS itself accepts bodies up to 1 MiB
(1,048,576 bytes) since August 2025
([announcement](https://aws.amazon.com/about-aws/whats-new/2025/08/amazon-sqs-max-payload-size-1mib),
[quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html)).

### 15.5 Lambda error text

When a handler throws, and the record's queue is in the error queue list, each record's
`trailbeam outcome` carries `trailbeam.error_message`: the thrown error's message (`str(e)` in Python;
`err.message` in Node.js, or the value itself if a string was thrown), after the text rules of 15.3,
cut to its first 1,024 Unicode code points. If redaction fails, the attribute is omitted. Error text
for other consumers is not in 3.2: they have no handler wrapper, so the packages never see the
exception.

Error text has one gate, the package's error queue list. Unlike bodies (15.1), the servers add no second
gate: `TRAILBEAM_STORE_BODIES` does not cover `trailbeam.error_message`, and the open-source server
stores it whenever a package sends it.

### 15.6 Verified and not verified

Verified:

- The FIFO record fields (`MessageGroupId`, `SequenceNumber`, `md5OfBody`, `messageAttributes`, `body`) are
  in the AWS sample events in
  [Using Lambda with Amazon SQS](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html) (checked
  2026-11-16).
- `messaging.message.body.size` (int, bytes) and the advice not to set `messaging.batch.message_count` on
  single-message spans are from the
  [messaging attribute registry](https://opentelemetry.io/docs/specs/semconv/registry/attributes/messaging/)
  (checked 2026-11-16). The semantic conventions have no generic message group attribute, so the FIFO
  names are ours.
- The pointer class name `software.amazon.payloadoffloading.PayloadS3Pointer`, from
  [payload-offloading-java-common-lib-for-aws](https://github.com/awslabs/payload-offloading-java-common-lib-for-aws/blob/master/src/main/java/software/amazon/payloadoffloading/PayloadS3Pointer.java).
- Both packages produce identical redaction, tenant and error-text output for every vector.
- Body, MD5 check, tenant attribute and message group against moto's SQS (Python tests).

- The pointer as a two-item array with members `s3BucketName` and `s3Key`, and the
  `ExtendedPayloadSize` message attribute on the send request and the receive response, on real SQS
  and S3 with AWS's Python Extended Client (`amazon-sqs-extended-client` 1.0.1) (QV-60, 2026-11-16).

Not verified:

- The Java and Node.js Extended Clients' pointer bytes. The Python client writes the same class name
  and member names as the Java library.
- `SequenceNumber` on a real FIFO queue: moto returns none, so the tests check it on canned responses.
  ElasticMQ and AWS were not run for 3.2.
- How large a span Trailbeam's ingest and common collectors accept (15.4).

## 16. Changes in revision 3.2

| 3.1 | 3.2 |
|---|---|
| no message contents: "the hooks must not request message attributes or read the body" | body size always; tenant, FIFO group and sequence number when present; body and Lambda error text opt-in per queue, redacted in the package (section 15) |
| receive hooks asked for five system attributes | also `MessageGroupId` and `SequenceNumber`, and the configured tenant message attribute |
| Lambda spans did not say where a record sat in its batch | `trailbeam.batch.size`, `trailbeam.batch.index`, and on outcomes `trailbeam.batch.reported_failure_count` |
| exception class name only | plus `trailbeam.error_message`, opt-in and redacted |
| | before release (QV-63): an exact S3 pointer is exported unredacted (15.3); `trailbeam.body.offloaded_size` from the Extended Client's size attribute, which the receive hooks ask for |

`trailbeam.schema_version` stays `3`: every new attribute is optional, Trailbeam requires none of them
(section 4.5 is unchanged), and a Trailbeam built for 3.1 ignores them.

## 17. The test span of `trailbeam doctor` (QV-148)

`trailbeam doctor` checks that a span sent to ingest is stored and can be read back. It sends one **probe**: a
span in the `trailbeam` scope that the packages never emit, which Trailbeam stores apart from the spans so it
never shows as a message.

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `trailbeam.schema_version` | int | must | `2` or `3`, as any span (section 4.5) |
| `trailbeam.kind` | string | must | `trailbeam.probe` |
| `trailbeam.probe.id` | string | must | 8 to 64 characters, each a letter, a digit, `-` or `_` |

Trailbeam reads no other attribute of a probe, and not its times or trace: it records the probe's ID and when it
stored it (its own clock). A probe is **not** a span: it has no row, it is in no message, page, count, rollup,
retention pass or `last_row`/`last_span_ns`, and it is not counted as inserted. Probes are stored after the
spans of the same export, on the same writer (so a probe stored says the export path works), and are kept for
at least a day. `GET /api/probes/{id}` answers when it was stored, or 404 ([`openapi.yaml`](openapi.yaml)).

An export carries at most **8** probes (`trailbeam doctor` sends 1); Trailbeam rejects the rest, with the
reason. The probes of one export are stored in one write transaction, whose time counts toward backpressure as
the spans' write does.

A rejected probe is rejected as a span would be: it counts as one rejected span, in
`partial_success.rejected_spans` and in `GET /api/health` `ingest.rejected`, and its reason is in
`partial_success.error_message`. Each reason starts with fixed text, so `trailbeam doctor` tells the cases apart:

| Reason starts with | When |
|---|---|
| `probe span without a trailbeam.probe.id` | the probe has no valid `trailbeam.probe.id` |
| `probe span not stored:` | the store does not keep probes |
| `probe span over the limit of` | the export carries more than 8 probes |
| `unknown trailbeam.kind 'trailbeam.probe'` | the answer of a Trailbeam older than this section (0.3 and before); nothing else in that export is affected |

A wrong `trailbeam.schema_version` is rejected with the reason any span gets (section 4.5). `trailbeam doctor`
reports each case as such, and does not count its own rejected probe among the rejected spans it reports.

`trailbeam.schema_version` stays `3`: nothing the packages send changes.

## 18. `trailbeam purge` (revision 3.3)

`PurgeQueue` deletes every message in a queue, including messages in flight, without a receipt handle and
without a `DeleteMessage` per message. Before 3.3 no span recorded it, so a message sent and then purged kept
one "Sent" step and no delete, and Trailbeam showed it as stuck until retention removed it (QV-245).

The client hooks emit one `trailbeam purge` per `PurgeQueue` call, after the response or the error, with the
attributes of section 3 (its `messaging.operation.name` is `PurgeQueue`, its interval and call-order ticks
are the call's, section 2 and 2a) and these:

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `trailbeam.result` | string | must | `ok` if SQS returned success, `failed` otherwise |
| `trailbeam.error_code` | string | must if failed | the SQS error code as the SDK reports it, without a protocol namespace, such as `PurgeQueueInProgress` (an SDK may report the legacy query code instead, such as `AWS.SimpleQueueService.NonExistentQueue` for `QueueDoesNotExist`), or the SDK error name if there is no code |

A purge span carries no `messaging.message.id` and no receipt-handle hash: SQS does not say which messages it
deleted. If the call throws before any response, the span is still emitted, with `trailbeam.result = failed`.
`messaging.operation.type` is `purge`, a custom value: the OpenTelemetry messaging conventions define none for
purging a queue. Test vectors: `purge` in [`span-contract-vectors.json`](span-contract-vectors.json); each
package **must** produce exactly the attributes of each case.

**What SQS does.** The
[PurgeQueue API reference](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_PurgeQueue.html)
says the deletion "takes up to 60 seconds", that messages sent before the call "might be received but are
deleted within the next minute", and that messages sent after it "might be deleted while the queue is being
purged" (checked 2026-11-23). So a successful purge removes every message that was in the queue when the call started, and may
remove messages that arrive within 60 seconds of its end. Trailbeam calls that span of time, from the call's
start to 60 seconds after its end, the purge's **window**.

**How Trailbeam reads it.** Only `trailbeam.result = ok` purges count. A message is on the queue of its latest
receive, else of its send (its `current_queue` in the API). For a message on a purged queue that has no final
delete (section 4.3) and no delete inferred from a Lambda outcome:

- **Purged:** the purge started after the message's newest step ended, on the hosts' clocks (`clock`, section
  9a: newest step's end < purge's start). Trailbeam shows it as "Purged (observed: PurgeQueue at *start of the
  purge*)", not as stuck. If several purges qualify, the earliest one is shown. The purge is observed; that it
  removed this message is what SQS's documentation says it does, so the step says so and carries the purge's
  span. `process_order` is not used: two calls of one process are on one clock, and a purge made in another
  process can only be ordered by the clock.
- **Possibly purged:** not purged by the rule above, but the message's newest step ended inside a purge's window
  (from its start to 60 seconds after its end). SQS may or may not have deleted it. Trailbeam shows "possibly
  purged (PurgeQueue at *start*)". It stays on the Stuck list, with that label, because a message that was not
  purged and has no delete really is stuck.
- A message whose newest step ended after the window is not affected by the purge.

A step after the purge (a later receive) means the message was not purged by it, and the rules above are
applied to that newest step. A failed purge is stored and changes no message. The purge is shown on the
message's timeline as an observed step of the queue (`queue_purged`), placed by the proven order (section 9a).

**Older Trailbeams.** A Trailbeam built before 3.3 rejects the span as an unknown `trailbeam.kind` and says
so in `partial_success`; the other spans of the export are stored as usual, and its messages show as before.
A 3.3 Trailbeam reads spans from older packages, which never purge.

**Not observed.** A purge made by a client without Trailbeam (the AWS console, the CLI, another SDK) is not
seen; its messages still show as stuck.

## 19. Changes in revision 3.3

| 3.2 | 3.3 |
|---|---|
| `PurgeQueue` was not hooked: purged messages showed as stuck | `trailbeam purge`, kind `sqs.purge`, one per call, with `trailbeam.result` and the call's interval and ticks (section 18) |
| | Trailbeam shows messages a purge removed as "Purged", and messages in its window as "possibly purged" |
| | the end of the retention period is named as unobservable (section 9) |

`trailbeam.schema_version` stays `3`: the span is optional, Trailbeam requires it for nothing, and spans of
3.2 and before are read as they were.

## 20. The Java package (QV-319)

The jar `trailbeam-aws-sdk-v2` ([`java/`](../java/README.md)) emits the spans of this contract, revision 3.3,
for the AWS SDK for Java v2 (2.17 or later, Java 11 or later). It registers a global `ExecutionInterceptor`
through the SDK's classpath file `software/amazon/awssdk/global/handlers/execution.interceptors`, so the
application changes no code, and it ships a Lambda handler wrapper. Like the other packages it has its own
always-on tracer provider (a relocated copy of the OpenTelemetry SDK) and exports OTLP over HTTP. Everything
in sections 1 to 19 applies to it as written, except where this section says how Java does it. It adds no
attribute and changes no rule, so `trailbeam.schema_version` stays `3`. Its evidence is the QV-312 spike
(report document `report` on QV-312, section 3) and the conformance kit, which CI runs alone, beside the
OpenTelemetry Java agent (also at 0% sampling), beside dd-java-agent and in Spring Boot executable jars.

### 20.1 Parent

The SDK runs global interceptors in `HashMap` order, not classpath order, so the jar can't rely on running
after an agent's interceptor and reading the active span. It reads the parent from the outgoing HTTP request in
`beforeTransmission`, after every interceptor has changed it: the `X-Amzn-Trace-Id` header (`Root=...;Parent=...`)
if it has one, else `traceparent`. The OpenTelemetry Java agent and dd-java-agent both put one there, also when
their sampler dropped the span, so the Trailbeam span shares the stock span's trace ID either way. With no
header, the span is a root span.

**A `ReceiveMessage` in a polling loop has no stock span.** Neither agent creates a span for a receive unless a
span is already active, and a consumer's polling loop usually has none. Its `trailbeam receive` spans are then
root spans. Trailbeam reads only Trailbeam spans, so its timeline doesn't change; only a trace viewer shows
the receive in a trace of its own. The conformance kit checks exactly this (its variant option
`receive_roots`): beside either agent, every send and settle span has a parent, and receives may be roots.

### 20.2 Time, call order and identity

- Times are `Instant.now()` in nanoseconds since the epoch, the wall clock (section 2). In `beforeExecution`,
  `trailbeam.order.issued` is ticked, then the start is read, before the SDK marshals, signs and sends the request and
  before any retry. The end is read and `trailbeam.order.returned` ticked in `afterExecution` or
  `onExecutionFailure`.
- The call-order counter (section 2a) is one per class loader that loads the jar: one per process for a normal
  application, one per webapp in an application server. It is an `AtomicLong`, so it is strictly increasing
  and safe from every thread, which is what section 2a's argument needs.
- `thread.name` is read in `beforeExecution`, on the thread that made the call. An async client completes its
  response on an SDK thread, which says nothing about the consumer.
- `service.instance.id` and `messaging.client.id` follow section 5; the random ID is chosen once per class
  loader, when the jar's tracer provider is created.
- **SnapStart.** One snapshot becomes many execution environments, which would share one counter scope and one
  instance ID. After a restore the jar chooses a new `trailbeam.order.scope` (its counter starts again at 1)
  and a new `service.instance.id`, through the `org.crac` hooks SnapStart runs, and it flushes before the
  snapshot. Without `org.crac` on the classpath, the application calls `Trailbeam.afterRestore()` itself.

### 20.3 Lambda wrapper

The wrapper is a handler class (`app.trailbeam.sqs.TrailbeamSqsLambdaWrapper::handleRequest`) that loads the
real handler named in `TRAILBEAM_LAMBDA_HANDLER`, or a base class the handler extends (`TrailbeamSqsHandler`).
It follows sections 2, 2a, 4.4 and 15.5 like the other wrappers. `trailbeam.error_type` is the fully qualified
class name (`java.lang.RuntimeException`), because that is what Lambda's Java runtime reports as `errorType`,
and the kit compares the two. Its parent is the invocation's X-Ray trace header (the system property
`com.amazonaws.xray.traceHeader`, else `_X_AMZN_TRACE_ID`), or none.

### 20.4 Configuration and failure

The jar reads the environment variables of sections 2, 7 and 15.1; there are no options in code. It never
throws into an SQS call or an invocation. Where Python and Node.js refuse a bad setting at install time (an
invalid redact key, an unknown pattern, an out-of-range `TRAILBEAM_CAPTURE_MAX_BYTES`), the jar logs one
warning and exports no body and no error text: the spans of a listed queue carry
`trailbeam.body.capture_status = redaction_failed` until the setting is fixed (section 15.4).

To read the receive count and the other attributes Trailbeam needs, the interceptor adds their names to each
`ReceiveMessage` request: in `messageSystemAttributeNames` on SDK 2.25 and later, in `attributeNames` before
that, plus the tenant message attribute (if configured) and the SQS Extended Client's size attributes. It
keeps every name the application asked for and leaves `All` (and `.*`) alone. This changes what SQS returns,
not the message, and the jar makes no SQS call of its own.

### 20.5 Not covered

AWS SDK for Java v1 and Java 8 are not supported (QV-312 report, section 9). A client built without the global
interceptor file on its classpath (an uber jar that dropped it, see the Java README) emits nothing until the
interceptor is registered by hand.
