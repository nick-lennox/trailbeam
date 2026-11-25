# Trailbeam span contract, schema version 4 (revision 4.0): broker-neutral kinds and RabbitMQ

This file is part of the span contract. [`span-contract.md`](span-contract.md) defines the SQS spans (and,
from 3.4, the SNS publish) and the rules every Trailbeam span follows. This file adds what schema version 4
brings: a naming rule for kinds that works for more than one broker, and the spans of a RabbitMQ client
speaking AMQP 0-9-1. Where this file says nothing, `span-contract.md` applies. Where the two differ for a
RabbitMQ span, this file wins.

The design behind it, and the proof on RabbitMQ 4.4.1 it rests on, is the QV-303 design spike
(`rabbitmq-design` on QV-303). The keywords **must**, **must not**, **should** and **may** are used as in
RFC 2119. **[verified]** marks a claim checked in that proof or in the documents linked; **[estimate]**
marks a judgment.

Scope of 4.0:

- RabbitMQ **quorum queues** are the target. Classic queues work, with no receive count on redeliveries
  (section A7). **Streams** (`x-queue-type: stream`) are not covered: a stream is a log with offsets, not
  a queue with acks, and the packages **must not** emit `amqp.*` spans for stream consumers.
- AMQP 0-9-1 only. AMQP 1.0, MQTT and STOMP clients of RabbitMQ are not covered.
- This revision is the contract. The server's ingest and store (QV-306), its timeline and pages (QV-307,
  QV-308) and the packages (QV-309 Python `pika`, QV-310 Node.js `amqplib`) follow it.

## A1. Schema version 4 and compatibility

| Span | `trailbeam.schema_version` a package sends | A 4.0 Trailbeam |
|---|---|---|
| `amqp.*` (this file) | **must** be `4` | accepts only `4` |
| `sqs.*`, `sns.*` | `3`, as before; `4` is allowed and means exactly what `3` means | accepts `2`, `3` and `4` |

The SQS and SNS packages do not change for 4.0: their spans, kinds and attributes stay as
`span-contract.md` defines them. The version exists so that a Trailbeam built before 4.0 rejects a
RabbitMQ span with a clear reason (`trailbeam.schema_version 4 is not one of 2, 3`) instead of
misreading it: RabbitMQ spans have required fields that differ from SQS's (no receipt-handle hash, an
optional receive count), and the old ingest would apply SQS's rules to them. Nothing else in an export is
affected by a rejected span.

## A2. Kinds: `<family>.<operation>`

`trailbeam.kind` is `<family>.<operation>`. The family names the protocol and its operation names; the
broker is in `messaging.system.name`. One family can serve more than one broker later (another AMQP 0-9-1
broker would use `amqp.*` with its own system name) [estimate: no second AMQP broker is planned].

| Family | `messaging.system.name` | Kinds | Defined in |
|---|---|---|---|
| `sqs` | `aws.sqs` | `sqs.send`, `sqs.receive`, `sqs.settle`, `sqs.outcome`, `sqs.purge` | `span-contract.md` |
| `sns` | `aws.sns` | `sns.publish` | `span-contract.md` section 20 (3.4) |
| `amqp` | `rabbitmq` | `amqp.publish`, `amqp.return`, `amqp.deliver`, `amqp.ack`, `amqp.nack`, `amqp.reject`, `amqp.cancel`, `amqp.channel_closed` | this file |

Each kind maps to one **step** of a message's life. The step is what Trailbeam's store and timeline work
with; the kind says which broker operation reported it. This table is the contract between the kinds
and the server (QV-306):

| Step | SQS / SNS | RabbitMQ |
|---|---|---|
| sent | `sqs.send`, `sns.publish` | `amqp.publish` |
| not routed | (none) | `amqp.return` |
| received | `sqs.receive` | `amqp.deliver` |
| completed | `sqs.settle` `DeleteMessage*` | `amqp.ack` |
| released to the queue | `sqs.settle` `ChangeMessageVisibility*` with timeout `0` | `amqp.nack` / `amqp.reject` with `requeue = true`, `amqp.channel_closed` |
| extended | `sqs.settle` `ChangeMessageVisibility*` with timeout above `0` | (none: AMQP 0-9-1 cannot extend a delivery) |
| refused by the consumer | (none) | `amqp.nack` / `amqp.reject` with `requeue = false` |
| consumer stopped | (none) | `amqp.cancel` |
| outcome | `sqs.outcome` | (none: RabbitMQ has no Lambda wrapper) |
| queue purged | `sqs.purge` | (none in 4.0) |

A message refused with `requeue = false` is dead-lettered if its queue has a dead-letter exchange and
dropped otherwise. The consumer can't tell which; the next delivery, on the dead-letter queue, says
(section A8).

## A3. Attributes on every RabbitMQ span

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `trailbeam.schema_version` | int | must | `4` |
| `trailbeam.kind` | string | must | one of the `amqp.*` kinds (A2) |
| `trailbeam.source` | string | must | `client` |
| `messaging.system.name` | string | must | `rabbitmq` |
| `messaging.operation.type` | string | must | from the table in A4 |
| `messaging.operation.name` | string | must | the AMQP method, from the table in A4 |
| `messaging.destination.name` | string | must | the **queue** for a span about a delivery; the **exchange** for `amqp.publish` and `amqp.return` (A5) |
| `trailbeam.amqp.vhost` | string | must | the virtual host of the connection, `/` by default |
| `server.address` | string | should | the broker host the connection was opened to, as configured (no reverse DNS) |
| `server.port` | int | should | the broker port |
| `messaging.client.id` | string | must | `span-contract.md` section 5, client hooks |
| `process.pid` | int | must | OS process ID |
| `thread.name` | string | should (Python) | as in `span-contract.md` section 3 |
| `trailbeam.order.scope` | string | must | `span-contract.md` section 2a |
| `trailbeam.order.issued` | int | must, except where A4 says otherwise | section 2a |
| `trailbeam.order.returned` | int | must | section 2a |
| `trailbeam.start_observed` | bool | must be `false` where A4 says so | the span's start is not the start of the operation. Omitted otherwise. |
| `trailbeam.amqp.channel.id` | string | must on every span but `amqp.publish` and `amqp.return` | a random UUID v4 the package chooses when the channel is opened (A6) |
| `trailbeam.amqp.channel.number` | int | may | the channel number on its connection, for people reading traces; numbers are reused after a channel closes, so Trailbeam joins on `trailbeam.amqp.channel.id` |

Absent values are omitted, never sent as `""`, `0` or `-1` (`span-contract.md` section 3). `aws.sqs.queue.url`
and the receipt-handle attributes are not set on RabbitMQ spans.

Trailbeam keeps the queues of different virtual hosts apart: a RabbitMQ queue is identified by
(`trailbeam.amqp.vhost`, `messaging.destination.name`). Brokers are not told apart in 4.0: two brokers with a
queue of the same name in the same virtual host name are merged, as two SQS queues of one name in different
accounts are (`span-contract.md` section 6).

## A4. The spans

| Span name | `trailbeam.kind` | `messaging.operation.type` | `messaging.operation.name` | When |
|---|---|---|---|---|
| `trailbeam publish` | `amqp.publish` | `send` | `basic.publish` | once per `basic.publish` the application makes, after its confirm or its failure (A4.1) |
| `trailbeam return` | `amqp.return` | `send` | `basic.return` | once per `basic.return` the broker sends the publisher |
| `trailbeam deliver` | `amqp.deliver` | `receive` | `basic.deliver` (push) or `basic.get` (pull) | once per delivery the client gets |
| `trailbeam ack` | `amqp.ack` | `settle` | `basic.ack` | once per delivery an ack covers |
| `trailbeam nack` | `amqp.nack` | `settle` | `basic.nack` | once per delivery a nack covers |
| `trailbeam reject` | `amqp.reject` | `settle` | `basic.reject` | once per reject |
| `trailbeam cancel` | `amqp.cancel` | `cancel` | `basic.cancel` | once per consumer cancelled, by the client or the broker |
| `trailbeam channel_closed` | `amqp.channel_closed` | `settle` | `channel.close`, `connection.close` or `connection.lost` | once per delivery still unsettled when its channel closed |

`cancel` is a custom `messaging.operation.type`, as `purge` is: the OpenTelemetry messaging conventions
define none for stopping a consumer. A push delivery is `receive` here, not OpenTelemetry's `process`,
because Trailbeam's receive step is the moment the message reached the consumer, as for the Lambda
receive in `span-contract.md`. Span names are for people; Trailbeam reads the kind (`span-contract.md`
section 1). The SNS publish (3.4) is also named `trailbeam publish`; its kind is `sns.publish`.

**Time.** A span of a call the application makes (`basic.publish`, `basic.get`, an ack, nack, reject or
client cancel) is the interval of that call, with both order ticks, as in `span-contract.md` sections 2
and 2a. A span of something the broker sent (`basic.deliver`, `basic.return`, a broker `basic.cancel`) or
of a channel closing is **zero-length** at the moment the package's handler got it, before the
application's callback runs, carries `trailbeam.start_observed = false` and has a `trailbeam.order.returned`
tick and no `trailbeam.order.issued`, like the Lambda receive. With push delivery and a prefetch above 1,
the client library buffers deliveries, so the callback time is only an upper bound on when the broker
sent the message [verified: the proof, QV-303].

### A4.1 `trailbeam publish`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.destination.name` | string | must | the exchange; `amq.default` for the default exchange (`""`) |
| `rabbitmq.destination.routing_key` | string | must when not empty | the routing key |
| `messaging.message.id` | string | must when set | the `message_id` property (A6) |
| `trailbeam.amqp.message_id.source` | string | must with `messaging.message.id` | `application`, or `package` when a package option set it (A6) |
| `trailbeam.amqp.mandatory` | bool | must | the `mandatory` flag |
| `trailbeam.amqp.confirmed` | bool | must | `true` when the channel is in confirm mode (`confirm.select`) |
| `trailbeam.result` | string | must | below |
| `trailbeam.error_code` | string | must if failed | below |
| `trailbeam.amqp.delivery_mode` | int | may | `1` (transient) or `2` (persistent) |

- **With confirms** (`trailbeam.amqp.confirmed = true`): the span ends when the broker's `basic.ack` or
  `basic.nack` for this publish arrives, or when the channel or connection closes first. The interval then
  contains the moment the broker accepted the message: RabbitMQ acks "when a message has been accepted by
  all the queues" it routed to, after a quorum of replicas confirmed it on quorum queues, and acks an
  unroutable message once the exchange finds no queue [verified:
  [publisher confirms](https://www.rabbitmq.com/docs/confirms)]. `trailbeam.result` is `ok` for an ack,
  `failed` for a nack (`trailbeam.error_code = nack`), or for a close before the confirm (the AMQP reply-code
  name the broker gave, such as `NOT_FOUND` for a missing exchange, else `channel_closed`).
- **Without confirms:** the span ends when the client library returns from writing the frame.
  `trailbeam.result = ok` means only that the frame was written; nothing says the broker got the message,
  and Trailbeam shows the send as "not confirmed". `failed` means the library raised, with the error's
  class name as `trailbeam.error_code`.
- Every `basic.publish` gets a span, including failed ones, unlike a failed SQS batch entry: a RabbitMQ
  publish has its message ID before it is sent.
- A mandatory publish that no queue takes gets a `trailbeam return` (A4.2) **and** a `trailbeam publish`
  with `result = ok`: the broker sends the return, then acks the publish [verified: confirms page above].
  A package whose client library raises on a return (pika's `UnroutableError`) **must** still emit both.
- An unroutable publish **without** `mandatory` is dropped by the broker with an ack and nothing else. No
  span can see the drop.

### A4.2 `trailbeam return`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.destination.name` | string | must | the exchange from the return; `amq.default` for `""` |
| `rabbitmq.destination.routing_key` | string | must when not empty | the routing key from the return |
| `messaging.message.id` | string | must when set | the `message_id` property of the returned message |
| `trailbeam.amqp.message_id.source` | string | must with `messaging.message.id` | as on the publish |
| `trailbeam.amqp.reply_code` | int | must | the return's reply code, `312` (`NO_ROUTE`) for an unroutable message |
| `trailbeam.amqp.reply_text` | string | should | the reply text, such as `NO_ROUTE` |

Zero-length, `start_observed = false` (A4). A return carries no delivery tag or publish sequence number,
so Trailbeam joins it to its publish by message ID only. A return of a message without `message_id` is
counted on its exchange and joins nothing.

### A4.3 `trailbeam deliver`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.destination.name` | string | must | the queue: the one named in `basic.consume` for the consumer tag, or in `basic.get` |
| `messaging.message.id` | string | must when set | the `message_id` property |
| `rabbitmq.message.delivery_tag` | int | must | the delivery tag |
| `trailbeam.amqp.consumer_tag` | string | must for `basic.deliver` | the consumer tag |
| `trailbeam.amqp.exchange` | string | must | the exchange the message was published to, from the delivery; `amq.default` for `""` |
| `rabbitmq.destination.routing_key` | string | must when not empty | the routing key from the delivery |
| `trailbeam.amqp.redelivered` | bool | must | the delivery's `redelivered` flag |
| `trailbeam.receive_count` | int | must when known | section A7 |
| `trailbeam.receive_count.source` | string | with `trailbeam.receive_count` | `x-acquired-count`, `x-delivery-count` or `first_delivery` (A7) |
| `trailbeam.amqp.acquired_count` | int | must when the header is there | the `x-acquired-count` header, verbatim |
| `trailbeam.amqp.delivery_count` | int | must when the header is there | the `x-delivery-count` header, verbatim |
| `trailbeam.dead_letter.*` | | must when `x-death` is there | section A8 |
| `trailbeam.amqp.timestamp_ms` | int | may | the `timestamp` property (POSIX seconds) × 1000, when the publisher set it |

- Push (`basic.deliver`): zero-length, `start_observed = false`, no `issued` tick (A4). Emitted **before** the
  application's callback runs, so a crash in the callback cannot lose it.
- Pull (`basic.get`): the interval of the call, both ticks. A `basic.get-empty` produces no span.
- The packages make no broker calls of their own: no `basic.get` to look at a message, no
  `basic.recover`, no `queue.declare` to learn a queue's type. They read the calls the application makes and
  what the broker sends back (as `span-contract.md` section 15 says for SQS).

### A4.4 `trailbeam ack`, `trailbeam nack`, `trailbeam reject`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.destination.name` | string | must | the queue of the delivery this span settles |
| `messaging.message.id` | string | must when the delivery had one | from the delivery (A6) |
| `rabbitmq.message.delivery_tag` | int | must | the delivery tag of the delivery this span settles |
| `trailbeam.amqp.consumer_tag` | string | must when the delivery had one | from the delivery |
| `trailbeam.amqp.multiple` | bool | must (ack, nack) | the `multiple` flag of the call |
| `trailbeam.amqp.requeue` | bool | must (nack, reject) | the `requeue` flag |
| `trailbeam.result` | string | must | `ok` if the client library wrote the frame, `failed` if it raised |
| `trailbeam.error_code` | string | must if failed | the error's class name, or the AMQP reply-code name if the channel was closed |

- **One span per delivery.** With `multiple = true` the call settles every delivery on the channel up to the
  tag. The package keeps a record of the deliveries it emitted on each channel and not yet settled, and emits
  one span for each of them that the call covers, each with that delivery's tag, queue and message ID, all
  with the call's interval and ticks. A tag `0` with `multiple = true` covers every delivery on the channel.
- A call naming a tag the package never recorded (a delivery from before the package was installed) produces
  no span: its queue and message are not known.
- **No reply.** AMQP 0-9-1 sends nothing back for `basic.ack`, `basic.nack` or `basic.reject`, so
  `trailbeam.result = ok` means "written to the socket", not "the broker removed the message". A settle the
  broker refuses closes the channel with `PRECONDITION_FAILED - unknown delivery tag N` [verified: confirms
  page above], which arrives later as a `trailbeam channel_closed` for the deliveries still open on the
  channel (A4.6).

### A4.5 `trailbeam cancel`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.destination.name` | string | must | the queue the consumer consumed from |
| `trailbeam.amqp.consumer_tag` | string | must | the consumer tag |
| `trailbeam.amqp.cancelled_by` | string | must | `client` (the application called `basic.cancel`) or `broker` (the broker sent `basic.cancel`) |
| `trailbeam.amqp.unsettled_count` | int | must | how many deliveries to this consumer the package had recorded and not settled at that moment; `0` is a real value |
| `trailbeam.result` | string | must (client) | `ok` when `basic.cancel-ok` arrived, `failed` otherwise |

A client cancel is the interval of the call, up to `cancel-ok`. A broker cancel is zero-length with
`start_observed = false` (A4). Cancelling a consumer "will neither discard nor requeue" its unsettled
deliveries [verified: [consumers](https://www.rabbitmq.com/docs/consumers)]; they can still be acked.
The broker cancels a consumer when its queue is deleted, and, on quorum queues since RabbitMQ 4.3, when a
delivery acknowledgement timeout (`consumer_timeout`) fires and the client supports cancel notifications;
in that case the consumer's unsettled deliveries are returned to the queue [verified:
[quorum queues](https://www.rabbitmq.com/docs/quorum-queues)]. The cancel frame does not say which of the
two it was.

### A4.6 `trailbeam channel_closed`

| Attribute | Type | Req. | Value |
|---|---|---|---|
| `messaging.destination.name` | string | must | the queue of the delivery |
| `messaging.message.id` | string | must when the delivery had one | from the delivery |
| `rabbitmq.message.delivery_tag` | int | must | the delivery's tag |
| `trailbeam.amqp.consumer_tag` | string | must when the delivery had one | from the delivery |
| `trailbeam.amqp.close.initiator` | string | must | `client` (the application closed the channel or connection), `broker` (the broker sent `channel.close` or `connection.close`) or `network` (the connection was lost without a close, a socket error or a missed heartbeat) |
| `trailbeam.amqp.close.reply_code` | int | must when a close method carried one | such as `406` |
| `trailbeam.amqp.close.reply_text` | string | must when a close method carried one | such as `PRECONDITION_FAILED - unknown delivery tag 7` |

`messaging.operation.name` is `channel.close` when only the channel closed, `connection.close` when its
connection was closed with a close method, and `connection.lost` for `initiator = network`. One span per
delivery the package recorded on that channel and had not settled, zero-length at the moment the package
learned of the close, with `start_observed = false` (A4). RabbitMQ requeues every unacked delivery of a
channel that closes, and sets `redelivered` on the next delivery [verified: confirms page above].

The span's time is when the **client** saw the close, not when the broker requeued: after a lost connection
the broker may notice only at its own heartbeat timeout, later. Trailbeam therefore **must not** order the
return to the queue by this span's time; only the next delivery's receive count (A9) orders it.

A broker close with `406` and a text starting `PRECONDITION_FAILED - delivery acknowledgement on channel`
is the consumer timeout of a client without cancel notifications [verified in the source: the error
`"delivery acknowledgement on channel ~w timed out. ..."` in
[`deps/rabbit/src/rabbit_channel.erl`](https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbit/src/rabbit_channel.erl),
read 2026-11-24; not run in the proof]. A close with
`PRECONDITION_FAILED - unknown delivery tag` follows a settle the broker refused (A4.4).

## A5. Destinations

OpenTelemetry's RabbitMQ conventions set `messaging.destination.name` to a combined `{exchange}:{routing
key}` on a send and `{exchange}:{routing key}:{queue}` on receive and settle
([RabbitMQ semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/rabbitmq/), checked
2026-11-24). Trailbeam does **not** use that form: it groups messages by `messaging.destination.name`
(`span-contract.md` section 6), and a combined name would split one queue into a group per routing key. So
the name is the queue on spans about a delivery and the exchange on publish and return, and the parts are in
their own attributes. The routing key and delivery tag use OpenTelemetry's names in the current `rabbitmq.`
namespace (the `messaging.rabbitmq.` names are deprecated,
[registry](https://opentelemetry.io/docs/specs/semconv/registry/attributes/messaging/)). The default
exchange `""` is written `amq.default`, the name those conventions use for it.

A publisher does not know which queues an exchange routes a message to. Trailbeam learns it only from the
deliveries: "routed to N queues" counts the queues where the message was delivered, which is a lower bound
(a queue no instrumented consumer reads is not seen).

## A6. Message identity

`messaging.message.id` is the AMQP `message_id` property, when it is set and not empty. Unlike SQS's
`MessageId`, it is chosen by the application: the broker neither sets it nor checks that it is unique.

- **Keys.** Trailbeam identifies a message by (`messaging.system.name`, `messaging.message.id`), so an SQS
  message and a RabbitMQ message with the same ID text are never merged. One copy of a message in one queue
  is (message, `trailbeam.amqp.vhost`, queue): a fanout publish to two queues is one message with two copies,
  **not** a duplicate [verified: the proof shows the server before 4.0 calling it one].
- **The same ID published twice** (a publisher retry) is one message with two `trailbeam publish` spans.
  Trailbeam shows it as published twice; how the timeline and the Duplicates page show it is the server's
  (QV-307, design choice P5).
- **No `message_id`.** The span is still emitted, without `messaging.message.id`. Trailbeam counts it on its
  queue or exchange and gives it no timeline (design choice P2, option b). The packages **must not** invent an
  ID by default: the packages don't change messages, and a hash of the body would merge identical bodies.
- **An ID set by a package** (P2, option a) is an opt-in package option, off by default, defined with the
  packages (QV-309). A package that sets `message_id` on a publish **must** mark that publish
  `trailbeam.amqp.message_id.source = package`; the deliveries can't tell, so it is shown on the publish only.
- **Settle and close spans** carry the delivery's message ID. A delivery tag is valid only on its own channel
  [verified: confirms page above], so the process that settles a delivery is always the one that received
  it, and the package always has the delivery's ID. There is no map to miss, unlike SQS (`span-contract.md`
  section 8).

## A7. Receive count

On SQS the receive count is required. On RabbitMQ it is set when the broker gives one, and omitted when it
does not. The package works it out from the delivery, in this order:

1. The `x-acquired-count` header is there: `trailbeam.receive_count` = its value + 1, source
   `x-acquired-count`. Quorum queues set it on a redelivery and count every time the message was assigned to
   a consumer: after `basic.nack`, `basic.reject`, a lost connection, a consumer timeout (since 4.3)
   [verified: [quorum queues](https://www.rabbitmq.com/docs/quorum-queues), and the proof on 4.4.1, where
   the header was absent on the first delivery].
2. Otherwise the `x-delivery-count` header is there: its value + 1, source `x-delivery-count`. This is a
   quorum queue before RabbitMQ 4.3, where every return counted toward it [estimate: from the docs' note
   on 4.3; not run on a broker before 4.3].
3. Otherwise `redelivered` is `false`: `1`, source `first_delivery`. The message was not delivered before,
   on any queue type.
4. Otherwise (a classic queue's redelivery) the count is omitted. A classic queue says only that the message
   may have been delivered before; the second delivery can't be told from the tenth.

A header counts only when its value is an integer (an AMQP integer type); any other value is treated as absent.

**A publisher can set these headers too.** RabbitMQ passes publisher-set `x-acquired-count` and
`x-delivery-count` through unchanged on the first delivery, on classic and quorum queues [verified: 4.4.1, a
published `x-acquired-count: 41` arrived with `redelivered = false`]. The broker sets them itself only on a
redelivery, or carries them over to a dead-letter queue, which adds `x-death`. So on a delivery with
`redelivered = false` and no usable `x-death` (A8: a first entry with `queue` and `reason`) the package **must**
ignore both headers: the count is `1`, source `first_delivery`, and `trailbeam.amqp.acquired_count` and
`trailbeam.amqp.delivery_count` are omitted. Otherwise a forged count would show deliveries not observed and a
return to the queue after an acked delivery, and a forged `x-delivery-count` would reach `x-delivery-limit`.
A publisher that also forges `x-death` is not caught here; it then reads as a dead-letter queue delivery, and
A8 limits what that forged move can do.

`x-acquired-count` and `x-delivery-count` are copied verbatim to their own attributes, because they mean
different things on 4.3 and later: `x-delivery-count` counts only failed deliveries (reject, lost connection),
and it is what `x-delivery-limit` is checked against. A `basic.nack` with `requeue = true` raises only
`x-acquired-count`, so a nack-requeue loop never reaches the delivery limit [verified: the proof, 6
deliveries with `x-delivery-limit 2`].

**On a dead-letter queue** both headers carry over from the source queue, so the first delivery there has a
count above 1 [verified: the proof read `x-acquired-count 3` there]. Trailbeam compares receive counts only
between deliveries on one queue (A9), so this orders nothing wrongly; it **must not** read the jump as
deliveries it didn't observe on the dead-letter queue.

## A8. Dead-letter attributes

When a delivery carries the `x-death` header, the package copies its **first** entry, which is the most
recent dead-lettering [verified: [dead letter exchanges](https://www.rabbitmq.com/docs/dlx)]:

| Attribute | Type | Req. | From `x-death[0]` |
|---|---|---|---|
| `trailbeam.dead_letter.source_queue` | string | must | `queue`: the queue the message was dead-lettered from |
| `trailbeam.dead_letter.reason` | string | must | `reason`: `rejected`, `expired`, `maxlen` or `delivery_limit`, verbatim |
| `trailbeam.dead_letter.count` | int | must | `count`: how many times it was dead-lettered from that queue for that reason |
| `trailbeam.dead_letter.time_ms` | int | must when present | `time` (an AMQP timestamp, POSIX seconds) × 1000. It is the **first** dead-lettering from that queue for that reason, not necessarily this one, and has one-second resolution. |
| `trailbeam.dead_letter.exchange` | string | should | `exchange`: where the message was published before it was first dead-lettered; `amq.default` for `""` |
| `trailbeam.dead_letter.first_queue` | string | may | the `x-first-death-queue` header |
| `trailbeam.dead_letter.first_reason` | string | may | the `x-first-death-reason` header |

An `x-death` that is not an array of tables, or whose first entry has no `queue` or `reason`, is not copied
(no `trailbeam.dead_letter.*` at all). The SQS attribute `trailbeam.dead_letter_queue_source_arn` is not used
for RabbitMQ.

**The move.** A delivery whose `trailbeam.dead_letter.source_queue` names another queue than its own reports
an **observed** move, as `DeadLetterQueueSourceArn` does on SQS (`span-contract.md` section 9b), with source
`amqp_x_death` and the reason. It is placed after the last step on the source queue and before the reporting
delivery, like the SQS move. The same rules hold: no `x-death`, no move; Trailbeam **must not** infer a move
from the message ID alone. A message dead-lettered because it expired (`expired`) or the queue was full
(`maxlen`) may never have been delivered, so its first delivery is on the dead-letter queue.

**`x-death` and `x-first-death-*` are publisher-controlled.** RabbitMQ 4.4 passes `x-death` and
`x-first-death-*` headers that a publisher set on its own message through to the consumer unchanged, on classic
and quorum queues [verified: RabbitMQ 4.4.1, the QV-526 review: a message published to one queue with `x-death`
naming `payments` arrived as published, on both queue types; not checked: brokers before 4.3, and whether a
policy or plugin strips them]. The broker adds its own entry only when it
dead-letters the message, and nothing on the delivery tells the two apart. So anyone who can publish to a queue an
instrumented consumer reads, and knows a message ID, can make a delivery report a move off any queue of that
virtual host. (On SQS, `DeadLetterQueueSourceArn` is set by AWS and a sender cannot set it.) The packages copy the
header as it is; Trailbeam **must** check the move against what it observed on the source queue:

- A message can be dead-lettered only while it is **on** its queue, or when a consumer refuses it. A delivery
  still **held** by a consumer (delivered, with no ack, no refusal, no requeue and no channel close after it) is
  not in the queue: its TTL, the queue's length limit and `x-delivery-limit` don't apply to it. An **acked**
  delivery is gone.
- So Trailbeam takes a delivery's `x-death` as a move off queue Q only when the **latest delivery of the
  message's copy on Q** (A6) was refused (`requeue = false`), requeued (`requeue = true`), or its channel closed,
  or when no delivery on Q was observed. When that latest delivery is held or acked, there is **no move**: no
  `moved_to_dead_letter_queue` step, the copy on Q is not closed ("moved") and stays on Stuck if it was there,
  the reporting delivery's queue is not a dead-letter queue (`in_dead_letter_queue`), no `dlq_receive` or
  `dlq_seen` queue event is counted, and the timeline gets the notice `x_death_not_a_move` naming Q.

What this does not catch [estimate, not run]: a forged move off a queue whose latest delivery was refused (the
copy already left its queue; the forgery only says it was dead-lettered rather than dropped) or requeued or
closed (the copy would be closed while RabbitMQ still holds it), or off a queue with no observed delivery (a step
on the timeline, but no copy to close). It also rules out one real move: a consumer that dies without its hook
recording the channel close (a killed process) leaves its delivery looking held, so if RabbitMQ then returns the
message and dead-letters it (`delivery_limit`, `expired`), the copy stays open, with the notice saying so. That
error keeps a message on Stuck rather than hiding one. Checking that the reporting delivery came through the
source queue's real dead-letter exchange would close more of this, but Trailbeam is not told the queues'
`x-dead-letter-exchange` settings.

## A9. Order of steps

The sources of `span-contract.md` section 9a apply, with these differences:

| Source | On RabbitMQ |
|---|---|
| `clock` | as on SQS, between spans whose start was observed; a zero-length span with `start_observed = false` is only an upper bound, so nothing is ordered **before** it by the clock |
| `process_order` | as on SQS (section 2a): one counter per process or realm, shared by SQS, SNS and RabbitMQ hooks |
| `receive_count` | between two deliveries of the message **on one queue** whose counts came from the same source (A7). A count from `first_delivery` (1) is below any other. |
| `sqs_timestamps` | does not exist. The broker sets no time on a message by default; the `timestamp` property is the publisher's clock and is not a source. |

A delivery tag is a broker-assigned number that grows on its channel [verified: confirms page above], but
4.0 does not use it as a source: Trailbeam joins on it and does not order by it.

## A10. What Trailbeam infers on RabbitMQ

There is no visibility timeout in AMQP 0-9-1, so Trailbeam **must not** say "visibility timeout expired" for a
RabbitMQ message. A redelivery is shown as an **inferred** "returned to the queue" step, between a delivery and
a later delivery of the message on the same queue with a higher receive count (or, with no counts, a later
delivery with `redelivered = true`). Its cause is the one the spans of the earlier delivery show:

| Spans of the earlier delivery | Cause shown |
|---|---|
| `amqp.nack` or `amqp.reject` with `requeue = true` | requeued by the consumer (observed) |
| `amqp.channel_closed` | its channel closed (observed), with the initiator and reply text |
| `amqp.cancel` by the broker for its consumer tag, after it, and no settle | the broker cancelled the consumer: a consumer timeout or a deleted queue |
| none of these | returned to the queue, cause not observed |

- **Deliveries not observed:** the receive count goes up by more than 1 between two deliveries on one queue,
  as on SQS. Not inferred from a count source change or on a dead-letter queue (A7).
- **Refused and not seen again:** `requeue = false` and no later delivery: "refused by the consumer;
  dead-lettered or dropped, not seen since". Trailbeam does not know whether the queue has a dead-letter exchange.
- **Requeue loop:** a message requeued by `basic.nack` several times on one queue. On RabbitMQ 4.3 and later
  these don't count toward `x-delivery-limit`, so the queue never dead-letters it. Trailbeam may warn about it;
  the threshold is the server's (QV-307).
- **Settled but the channel then closed** with `unknown delivery tag`: Trailbeam shows the close. It can't tell
  which settle the broker refused.

**Not seen at all:** a non-mandatory unroutable publish (A4.1), the expiry of a message with no dead-letter
exchange, a queue being deleted or purged, and a message removed by a client without a Trailbeam package. Such
messages keep their last observed step.

## A11. Message contents

`span-contract.md` section 15 applies to `trailbeam publish` and `trailbeam deliver`, with these changes:

- **The tenant** comes first from the header named by the tenant option (`TRAILBEAM_TENANT_ATTRIBUTE`), when its
  value is a string or a number, then from the body path. `trailbeam.tenant.source` is `header` for the first.
- **The queue lists** for bodies and error text match the delivery's queue, and on a publish the exchange name.
- **Body size** (`messaging.message.body.size`) is the body's length in bytes, as sent.
- **Text only.** A body is captured only if it has no `content_encoding` (or `identity`) and its bytes are valid
  UTF-8. Otherwise `trailbeam.body.capture_status` is `not_text` (new in 4.0) and no body is exported.
- **No MD5 check** (`md5_mismatch` is never set: the broker gives no checksum), **no offload** pointers, and no
  `trailbeam.body.envelope`.

## A12. What Trailbeam requires (4.0)

Trailbeam applies `span-contract.md` section 4.5 to every span, with the version rule of A1, and rejects a
RabbitMQ span, with the reason in `partial_success`, when any of these is true:

- `trailbeam.schema_version` is not `4` (reason: `amqp span with trailbeam.schema_version V; needs 4`);
- `messaging.system.name` is not `rabbitmq`
  (`amqp span with messaging.system.name 'X'; needs 'rabbitmq'`);
- `messaging.operation.name` does not match the kind (A4): the reason any unknown kind or operation pair gets,
  `unknown trailbeam.kind 'K' / messaging.operation.name 'N'`;
- a required attribute is missing or has the wrong type (`K span without A, B`). Booleans must be OTLP booleans;
  integers may be decimal strings, as in section 4.5;

  | Kind | Required attributes |
  |---|---|
  | `amqp.publish` | `messaging.destination.name`, `trailbeam.amqp.vhost`, `trailbeam.amqp.confirmed`, `trailbeam.amqp.mandatory`, `trailbeam.result` |
  | `amqp.return` | `messaging.destination.name`, `trailbeam.amqp.vhost`, `trailbeam.amqp.reply_code` |
  | `amqp.deliver` | `messaging.destination.name`, `trailbeam.amqp.vhost`, `trailbeam.amqp.channel.id`, `rabbitmq.message.delivery_tag`, `trailbeam.amqp.redelivered` |
  | `amqp.ack` | `messaging.destination.name`, `trailbeam.amqp.vhost`, `trailbeam.amqp.channel.id`, `rabbitmq.message.delivery_tag`, `trailbeam.amqp.multiple`, `trailbeam.result` |
  | `amqp.nack` | as `amqp.ack`, and `trailbeam.amqp.requeue` |
  | `amqp.reject` | `messaging.destination.name`, `trailbeam.amqp.vhost`, `trailbeam.amqp.channel.id`, `rabbitmq.message.delivery_tag`, `trailbeam.amqp.requeue`, `trailbeam.result` |
  | `amqp.cancel` | `messaging.destination.name`, `trailbeam.amqp.vhost`, `trailbeam.amqp.channel.id`, `trailbeam.amqp.consumer_tag`, `trailbeam.amqp.cancelled_by` |
  | `amqp.channel_closed` | `messaging.destination.name`, `trailbeam.amqp.vhost`, `trailbeam.amqp.channel.id`, `rabbitmq.message.delivery_tag`, `trailbeam.amqp.close.initiator` |

- a value is outside its list: `trailbeam.result` not `ok` or `failed`; `trailbeam.amqp.cancelled_by` not
  `client` or `broker`; `trailbeam.amqp.close.initiator` not `client`, `broker` or `network`;
  `trailbeam.receive_count.source` not one of the three in A7 (`K span with A 'X'; not one of ...`).

`messaging.message.id` is **not** required on any RabbitMQ span (A6), and neither is `trailbeam.receive_count`
(A7). A `trailbeam.receive_count` without `trailbeam.receive_count.source` is read as having no count. The
dead-letter and body attributes are never required.

## A13. Test vectors and fixtures

- [`span-contract-amqp-vectors.json`](span-contract-amqp-vectors.json): `emit` cases (what the broker sent or
  the application called, and the attributes a package **must** produce, as the `purge` vectors do), `receive_count`
  cases for A7, `dead_letter` cases for A8, and `ingest` cases (a span's attributes, and whether a 4.0 Trailbeam
  accepts it or the start of the reason it rejects it with).
- [`fixtures/amqp-4.0-spans.json`](fixtures/amqp-4.0-spans.json): the proof's scenarios as spans, written by
  [`fixtures/make-amqp-spans.py`](fixtures/make-amqp-spans.py) from this contract. They are **not recorded**:
  no package emits them yet. Re-record them from the Python package when QV-309 lands, and keep the scenarios.
- `python/tests/test_amqp_contract.py` checks the vectors against each other and against A7, A8 and A12, and
  that every fixture span is accepted by the A12 rules. The server (QV-306) and each package **must** pass the
  vectors.

## A14. Verified and not verified

Verified:

- The proof on RabbitMQ 4.4.1 with pika 1.5.0 (QV-303): the counter headers and their absence on a first
  delivery, the counts carried over to a dead-letter queue, the `x-death` reason `rejected`, the nack loop that
  never dead-letters, the fanout copies, the publisher retry.
- RabbitMQ's documentation, checked 2026-11-24: [quorum queues](https://www.rabbitmq.com/docs/quorum-queues)
  (the counters, the limit on `delivery-count` since 4.3, consumer timeouts since 4.3),
  [dead letter exchanges](https://www.rabbitmq.com/docs/dlx) (`x-death`, newest first, its fields and reasons),
  [publisher confirms and consumer acknowledgements](https://www.rabbitmq.com/docs/confirms) (confirm timing,
  return before ack, unknown tag error, per-channel tags, requeue on close),
  [consumers](https://www.rabbitmq.com/docs/consumers) (cancel neither discards nor requeues).
- OpenTelemetry: `rabbitmq` as a `messaging.system.name` value, the operation types, the `rabbitmq.` attribute
  names and the destination-name guidance ([registry](https://opentelemetry.io/docs/specs/semconv/registry/attributes/messaging/),
  [RabbitMQ conventions](https://opentelemetry.io/docs/specs/semconv/messaging/rabbitmq/)). All at `development`
  stability.

Not verified:

- RabbitMQ before 4.3 (A7 rule 2), classic queues beyond the one redelivery in the proof, and a broker cancel on
  consumer timeout (the proof ran with the default 30-minute timeout).
- A consumer-timeout channel close on a running broker (A4.6): its text was read in the source only.
- The `x-death` `time` field's encoding in each client library: the AMQP type is a timestamp in seconds; pika
  and amqplib decode it to their own date types, which the packages convert.
- That pika and amqplib give a package every hook it needs (the confirm, return and close callbacks): that is
  checked with the packages (QV-309, QV-310) [estimate: the proof's shim used each of them in pika].

## A15. Changes in schema version 4

| 3.x | 4.0 |
|---|---|
| kinds `sqs.*` (and 3.4 `sns.publish`), one broker | `<family>.<operation>` for any broker, with the step each kind maps to (A2); SQS and SNS kinds unchanged |
| `messaging.system.name` is `aws.sqs` (or `aws.sns`) | also `rabbitmq` |
| a message is its `MessageId` | a message is (system, message ID); a RabbitMQ copy is also keyed by its queue (A6) |
| receive count required on every receive | required on SQS; optional, with its source, on RabbitMQ (A7) |
| a settle joins its receive through the receipt-handle hash | RabbitMQ: through (`trailbeam.amqp.channel.id`, `rabbitmq.message.delivery_tag`), with no hash (A6) |
| a redelivery is a visibility timeout | on RabbitMQ, a return to the queue with its observed cause (A10) |
| dead-letter source from `DeadLetterQueueSourceArn` | on RabbitMQ, from `x-death`, with the reason (A8) |
| capture statuses of section 15.4 | also `not_text` (A11) |
| Trailbeam accepts schema versions 2 and 3 | 2, 3 and 4 (A1) |

Why no receipt-hash for RabbitMQ: a delivery tag is a small counter valid only on its own channel, so it gives
no power to anyone outside that channel, unlike an SQS receipt handle. The design spike suggested hashing it
anyway; this contract doesn't, because the hash would add a key to configure and protect nothing.
