# qv2 store API, version 0.1

The qv2 store (`server/`, command `qv2-store`) serves three things on one port (default 4318):

- `POST /v1/traces`: OTLP/HTTP trace ingest, for the qv2 packages or the [Collector](../collector/).
- `GET /api/...`: a read-only JSON API, described here.
- `GET /`, `/message?id=`, `/duplicates`, `/stuck`: the web screen, built from this API (see [`server/README.md`](../server/README.md#web-screen)).

It reads the spans defined in [`span-contract.md`](span-contract.md). Section numbers below ("contract §9")
refer to that file.

## Conventions

- Every response is `application/json`. Errors are `{"error": "<message>"}` with status `400` (bad
  parameter) or `404` (unknown endpoint or message).
- Times are ISO 8601 UTC with microseconds, such as `"2026-11-13T21:40:01.123456Z"`. Observed events
  also carry `time_unix_nano` as a **string**, because nanosecond timestamps do not fit in a JavaScript
  number.
- Message IDs go in the path URL-encoded (`encodeURIComponent`).
- List endpoints take `limit` (1 to 1000, default 100) and `queue` (only messages with at least one span
  on that queue, by `messaging.destination.name`).
- `lambda_timeout_s` (1 to 86400) is accepted by every endpoint that builds timelines. It overrides the
  server's `--lambda-timeout` (default 900 s, the Lambda maximum, because the store does not know the
  function's real timeout).
- The server's `--max-receive-count QUEUE=N,...` (env `QV2_MAX_RECEIVE_COUNT`) gives the
  `maxReceiveCount` of each queue's redrive policy. Spans don't carry it. Without it, the store never
  says a message is due on its dead-letter queue (`dead_letter_queue_move`, below).
- There is no authentication in 0.1. Bind to `127.0.0.1` (the default) unless the network is trusted.

## `GET /api/messages/{message_id}`: one message's timeline

`404` if no span has this message ID.

```json
{
  "message_id": "5fea7756-0ea4-451a-a703-a558b933e274",
  "state": "deleted",
  "source_queue": "orders",
  "current_queue": "orders",
  "in_dead_letter_queue": false,
  "dead_letter_queue_move": null,
  "send_observed": true,
  "deliveries": 2,
  "successful_deliveries": 1,
  "first_seen": "2025-11-14T00:26:40.030000Z",
  "last_seen": "2025-11-14T00:27:20.005000Z",
  "concurrent_groups": 0,
  "groups": [
    {"group": 0, "concurrent": false, "label": null, "observed_events": 1, "inferred_events": 0,
     "ordered_inside": 0, "start": "2025-11-14T00:26:40.000000Z", "end": "2025-11-14T00:26:40.030000Z"},
    {"group": 1, "...": "..."}, {"group": 2, "...": "..."}, {"group": 3, "...": "..."}
  ],
  "events": [
    {"id": "e1", "type": "sent", "inferred": false, "group": 0,
     "start": "2025-11-14T00:26:40.000000Z", "start_unix_nano": "1763080040000000000",
     "end": "2025-11-14T00:26:40.030000Z", "end_unix_nano": "1763080040030000000", "start_observed": true,
     "time": "2025-11-14T00:26:40.030000Z", "time_unix_nano": "1763080040030000000",
     "queue": "orders", "consumer": {"service_name": "orders-api", "client_id": "producer", "source": "client", "pid": 4242},
     "span": {"trace_id": "8f0c...", "span_id": "0000000000000001"}, "after": []},
    {"id": "e2", "type": "received", "inferred": false, "group": 1,
     "start": "2025-11-14T00:26:39.100000Z", "end": "2025-11-14T00:26:40.020000Z", "...": "...",
     "delivery": 1, "receive_count": 1,
     "sent_timestamp": "2025-11-14T00:26:40.004000Z", "first_receive_timestamp": "2025-11-14T00:26:40.006000Z",
     "after": [{"event": "e1", "type": "sent", "sources": ["sqs_timestamps"], "source": "sqs_timestamps",
                "why": "SQS's timestamps show this message was sent (2025-11-14T00:26:40.004000Z) before its first receive (2025-11-14T00:26:40.006000Z)."}]},
    {"id": "e3", "type": "visibility_timeout_expired", "inferred": true, "group": null, "time": null,
     "window": {"from": "2025-11-14T00:26:39.100000Z", "to": "2025-11-14T00:27:20.000000Z"},
     "reason": "The visibility timeout expired before the message was deleted, so SQS delivered it again.",
     "queue": "orders", "after_delivery": 1, "before_delivery": 2,
     "after": [{"event": "e2", "type": "received", "sources": ["inferred_from"], "source": "inferred_from",
                "why": "Inferred from receive 1: it came after it."}],
     "before": {"event": "e4", "type": "received", "sources": ["inferred_from"], "source": "inferred_from",
                "why": "Inferred from receive 2: it came before it."}},
    {"id": "e4", "type": "received", "inferred": false, "group": 2, "...": "...", "delivery": 2, "receive_count": 2,
     "after": [{"event": "e2", "type": "received", "sources": ["clock", "process_order", "receive_count"], "source": "clock",
                "why": "Receive 1 ended (2025-11-14T00:26:40.020000Z) before this started (2025-11-14T00:27:18.900000Z), on the hosts' clocks. Same process: receive 1 returned before this call was issued. SQS's receive count shows receive 1 (receive count 1) came before this one (receive count 2)."}]},
    {"id": "e5", "type": "deleted", "inferred": false, "group": 3, "...": "...", "delivery": 2, "result": "ok", "status": "final",
     "after": [{"event": "e4", "type": "received", "sources": ["process_order"], "source": "process_order",
                "why": "Same process: receive 2 returned before this call was issued."}]}
  ],
  "order": {
    "edges": [
      {"from": "e1", "to": "e2", "sources": ["sqs_timestamps"]},
      {"from": "e1", "to": "e4", "sources": ["clock"]},
      {"from": "e1", "to": "e5", "sources": ["clock"]},
      {"from": "e2", "to": "e4", "sources": ["clock", "process_order", "receive_count"]},
      {"from": "e2", "to": "e5", "sources": ["clock", "process_order"]},
      {"from": "e4", "to": "e5", "sources": ["process_order"]}
    ],
    "edges_by_source": {"clock": 4, "process_order": 3, "receive_count": 1, "sqs_timestamps": 1},
    "conflicts": []
  },
  "notices": []
}
```

Here the receive's long poll (from 39.100) was already waiting when the message was sent (40.000 to
40.030), and its response (40.020) came back before the send's: the clocks can't order the two. SQS's
own clock can: the receive carries `SentTimestamp` 40.004 and `ApproximateFirstReceiveTimestamp`
40.006, so the send came first (`sqs_timestamps`). The redelivery's receive count orders the two
receives, and the consumer's process recorded that it issued the delete after its receive returned
(`process_order`), even though the delete started in the same millisecond the receive ended. If SQS's
two timestamps had been equal (the same millisecond, common on a local emulator), the send and receive
1 would be one "concurrent, order unknown" group.

### Summary fields

| Field | Meaning |
|---|---|
| `state` | `deleted`: an observed final delete (below). `deleted_inferred`: the latest delivery was to Lambda and its outcome was `success`, so Lambda deleted it (not observed). `processing`: the latest delivery is a Lambda invocation still within `lambda_timeout_s`, with no outcome yet. `not_deleted`: anything else. |
| `source_queue` | Queue of the first `sent` event, or of the first receive if the send was not observed. |
| `current_queue` | Queue of the latest receive (or `source_queue`). |
| `in_dead_letter_queue` | `current_queue` is a queue SQS reported moving the message to: an observed `moved_to_dead_letter_queue` event (contract §9b) has it as `to_queue`. A receive on another queue without that report doesn't count. |
| `dead_letter_queue_move` | `"observed"`: SQS reported a move to a dead-letter queue (an event below). `"not_seen_on_dlq_yet"`: the highest receive count on `source_queue` has reached its configured `maxReceiveCount`, the message is not deleted, and no receive from a dead-letter queue has reported a move. No move is shown, and the notice `dlq_move_not_seen` says so. The store does not receive from the DLQ to check, because a receive changes the message's state. `null` otherwise, including when `maxReceiveCount` is not configured. |
| `deliveries` | Number of observed receives, on any queue. |
| `successful_deliveries` | Deliveries that ended in an accepted delete (counted once per delivery) or a Lambda `success`. More than one means the message was processed more than once. |
| `first_seen` | Send time (the end of the send call); if the send was not observed, the earliest SQS `SentTimestamp` from a receive. |
| `groups` | The timeline's groups, in order (below). Each has `group` (its number), `concurrent` (more than one event, observed or inferred), `label` (`"concurrent, order unknown"` or `null`), `observed_events`, `inferred_events`, `ordered_inside` (how many `after` entries link two members of the group: pairs inside the group whose order is known), `start` (earliest start; `null` if any member's start is unknown) and `end` (latest end; `null` for a group of inferred events only). |
| `concurrent_groups` | Number of groups with `concurrent: true`. |
| `order` | The proven order between observed events (below): `edges` (every directly proven pair, `from` before `to`, with the `sources` that prove it), `edges_by_source` (edges per source) and `conflicts` (edges dropped because the sources contradicted each other). |

### Events and their order

Each observed event is an interval (contract §2): `start` is just before the SQS request was sent,
`end` is when its response arrived, and SQS acted somewhere in between. Events are in display order,
by these rules (contract §9a):

- **Event A is shown before event B only if an observed fact proves it.** Each fact is a **source**:

  | Source | A is before B when | Wording on the screen |
  |---|---|---|
  | `clock` | A ended before B started (`A.end < B.start`), on the hosts' clocks | "Receive 1 ended (...) before this started (...), on the hosts' clocks." |
  | `process_order` | A and B were made by the same process (same `qv2.order.scope`), and A's call returned before B's was issued (contract §2a) | "Same process: receive 1 returned before this call was issued." |
  | `receive_count` | A and B are receives on the same queue, and A's `ApproximateReceiveCount` is lower | "SQS's receive count shows receive 1 (receive count 1) came before this one (receive count 2)." |
  | `sqs_timestamps` | A is the send and B the first receive (receive count 1) on its queue, and B's `SentTimestamp` is earlier than its `ApproximateFirstReceiveTimestamp`. Both come from SQS's clock, which is never compared with a host clock; equal values prove nothing | "SQS's timestamps show this message was sent (...) before its first receive (...)." |

- Order is transitive: A before B by one source and B before C by another puts A before C.
  `order.edges` lists every directly proven pair, so a client can follow the chain. Each event's
  `after` lists the events it immediately follows (no other proven event in between), each with its
  `sources`, `source` (the first of them) and `why` (the sentence the screen shows).
- The events are listed in an order that agrees with every proven edge, and cut into **groups** at
  every point where everything before the cut is proven before everything after it. A group with
  more than one event (observed or inferred) is **concurrent**: the screen shows it as one box
  labelled "concurrent, order unknown". Some pairs inside a box may still be ordered; their `after`
  entries say so, and the listing agrees with them. Otherwise the order inside a box (by `end`) only
  keeps the listing stable and means nothing.
- An event whose start was not observed (`start: null`, `start_observed: false`: a Lambda receive,
  where only an upper bound is known) is never after anything by the `clock` or `process_order`
  sources; the receive count and SQS's timestamps can still order it.
- Events are **never re-ordered by cause**: a rule such as "a receive comes after its send" or "a
  release comes before its redelivery" is not a source. If a host's clock is behind, its delete can be
  shown before the receive whose handle it used (its `delivery` still says which one).
- If the sources contradict each other (a cycle, for example a host clock that jumped back between
  two calls of one process), no order is shown between the events on the cycle, the dropped edges are
  in `order.conflicts`, and an `order_conflict` notice says so. 0.1 does not correct for clock offset
  between hosts.

Every event has `id` (`e1`, `e2`, ... in display order; valid within one response), `type`,
`inferred`, `group` (its group's number; `null` for an inferred event alone between two groups) and
`after`.

**Observed events** (`inferred: false`) come from one qv2 span each, and have `start`, `end`,
`start_unix_nano`, `end_unix_nano` (strings), `start_observed`, `time` and `time_unix_nano` (the same
as `end`, kept for clients of the first 0.1 API), `queue`, `consumer` and `span`. `consumer` holds whichever of `service_name`, `client_id`
(`messaging.client.id`), `source` (`client` or `lambda`), `pid`, `thread_name`, `faas_name` and
`faas_invocation_id` the span carried (contract §5). `span` holds `trace_id` and `span_id` (hex), for
linking to a tracing backend.

| `type` | Extra fields |
|---|---|
| `sent` | `batch_entry_id` (batch sends) |
| `received` | `delivery` (1, 2, ... in observed order), `receive_count` (SQS `ApproximateReceiveCount`), `sent_timestamp`, `first_receive_timestamp` (SQS `ApproximateFirstReceiveTimestamp`) |
| `deleted` | `delivery`, `result` (`ok`/`failed`), `status`, `error_code`, `batch_entry_id` |
| `visibility_changed` | `delivery`, `result`, `visibility_timeout_s` (`0` makes the message visible at once), `error_code`, `batch_entry_id` |
| `lambda_outcome` | `outcome` (`success`, `failed_reported`, `failed_invalid_response`, `failed_exception`), `error_type`, `delivery` |
| `moved_to_dead_letter_queue` | Observed, but with no span and no time (below): `source` (`sqs_dlq_source`), `reported_by`, `from_queue`, `to_queue`, `source_arn`, `reported_on_delivery`, `window`, `reason`, `after`, `before` |

**The move to a dead-letter queue** (contract §9b) is observed: SQS reports it on the receive from the
dead-letter queue, whose `DeadLetterQueueSourceArn` names the queue it came from. It is
`inferred: false` with `source: "sqs_dlq_source"` and `reported_by: "reported by SQS
(DeadLetterQueueSourceArn on the DLQ receive)"`. SQS gives the move no time, so it has `time: null`,
no `start`/`end`/`span`, and a `window`. It is placed like an inferred event, by two anchors: after the
last receive on `from_queue` (or the send, if no receive there was observed), and before the first
receive that reports it (`reported_on_delivery`). Its `after` entry and `before` field have source
`sqs_dlq_source`. Like inferred anchors, they never appear in `order.edges` and never order two
observed events. There is one move per source and dead-letter queue pair. The store **never infers** a
move from the message ID alone: a receive on another queue without the attribute shows no move.

`delivery` on a delete or visibility change is the delivery whose receipt handle was used, found by the
receipt-handle hash join (below). It is `null` if no stored receive has that hash.

`deleted.status`:

| Value | Meaning |
|---|---|
| `final` | SQS accepted the delete, and the handle is from the message's **latest** receive. The message is gone. |
| `stale_handle` | SQS accepted the delete, but the handle is from an earlier delivery. SQS accepts old handles without always deleting the message (contract §4.3), and the later receive shows it did not. |
| `failed` | SQS rejected the delete (`error_code` says why). |
| `unmatched_handle` | Accepted, but no stored receive has this handle's hash, so it is not known which delivery it settled. See `notices`. |

**Inferred events** (`inferred: true`) have **no span**. The store deduced them (contract §9). They have
`time: null`, a `window` (`from`, `to`; either may be `null` when unknown), and a `reason` sentence to
show the user. Show them differently from observed events.

An inferred event is placed by the observed events it was inferred from (its anchors, such as the
two receives around a visibility-timeout expiry), never by a time of its own, and is **ordered against
another event only through its anchors**: it is after an event exactly when that event is its earlier
anchor or proven before it, and before an event exactly when that event is its later anchor or proven
after it. Anything else may have happened on either side of it, so it shares a group with the inferred
event. Its `after` entry and `before` field name its anchors (source `inferred_from`); these never
appear in `order.edges` and never order two observed events. Its `window` runs from the earlier
anchor's `start` (or `null` if unknown) to the later anchor's `end`
(`lambda_invocation_ended_without_outcome`: the receive's end plus the Lambda timeout). For a
visibility-timeout expiry, the earlier anchor is the last `ChangeMessageVisibility` SQS accepted for the
earlier delivery that is proven before the later receive, if there is one (the timer restarted then),
else the earlier receive. Example: a rejected delete of delivery 1 between delivery 1 and its
redelivery shares a group with the expiry, since the store cannot tell whether the delete came before
or after it.

| `type` | When | Extra fields |
|---|---|---|
| `visibility_timeout_expired` | A receive on a queue follows an earlier receive on the same queue with a lower receive count. The `reason` says whether a `ChangeMessageVisibility` to 0 or a delete that came too late explains it. | `queue`, `after_delivery`, `before_delivery` |
| `deliveries_not_observed` | The receive count jumps by more than one, or the first receive on the source queue has a count above 1: receives by a consumer without qv2, or lost spans. | `queue`, `count` |
| `deleted_by_lambda` | After a Lambda `success` outcome: the event source mapping deletes the message, unobserved. `window.from` is the outcome time. | `queue`, `delivery` |
| `lambda_invocation_ended_without_outcome` | A Lambda receive with no outcome after `lambda_timeout_s`: most likely a timeout or out-of-memory kill. | `queue`, `delivery`, `faas_invocation_id` |

### Notices

`notices` is a list of `{"code", "message"}` to show above the timeline:

| `code` | Meaning |
|---|---|
| `default_key` | Some spans use the published default receipt-hash key (key ID `2bd63d10`). |
| `key_mismatch` | A delete or visibility change for this message used a different hash key than its receives, so it cannot be joined. The services do not share `QV2_RECEIPT_HASH_KEY`. |
| `unmatched_handle` | A delete or visibility change carries this message ID, but no stored receive has its hash. |
| `send_not_observed` | No send span: the producer is not instrumented, or the send is older than the data. |
| `dlq_move_not_seen` | "Receive count exceeded maxReceiveCount; not seen on the DLQ yet." See `dead_letter_queue_move`. |
| `order_conflict` | The observed facts contradict each other about the order of some steps (for example, a host clock that jumped), so no order is shown between them; see `order.conflicts`. |

## `GET /api/messages`: recent messages

Messages by latest activity, newest first. Parameters: `limit`, `queue`.

```json
{"messages": [<summary>, ...]}
```

A **summary** is:

```json
{
  "message_id": "...", "state": "deleted", "source_queue": "orders", "current_queue": "orders",
  "in_dead_letter_queue": false, "deliveries": 2, "successful_deliveries": 2,
  "consumers": [{"service_name": "orders-worker", "client_id": "consumer-a"}],
  "first_seen": "...", "last_seen": "..."
}
```

`consumers` lists the distinct consumers that received the message.

## `GET /api/duplicates`: duplicate deliveries

Messages received more than once, or with more than one successful delivery. Newest activity first.
Parameters: `limit`, `queue`, and `reason` to list only one kind:

- `received_more_than_once`: more than one observed receive (redeliveries after a visibility timeout,
  SQS at-least-once duplicates, retries after a failure);
- `succeeded_more_than_once`: more than one delivery ended in an accepted delete or a Lambda success,
  so the work was done more than once.

```json
{"messages": [<summary> + {"reasons": ["received_more_than_once", "succeeded_more_than_once"]}, ...]}
```

## `GET /api/stuck`: stuck messages

Messages first seen more than `older_than_s` seconds ago (default: the server's `--stuck-after`, 300 s)
that have no final delete: no accepted delete with the handle of the latest receive, and no Lambda
`success` for the latest receive. Includes messages never received, and messages waiting in a
dead-letter queue (`in_dead_letter_queue: true`). Oldest first. Parameters: `older_than_s` (0 to 10⁹),
`limit`, `queue`.

```json
{"older_than_s": 300, "messages": [<summary> + {"age_s": 3600.0}, ...]}
```

## `GET /api/health`

```json
{
  "status": "ok", "version": "0.1.0.dev0",
  "ingest": {"requests": 12, "inserted": 340, "duplicates": 0, "ignored": 55, "rejected": 0},
  "store": {"spans": {"send": 100, "receive": 110, "delete": 100, "change_visibility": 3, "outcome": 0},
            "messages": 100, "unjoined_settles": 0},
  "config": {"stuck_after_s": 300, "lambda_timeout_s": 900}
}
```

`ingest` counts since the server started: `ignored` spans had another instrumentation scope, `rejected`
qv2 spans broke the contract. `store.unjoined_settles` is the number of deletes and visibility changes
whose receive has not arrived (or never will, for example with a different hash key).

## Known limits in 0.1

- Two queues with the same name in different accounts or regions are merged (contract §6).
- The `clock` source across hosts relies on their clocks (see "Events and their order"); the store does not
  correct for clock offset between hosts. The local demo runs everything on one host, one clock.
- No retention: data is kept until the SQLite file is deleted.
- List endpoints build a timeline for each listed message, so they are meant for the `limit` sizes the
  screens use, not for exports.
