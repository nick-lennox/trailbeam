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
  "send_observed": true,
  "deliveries": 2,
  "successful_deliveries": 1,
  "first_seen": "2025-11-14T00:26:40.030000Z",
  "last_seen": "2025-11-14T00:27:21.000000Z",
  "concurrent_groups": 1,
  "groups": [
    {"group": 0, "concurrent": true, "label": "concurrent, order unknown", "observed_events": 2,
     "inferred_events": 0, "start": "2025-11-14T00:26:39.100000Z", "end": "2025-11-14T00:26:40.030000Z"},
    {"group": 1, "concurrent": false, "label": null, "observed_events": 1, "inferred_events": 0,
     "start": "2025-11-14T00:27:19.900000Z", "end": "2025-11-14T00:27:20.000000Z"},
    {"group": 2, "concurrent": false, "label": null, "observed_events": 1, "inferred_events": 0,
     "start": "2025-11-14T00:27:20.950000Z", "end": "2025-11-14T00:27:21.000000Z"}
  ],
  "events": [
    {"type": "received", "inferred": false, "group": 0,
     "start": "2025-11-14T00:26:39.100000Z", "start_unix_nano": "1763080039100000000",
     "end": "2025-11-14T00:26:40.020000Z", "end_unix_nano": "1763080040020000000", "start_observed": true,
     "time": "2025-11-14T00:26:40.020000Z", "time_unix_nano": "1763080040020000000",
     "queue": "orders", "consumer": {"service_name": "orders-worker", "client_id": "consumer-a", "source": "client", "pid": 4242, "thread_name": "worker-1"},
     "span": {"trace_id": "8f0c...", "span_id": "0000000000000002"}, "delivery": 1, "receive_count": 1},
    {"type": "sent", "inferred": false, "group": 0, "start": "2025-11-14T00:26:40.000000Z", "end": "2025-11-14T00:26:40.030000Z", "...": "..."},
    {"type": "visibility_timeout_expired", "inferred": true, "group": null, "time": null,
     "window": {"from": "2025-11-14T00:26:39.100000Z", "to": "2025-11-14T00:27:20.000000Z"},
     "reason": "The visibility timeout expired before the message was deleted, so SQS delivered it again.",
     "queue": "orders", "after_delivery": 1, "before_delivery": 2},
    {"type": "received", "inferred": false, "group": 1, "...": "...", "delivery": 2, "receive_count": 2},
    {"type": "deleted", "inferred": false, "group": 2, "...": "...", "delivery": 2, "result": "ok", "status": "final"}
  ],
  "notices": []
}
```

Here the receive's long poll (from 39.100) was already waiting when the message was sent (40.000 to
40.030), and its response (40.020) came back before the send's: the two intervals overlap, so they
are one concurrent group and the store does not say which came first.

### Summary fields

| Field | Meaning |
|---|---|
| `state` | `deleted`: an observed final delete (below). `deleted_inferred`: the latest delivery was to Lambda and its outcome was `success`, so Lambda deleted it (not observed). `processing`: the latest delivery is a Lambda invocation still within `lambda_timeout_s`, with no outcome yet. `not_deleted`: anything else. |
| `source_queue` | Queue of the first `sent` event, or of the first receive if the send was not observed. |
| `current_queue` | Queue of the latest receive (or `source_queue`). |
| `in_dead_letter_queue` | `current_queue` differs from `source_queue`. |
| `deliveries` | Number of observed receives, on any queue. |
| `successful_deliveries` | Deliveries that ended in an accepted delete (counted once per delivery) or a Lambda `success`. More than one means the message was processed more than once. |
| `first_seen` | Send time (the end of the send call); if the send was not observed, the earliest SQS `SentTimestamp` from a receive. |
| `groups` | The timeline's groups, in order (below). Each has `group` (its number), `concurrent` (more than one observed event), `label` (`"concurrent, order unknown"` or `null`), `observed_events`, `inferred_events`, `start` (earliest start; `null` if any member's start is unknown) and `end` (latest end). |
| `concurrent_groups` | Number of groups with `concurrent: true`. |

### Events and their order

Each observed event is an interval (contract §2): `start` is just before the SQS request was sent,
`end` is when its response arrived, and SQS acted somewhere in between. Events are in display order,
by this rule (contract §9a):

- **Event A is shown before event B only if A ended before B started** (`A.end < B.start`).
- Events whose intervals overlap, directly or through a chain of overlapping events, form one
  **group**. A group with more than one observed event is **concurrent**: its order is unknown, and
  the screen shows it side by side, labelled "concurrent, order unknown". Inside a group, events are
  listed by `end` only so that the listing is stable; that order means nothing.
- An event whose start was not observed (`start: null`, `start_observed: false`: a Lambda receive,
  where only an upper bound is known) is treated as starting at minus infinity, so it is concurrent
  with everything that started before its end.
- Events are **never re-ordered by cause**. If a host's clock is behind, its delete can be shown
  before the receive whose handle it used (its `delivery` still says which one); the store shows what
  the clocks say. 0.1 does not correct for clock offset between hosts.

Every event has `type`, `inferred` and `group` (its group's number; `null` for an inferred event
placed between groups).

**Observed events** (`inferred: false`) come from one qv2 span each, and have `start`, `end`,
`start_unix_nano`, `end_unix_nano` (strings), `start_observed`, `time` and `time_unix_nano` (the same
as `end`, kept for clients of the first 0.1 API), `queue`, `consumer` and `span`. `consumer` holds whichever of `service_name`, `client_id`
(`messaging.client.id`), `source` (`client` or `lambda`), `pid`, `thread_name`, `faas_name` and
`faas_invocation_id` the span carried (contract §5). `span` holds `trace_id` and `span_id` (hex), for
linking to a tracing backend.

| `type` | Extra fields |
|---|---|
| `sent` | `batch_entry_id` (batch sends) |
| `received` | `delivery` (1, 2, ... in observed order), `receive_count` (SQS `ApproximateReceiveCount`), `sent_timestamp` |
| `deleted` | `delivery`, `result` (`ok`/`failed`), `status`, `error_code`, `batch_entry_id` |
| `visibility_changed` | `delivery`, `result`, `visibility_timeout_s` (`0` makes the message visible at once), `error_code`, `batch_entry_id` |
| `lambda_outcome` | `outcome` (`success`, `failed_reported`, `failed_invalid_response`, `failed_exception`), `error_type`, `delivery` |

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
two receives around a visibility-timeout expiry), never by a time of its own: just before the later
anchor's group, or, if that group is concurrent, inside it (it then has that `group` number and is
part of the concurrent group, not ordered against its members). An event with only an earlier anchor
(`deleted_by_lambda`, `lambda_invocation_ended_without_outcome`) goes just after that anchor, in the
same way. Its `window` runs from the earlier anchor's `start` (or `null` if unknown) to the later
anchor's `end`.

| `type` | When | Extra fields |
|---|---|---|
| `visibility_timeout_expired` | A receive on a queue follows an earlier receive on the same queue with a lower receive count. The `reason` says whether a `ChangeMessageVisibility` to 0 or a delete that came too late explains it. | `queue`, `after_delivery`, `before_delivery` |
| `deliveries_not_observed` | The receive count jumps by more than one, or the first receive on the source queue has a count above 1: receives by a consumer without qv2, or lost spans. | `queue`, `count` |
| `moved_to_dead_letter_queue` | The message ID is received on another queue than the source queue. SQS keeps the ID when it moves a message; when exactly it moved is not known. | `from_queue`, `to_queue` |
| `moved_back_to_source_queue` | Received on the source queue again after another queue (a redrive). | `from_queue`, `to_queue` |
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
- Time order across hosts relies on their clocks (see "Events and their order"); the store does not
  correct for clock offset between hosts. The local demo runs everything on one host, one clock.
- No retention: data is kept until the SQLite file is deleted.
- List endpoints build a timeline for each listed message, so they are meant for the `limit` sizes the
  screens use, not for exports.
