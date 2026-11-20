# Tenants: the query

Self-hosted Trailbeam (Python, SQLite) and the hosted app (TypeScript, its own store) both show a
Tenants page, a tenant filter and a tenant search. This file defines them once. Both implementations
are tested against the same inputs and the same expected outputs:

- inputs: [`fixtures/contract-3.2-spans.json`](fixtures/contract-3.2-spans.json), 32 spans recorded
  from the Node.js package (contract revision 3.2) against canned SQS responses. Four OTLP protobuf
  exports, one per service. Do not edit it; it is re-recorded by `scripts/record-contract-spans.cjs`
  in the hosted app's repository.
- expected outputs: [`fixtures/tenants-expected.json`](fixtures/tenants-expected.json).

Self-hosted Trailbeam checks them in `python/tests/server/test_tenants.py`. The app keeps a copy of
both files and checks them in its own tests. When this file or the expected outputs change, update
both.

The tenant itself comes from the packages: `trailbeam.tenant` on send, receive and Lambda outcome
spans (span contract, section 15). Trailbeam doesn't choose the field; each service's package
configuration does (`TRAILBEAM_TENANT_ATTRIBUTE` or `TRAILBEAM_TENANT_BODY_PATH`, section 15.1).

## A message's tenant

A span names a tenant when its `trailbeam.tenant` attribute is a non-empty string. Only send, receive
and outcome spans count. The value is cut to its first 256 characters. The packages send no longer value: they drop a tenant
longer than 256 characters rather than cut it (span contract, section 15.2), so the cut only matters for
spans from other senders.

A message's tenant is the one its earliest send span names. If no send span names one, it is the one
its earliest receive or outcome span names. Earliest is by span end time, then by trace ID, then by
span ID. A message whose spans name no tenant has none.

## Tenants page

**Inputs:** `from` and `to`, in milliseconds since the epoch, both included; `now`, for building the
timelines (in nanoseconds, as everywhere else); the Lambda timeout and maxReceiveCounts settings the
timeline uses.

**Which messages:** every message with at least one stored span whose end time is in [`from`, `to`].
There is no cap: self-hosted Trailbeam read at most 2,000 messages until 0.2 and set `capped` when there
were more; it now counts every message and `capped` is always false. An implementation that still caps
must set `capped` when it left messages out, and the page says so. Each message counts as its timeline
says, by the usual rules (`timeline.py`, and the app's port of it). Self-hosted Trailbeam doesn't build
the timelines for this page: it keeps what each message adds to it (`summary.py`) as spans arrive, adds
that up per hour, queue and tenant (`rollup.py`), and its tests check both against the timelines and
against adding up every message.

**Which events count**, from each message's timeline. An event's time is its end time in whole
milliseconds (nanoseconds divided by 1,000,000, rounded down). Only events whose time is in [`from`,
`to`] count.

| Event | Its time | Counts as |
|---|---|---|
| `sent` | `end_unix_nano` | one send |
| `received` with `delivery` 1 and `receive_count` 1 | `end_unix_nano` | one first delivery, with its time in queue (below) |
| `moved_to_dead_letter_queue` | `window.to` (the receive that reported the move) | the message was dead-lettered |

Every other event (later receives, deletes, visibility changes, Lambda outcomes and anything
inferred) does not count.

**Time in queue** of a first delivery: its time minus the time of the message's first `sent` event in
timeline order. If the message has no `sent` event, its time minus the receive's `sent_timestamp_ms`
(SQS's SentTimestamp, so SQS's clock against the consumer's). If neither exists, the delivery has no
time in queue and is not counted. A negative difference (clock skew) counts as 0.

**Outputs:**

| Field | What it is |
|---|---|
| `total_sent` | sends in the range, with or without a tenant |
| `untagged_sent` | sends in the range whose message has no tenant |
| `capped` | messages in the range were left out (always false in self-hosted Trailbeam, which has no cap) |
| `rows` | one per tenant that has at least one send, first delivery with a time in queue, or dead-lettered message in the range |

Each row:

| Field | What it is |
|---|---|
| `tenant` | the tenant |
| `sent` | sends in the range whose message has this tenant |
| `share` | `sent / total_sent`, or 0 when `total_sent` is 0 |
| `p95_in_queue_ms` | p95 of the tenant's times in queue, within 1% of the nearest-rank p95 (below). `null` when there are none |
| `first_deliveries` | how many times in queue the p95 is over |
| `dead_lettered` | distinct messages with this tenant that have a `moved_to_dead_letter_queue` event in the range |

Rows are ordered by `sent` (highest first), then `dead_lettered` (highest first), then `tenant` in
code-point order.

So a tenant appears when a message of theirs was sent, first delivered or dead-lettered in the range,
even if it was sent before the range began. Its `sent` is then 0, and its share 0.

**p95.** The nearest-rank p95 of n times in queue is the one at position ceil(0.95 × n), counting from 1,
when they are sorted ascending. An implementation may return it exactly, or an estimate within 1% of it:
a whole number of milliseconds, at most 1% above or below it (|returned − exact| ≤ 0.01 × exact). Every
other field is exact.

Self-hosted Trailbeam returns an estimate, so the page costs the same however much traffic its range
covers. It keeps the times in queue as counts per bucket, which add up across hours and queues
(a sketch, after DDSketch: Masson, Rim and Lee, VLDB 2019, <https://arxiv.org/abs/1908.10693>). Times
up to 100 ms have a bucket each and come back exact. Above that, each bucket is about 1% wide and comes
back as the whole number nearest its middle: at most 0.91% off. A p95 of 100 ms or less is therefore always
exact. The same spans always give the same estimate, so the same range shows the same number on every
request and on every replica.

## Tenant filter

The messages whose spans name a tenant, most recently active first. A span names the tenant as above,
and any send, receive or outcome span of the message counts, not only the one that decides the
message's tenant. The name must match exactly.

## Tenant search

For a search box with suggestions: the tenants named by any span whose name starts with the text,
ignoring case for the ASCII letters A to Z only. An exact match (ignoring case) comes first, then the
rest by when a span last named them (most recent first), then in code-point order. At most `limit`
(default 8). `%` and `_` are ordinary characters. Empty text gives no suggestions.

The search can be bounded, so it costs the same however much history is kept: by a time range (only
tenants a message names whose newest span naming them is in the range) and by a queue (only messages
with a span on it). Self-hosted Trailbeam's `/api/tenants/search` bounds it by the Tenants page's
range (`from_ms`, `to_ms`, `since_s`: default the last day) and takes `queue`. The expected outputs
are unbounded.

## What the expected outputs cover

`fixtures/tenants-expected.json` has `now_ns` (60 s after the last span ends) and these cases:

- `stats`: the page for a range, given as `from_ms` and `to_ms`. The whole fixture, a range that
  starts after the sends but before the dead-letter move, and an empty range.
- `filter`: the message IDs for a tenant, sorted (order by activity is checked separately in each
  implementation, since the fixture's spans are milliseconds apart).
- `search`: suggestions for a text.

`p95_in_queue_ms` is the exact nearest-rank value, and a result passes when its p95 is within
`p95_relative_error` (0.01) of it, as above. Every other number is compared exactly; `share` is a double
computed as `sent / total_sent` in both.
