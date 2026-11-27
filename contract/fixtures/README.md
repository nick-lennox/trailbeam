# Recorded spans

`contract-3.2-spans.json` holds 32 real spans in 4 protobuf OTLP exports, made by this repository's
Node.js package (contract revision 3.2, plus one older checkout for spans with no capture status)
against canned SQS responses. No AWS account was used; account `123456789012` and every body are
made up.

The hosted app tests against a byte-identical copy, so the open-source server and the app read the
same inputs. Do not edit it by hand: re-record it, and replace both copies together.

| Service | Spans | What they show |
|---|---|---|
| `checkout-api` | 6 | sends to `orders` (body captured, tenant), `payments` (capture off), `orders.fifo` (FIFO) |
| `orders-worker` | 15 | receives and deletes, a receive with no instrumented producer (`m-orphan`), a move to `payments-dlq` |
| `billing-fn` | 8 | a Lambda on `billing` that throws, with bodies and error text captured |
| `legacy-svc` | 3 | a package from before 3.2: no `trailbeam.body.capture_status` |

Used by `python/tests/server/test_store_bodies.py`.

## `body-views.json`

What [`../body-view.md`](../body-view.md) gives for those spans, under four `TRAILBEAM_STORE_BODIES`
settings: each message's Body tab (`GET /api/messages/{id}/body`) and the result of a list of
`body.*` searches. It also holds term cases written by hand from the spec (a body, a query, whether
it matches). It is written by `make-body-views.py` from the open-source server; don't edit it by hand.

Checked by `python/tests/server/test_body_view.py` and `ui/test/body.test.ts` here, and by the hosted
app's `test/body-shared.test.ts`, which runs the app's own store and explorer on the same spans.
`@trailbeam/ui` ships both files under `@trailbeam/ui/fixtures/`.

## `amqp-4.0-spans.json`

RabbitMQ spans of contract 4.0 ([`../span-contract-amqp.md`](../span-contract-amqp.md)) for the scenarios of the
QV-303 proof: a normal ack, requeues, a nack loop that never dead-letters, a crash loop to the dead-letter queue,
a reject to it, fan-out, a publisher retry, an unroutable publish, a classic queue, a message without
`message_id`, a consumer timeout and a stuck delivery. They are **written** by `make-amqp-spans.py` from the
contract, not recorded, because no package emits them yet; re-record them from the Python package (QV-309)
and keep the scenarios. Checked by `python/tests/test_amqp_contract.py`, which also fails if the file is not
what the script writes.

## `alerts-parity.json`

The alert rule cases both engines must agree on (QV-627): a dashboard rule's tone against its warn and crit limits,
the changes between two checks (fired, changed, resolved, a rule gone), a cause rule at one check (the quiet period
and its 5-minute slack, every check, mutes, a merge of error texts) and one quiet rule walked through a day of
checks. It is written by `make-alerts-parity.py` from the open-source engine (`server/src/trailbeam/server/alerts.py`);
don't edit it by hand. Checked by `python/tests/server/test_alerts.py`, which also fails if the file is not what the
script writes, and by the hosted app's `test/alerts-parity.test.ts` against its `lib/alerts/evaluate.ts`.
`@trailbeam/ui` ships it as `@trailbeam/ui/fixtures/alerts-parity.json`, so there is one file and no copy.

## `measures-parity.json`

The dashboard widget measure cases both engines must agree on (QV-632): every measure as a series (all of it, by queue,
over other ranges, with no AWS figures), group-by fields, filters in the Event explorer's query language (fields,
wildcards, numbers and ranges, body paths, attributes, bare text, bad terms), a queue's figure, missing figures and
history and why, queue health, filters with dashboard variables, number formatting, the 95th percentile, histogram
buckets, what a widget with a rule alerts with, and checking a widget and a dashboard definition. The inputs are
datasets in the hosted app's EventData shape (a workspace's own events and each queue's AWS figures), so the app passes
them in unchanged. It is written by `make-measures-parity.py` from the open-source engine
(`server/src/trailbeam/server/measures.py`); don't edit it by hand. Checked by `python/tests/server/test_measures.py`,
which also fails if the file is not what the script writes, and by the hosted app's `test/measures-parity.test.ts`
against its `lib/dashboards/measures.ts`. `@trailbeam/ui` ships it as `@trailbeam/ui/fixtures/measures-parity.json`.

## `alerts-delivery.json`

What an alert sends, which both products must agree on (QV-636): for a webhook, Slack and email channel, the request
(URL, headers with the HMAC signature, body) or the emails a check's changes make, from dashboard rules and "Alert me"
cause rules (quiet and every check, resolved, after a snooze, regressed, back from ignored, triage, a capped read, an
inferred or long label, escaping); which channels want which change; and how a channel's target is checked (http or
https, private and local hosts, Slack's own URLs, email lists) and shown without its secret part. Links in it are the
hosted app's, `<origin>/app/<slug>/...`; self-hosted Trailbeam makes the same under its own URL. It is written by
`make-alerts-delivery.py` from the open-source code (`server/src/trailbeam/server/alert_messages.py` and
`alert_delivery.py`); don't edit it by hand. Checked by `python/tests/server/test_alert_delivery.py`, which also sends
every case for real to a local HTTP server and a fake SMTP server and fails if the file is not what the script writes,
and by the hosted app's `test/alerts-delivery.test.ts` against its `lib/alerts/deliver.ts` and `channels.ts`.
`@trailbeam/ui` ships it as `@trailbeam/ui/fixtures/alerts-delivery.json`.
