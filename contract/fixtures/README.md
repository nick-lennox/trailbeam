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
