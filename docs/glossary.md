# Glossary

Each term below has one meaning in the Trailbeam docs. Amazon SQS and OpenTelemetry terms keep their
upstream meaning: visibility timeout, in flight, receive count, receipt handle, `maxReceiveCount`,
redrive policy, dead-letter queue, span, attribute, instrumentation scope.

| Term | Meaning in Trailbeam |
|---|---|
| anchor | An observed event that an inferred event, or the dead-letter queue move, was placed by. Each has at most an earlier and a later anchor. |
| client hooks | The Python boto3 hooks (`trailbeam.boto.install()`) and the Node.js AWS SDK v3 middleware (`trailbeam.instrument(client)`). |
| concurrent group | A group with more than one event. The screen labels it "concurrent, order unknown". |
| delivery | One receive of a message, numbered 1, 2, ... by the end times of the receive spans. A delivery has its own receipt handle. |
| edge | A proven "A before B" between two observed events, with the sources that prove it. Listed in `order.edges`. |
| event | One entry in a timeline. Observed events come from one span each. Inferred events have no span. The web screen calls events "steps". |
| final delete | A delete that SQS accepted, made with the receipt handle of the message's latest receive. |
| group | A run of events in the timeline such that every event before it is proven before every event in it, and every event in it is proven before every event after it. |
| inferred | Deduced by the store from other events, with no span. Marked `inferred: true`. |
| Lambda wrapper | `trailbeam.aws_lambda.wrap` (Python) or `trailbeam.wrapHandler` (Node.js), for handlers invoked by an SQS event source mapping. |
| observed | Backed by a Trailbeam span, or, for the dead-letter queue move, reported by SQS. Marked `inferred: false`. |
| Trailbeam span | A span with instrumentation scope `trailbeam`. The store reads only these. |
| receipt-hash key | The secret key for the receipt-handle hash, set with `TRAILBEAM_RECEIPT_HASH_KEY`. Every service on the same queues must use the same key. |
| scope (call order) | `trailbeam.order.scope`: the ID of one call-order counter. One per Python process, one per Node.js realm. |
| settle span | A `trailbeam delete` or `trailbeam change_visibility` span. |
| source | An observed fact that proves an edge: `clock`, `process_order`, `receive_count` or `sqs_timestamps`. |
| store | The `trailbeam-store` process: OTLP/HTTP ingest, SQLite, the JSON API and the web screen on one port. |
| tick | A value of the call-order counter: `trailbeam.order.issued` (before the request) or `trailbeam.order.returned` (after the response). |
