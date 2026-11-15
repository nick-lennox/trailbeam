# Glossary

Each term below has one meaning in the qv2 docs. Amazon SQS and OpenTelemetry terms keep their
upstream meaning: visibility timeout, in flight, receive count, receipt handle, `maxReceiveCount`,
redrive policy, dead-letter queue, span, attribute, instrumentation scope.

| Term | Meaning in qv2 |
|---|---|
| anchor | An observed event that an inferred event, or the dead-letter queue move, was placed by. Each has at most an earlier and a later anchor. |
| client hooks | The Python boto3 hooks (`qv2.boto.install()`) and the Node.js AWS SDK v3 middleware (`qv2.instrument(client)`). |
| concurrent group | A group with more than one event. The screen labels it "concurrent, order unknown". |
| delivery | One receive of a message, numbered 1, 2, ... by the end times of the receive spans. A delivery has its own receipt handle. |
| edge | A proven "A before B" between two observed events, with the sources that prove it. Listed in `order.edges`. |
| event | One entry in a timeline. Observed events come from one span each. Inferred events have no span. The web screen calls events "steps". |
| final delete | A delete that SQS accepted, made with the receipt handle of the message's latest receive. |
| group | A run of events in the timeline such that every event before it is proven before every event in it, and every event in it is proven before every event after it. |
| inferred | Deduced by the store from other events, with no span. Marked `inferred: true`. |
| Lambda wrapper | `qv2.aws_lambda.wrap` (Python) or `qv2.wrapHandler` (Node.js), for handlers invoked by an SQS event source mapping. |
| observed | Backed by a qv2 span, or, for the dead-letter queue move, reported by SQS. Marked `inferred: false`. |
| qv2 span | A span with instrumentation scope `qv2`. The store reads only these. |
| receipt-hash key | The secret key for the receipt-handle hash, set with `QV2_RECEIPT_HASH_KEY`. Every service on the same queues must use the same key. |
| scope (call order) | `qv2.order.scope`: the ID of one call-order counter. One per Python process, one per Node.js realm. |
| settle span | A `qv2 delete` or `qv2 change_visibility` span. |
| source | An observed fact that proves an edge: `clock`, `process_order`, `receive_count` or `sqs_timestamps`. |
| store | The `qv2-store` process: OTLP/HTTP ingest, SQLite, the JSON API and the web screen on one port. |
| tick | A value of the call-order counter: `qv2.order.issued` (before the request) or `qv2.order.returned` (after the response). |
