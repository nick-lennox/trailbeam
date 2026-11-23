# Node.js with AWS SDK v3

A producer, a worker and a dead-letter reader, each a separate process using one client wrapped with
`trailbeam.instrument(new SQSClient(...))` ([`sqs.js`](sqs.js)):

- [`producer.js`](producer.js) sends three orders to `orders` and exits. The package sends its spans
  when the script ends; only a script that calls `process.exit()` needs `await trailbeam.forceFlush()`.
- [`worker.js`](worker.js) receives from `orders` and deletes each order it processed. A failed order is
  not deleted, so SQS delivers it again after the visibility timeout.
- [`dlq.js`](dlq.js) receives from `orders-dlq` and deletes. That receive is what shows the move to the
  dead-letter queue: SQS reports it on the message (`DeadLetterQueueSourceArn`).

```sh
docker compose -f examples/node-sdk-v3/compose.yaml up --build
```

Then open the links the producer prints. What each order's timeline shows, and how to clean up:
[examples](../README.md).

In your own service: `npm install trailbeam @aws-sdk/client-sqs`, and set `TRAILBEAM_RECEIPT_HASH_KEY`
(the same in every service on the queue), `OTEL_SERVICE_NAME`, and, if Trailbeam is not on the same
machine, `TRAILBEAM_OTLP_TRACES_ENDPOINT`. See [`compose.yaml`](compose.yaml) for all of them.
