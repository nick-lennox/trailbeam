# Python with boto3

A producer, a worker and a dead-letter reader, each a separate process with
`trailbeam.boto.install()` before its first SQS call:

- [`producer.py`](producer.py) sends three orders to `orders` and exits.
- [`worker.py`](worker.py) receives from `orders` and deletes each order it processed. A failed order is
  not deleted, so SQS delivers it again after the visibility timeout.
- [`dlq.py`](dlq.py) receives from `orders-dlq` and deletes. That receive is what shows the move to the
  dead-letter queue: SQS reports it on the message (`DeadLetterQueueSourceArn`).

```sh
docker compose -f examples/python-boto3/compose.yaml up --build
```

Then open the links the producer prints. What each order's timeline shows, and how to clean up:
[examples](../README.md).

In your own service: `pip install trailbeam boto3`, and set `TRAILBEAM_RECEIPT_HASH_KEY` (the same in
every service on the queue), `OTEL_SERVICE_NAME`, and, if Trailbeam is not on the same machine,
`TRAILBEAM_OTLP_TRACES_ENDPOINT`. See [`compose.yaml`](compose.yaml) for all of them.
