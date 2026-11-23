# Node.js Lambda with an SQS trigger

A Lambda function ([`handler.js`](handler.js)) wrapped with `trailbeam.wrapHandler`, with two SQS
triggers: `orders`, and its dead-letter queue `orders-dlq`. The triggers have ReportBatchItemFailures on,
so the handler returns the failed orders in `batchItemFailures` and only those come back. A producer
([`producer.js`](producer.js), AWS SDK v3) sends three orders and exits.

```sh
docker compose -f examples/node-lambda/compose.yaml up --build
```

Then open the links the producer prints. What each order's timeline shows, and how to clean up:
[examples](../README.md). One difference from a worker: the function never calls `DeleteMessage`.
Lambda's poller deletes the records that succeeded, out of sight of the function, so the timeline shows
that delete as **inferred** ("Deleted by Lambda") from the function's outcome.

## How it runs without AWS

- `function` is the function in the AWS Lambda base image for Node.js (`public.ecr.aws/lambda/nodejs:22`),
  whose Runtime Interface Emulator takes invocations on port 8080 as the Lambda service would deliver them.
- `event-source-mapping` ([`../shared/event-source-mapping/esm.py`](../shared/event-source-mapping/esm.py))
  stands in for the poller AWS runs for each SQS trigger: it receives a batch, invokes the function with
  the SQS event, and deletes the records that succeeded. It has no Trailbeam hooks, like AWS's poller.

In AWS you need neither: create the function and its triggers (event source mappings) with
`FunctionResponseTypes: ["ReportBatchItemFailures"]`. Deploy `trailbeam` in your function's `node_modules` (`npm install
trailbeam`), and set `TRAILBEAM_RECEIPT_HASH_KEY`, `OTEL_SERVICE_NAME` and
`TRAILBEAM_OTLP_TRACES_ENDPOINT` in its environment. More in
[AWS Lambda](https://docs.trailbeam.app/instrument/lambda).
