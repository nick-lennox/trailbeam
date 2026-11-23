# Examples

The smallest complete service for each common way of using SQS, instrumented with `trailbeam`. Each one
runs with a single command against an SQS emulator ([ElasticMQ](https://github.com/softwaremill/elasticmq))
and a Trailbeam built from this checkout. No AWS account, no AWS credentials: only Docker with Compose v2.

| Example | What it is |
|---|---|
| [`python-boto3`](python-boto3/) | Python producer, worker and dead-letter reader with boto3 |
| [`python-lambda`](python-lambda/) | Python Lambda function with SQS triggers (event source mapping) |
| [`node-sdk-v3`](node-sdk-v3/) | Node.js producer, worker and dead-letter reader with AWS SDK v3 |
| [`node-lambda`](node-lambda/) | Node.js Lambda function with SQS triggers (event source mapping) |

Run one from the repository root, for example:

```sh
docker compose -f examples/python-boto3/compose.yaml up --build
```

The producer prints a Trailbeam link for each of the three orders it sends. Open them (or
http://127.0.0.1:4318/ and click a message ID) after about 20 seconds:

| Order | What the service does | What its timeline shows |
|---|---|---|
| `A-1001` | processes it | sent, received, deleted |
| `A-1002` | fails on the first receive, succeeds on the second | sent, received, visibility timeout expired (inferred), received again, deleted |
| `A-1003` | fails every time | received 3 times, then moved to the dead-letter queue `orders-dlq` (reported by SQS), received there |

Ctrl-C stops it. To remove its containers and the images it built:

```sh
docker compose -f examples/python-boto3/compose.yaml down -v --rmi local
```

Port 4318 already in use (for example by the [demo](../demo/README.md) or your own Trailbeam)? Set
another one: `TRAILBEAM_EXAMPLE_PORT=4400 docker compose -f examples/python-boto3/compose.yaml up --build`.

## What they share

- [`shared/compose.yaml`](shared/compose.yaml): the emulator and Trailbeam, included by every example.
- [`shared/elasticmq.conf`](shared/elasticmq.conf): the queues, created when the emulator starts, the way
  you would create them with Terraform or the console. `orders` has a 5 s visibility timeout, so a failed
  message comes back quickly, and a redrive policy to `orders-dlq` after 3 receives. The services never
  create queues.
- [`shared/event-source-mapping`](shared/event-source-mapping/esm.py): for the Lambda examples, a stand-in
  for the poller AWS runs for an SQS trigger (see [python-lambda](python-lambda/README.md#how-it-runs-without-aws)).
- [`check.py`](check.py) and [`run.sh`](run.sh): what CI runs for every example. `examples/run.sh
  python-boto3` starts it, waits until the three timelines above are in Trailbeam (and no span was
  rejected), and removes everything it created.

The receipt-hash key in the compose files is for the examples only: make your own with
`openssl rand -hex 32` and keep it secret ([Add it to your services](../README.md#add-it-to-your-services)).
