# Quickstart: one message's timeline on your laptop

This tutorial runs qv2 on your machine against ElasticMQ, an SQS emulator. At the end you open the
timeline of one SQS message in the web screen and as JSON. It needs no AWS account and uses no AWS
credentials.

To learn how the parts fit together, read [How qv2 works](architecture.md) after this page.

## Before you start

You need:

- git;
- Docker with Compose v2: Docker Desktop on macOS or Windows, or Docker Engine with the compose
  plugin on Linux;
- about 2 GB of free disk for the images (about 1.5 GB once built);
- access to the private repository `nick-lennox/qv2` on GitHub, and the
  [GitHub CLI](https://cli.github.com/) logged in with that account (`gh auth login`).

Check the tools:

1. Run `git --version`. It prints a version.
2. Run `docker compose version`. It prints `v2` or later.
3. Run `docker info`. Look for a `Server Version:` line. If the output has a
   `failed to connect to the docker API` or `Cannot connect to the Docker daemon` line instead, the
   daemon is not running. `docker info` prints a `Server:` heading even then, so check for
   `Server Version:`. See [If something goes wrong](#if-something-goes-wrong).

## 1. Get the code

1. Run `gh repo clone nick-lennox/qv2`.
2. Run `cd qv2`.

A plain `git clone https://github.com/nick-lennox/qv2` fails with
`fatal: could not read Username for 'https://github.com'`, unless git already has credentials for an
account with access.

## 2. Start the demo

1. Run this command from the repository root:

   ```sh
   docker compose -f demo/compose.yaml up -d --build --wait
   ```

   The first run downloads the ElasticMQ, Collector, Python and Node.js base images and builds four
   images: the store, the Collector image, and the Python and Node.js demo programs. It takes 1 to 5 minutes, depending on download speed. The command returns when every
   container is running.

2. Run `docker compose -f demo/compose.yaml ps`. It lists 7 containers with status `Up`:

   | Service | What it is |
   |---|---|
   | `sqs` | ElasticMQ 1.6.15. The demo programs create the queue `orders` and its dead-letter queue `orders-dlq`. |
   | `producer` | Python, with the `qv2` boto3 hooks. Sends one message per second to `orders`. |
   | `consumer-python` | Python, 4 consumer threads on `orders`, and 1 thread that receives and deletes from `orders-dlq`. |
   | `consumer-node` | Node.js, with `@qv2/sdk`. 4 concurrent consumers on `orders`. |
   | `deleter` | Python. A separate process that deletes messages by receipt handle, for messages the consumers hand to it. |
   | `collector` | The OpenTelemetry Collector with [`collector/qv2-collector.yaml`](../collector/qv2-collector.yaml). |
   | `qv2` | The qv2 store and web screen, on `127.0.0.1:4318`. It shows the status `(healthy)`. |

`orders` has a visibility timeout of 10 s and a redrive policy to `orders-dlq` with
`maxReceiveCount` 3.

If port 4318 is in use on your machine, see [Use another port](#use-another-port).

## 3. Look up a message

1. Wait about one minute, so that some messages go through.
2. Open `http://127.0.0.1:4318/`. The start page lists the most recent messages.
3. Click a message ID. The message page shows its timeline: the send, each delivery with its receive
   count and consumer, and the delete.

### Read the timeline

Each observed event shows an interval, for example
`… 12:00:01.204 to … 12:00:01.231 (27.0 ms)`. The interval starts just before the SQS request was
sent and ends when the response came back. SQS acted at some moment inside it.

The store shows event A above event B only when an observed fact proves that A came first. Each fact
is a source, and the page names it under the event:

| Source | The page says |
|---|---|
| `clock` | A's interval ended before B's started. |
| `process_order` | One process: A's call returned before B's call was issued. |
| `receive_count` | Two receives on one queue: SQS gave A the lower receive count. |
| `sqs_timestamps` | The send and the first receive: SQS's `SentTimestamp` is earlier than its `ApproximateFirstReceiveTimestamp`. |

When no source orders some events, the page draws them in one dashed box with a `concurrent` badge and
the label "concurrent, order unknown". Inside the box, each event is a bar on one time scale. The rows
are sorted by request start time. That is not the order in which SQS acted.

Expect this box often around a send and its first receive. An idle consumer waits in a long poll
(`WaitTimeSeconds`), so its `ReceiveMessage` started before the message existed. ElasticMQ also
stamps `SentTimestamp` and `ApproximateFirstReceiveTimestamp` in the same millisecond, which proves
nothing. qv2 does not order events by cause, so the send and the receive stay concurrent.

Events that the store inferred, rather than observed, have a dashed border and an `inferred` badge.
They show a time window and a reason instead of a time. An example is a visibility timeout that
expired.

A receive from a Lambda wrapper shows only "no later than" one time. The wrapper sees the event after
Lambda received the message, so only an upper bound is known.

### Messages to open

The demo producer sends each kind of message on a fixed schedule
([`demo/README.md`](../demo/README.md#what-to-look-at)).

| Look for | Kind | What the timeline shows |
|---|---|---|
| Deliveries `2 (1 ok)`, First seen and Last seen about equal | `retry_once` | A visibility change to 0 on delivery 1, an inferred visibility-timeout expiry, and delivery 2, which is deleted. |
| Deliveries `2 (1 ok)`, Last seen about 13 s after First seen | `slow` | An inferred visibility-timeout expiry, delivery 2 to another consumer, which deletes the message, then the first consumer's late delete. That delete is `failed` with `ReceiptHandleIsInvalid`, because ElasticMQ rejects a stale receipt handle. |
| Deliveries `4 (1 ok)`, after about one minute | `poison` | Three receives on `orders` with receive counts 1 to 3, an inferred visibility-timeout expiry after each of the first two, a move to the dead-letter queue "reported by SQS (DeadLetterQueueSourceArn on the DLQ receive)", a receive on `orders-dlq`, and a final delete there. |

Also open these lists:

- `http://127.0.0.1:4318/duplicates`: messages received more than once, or that succeeded more than
  once.
- `http://127.0.0.1:4318/stuck`: messages without a final delete for more than 60 s. The demo sets
  `QV2_STUCK_AFTER_S` to 60.

Every message page shows the notice for the published default receipt-hash key. The start page does
not. This is expected in the demo. To use your own key, see [Use your own key](#use-your-own-key).

### Get the same data as JSON

1. Run these commands. They read `QV2_DEMO_PORT` if you set it:

   ```sh
   port=${QV2_DEMO_PORT:-4318}
   id=$(curl -s "http://127.0.0.1:$port/api/messages?limit=1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["messages"][0]["message_id"])')
   curl -s "http://127.0.0.1:$port/api/messages/$id" | python3 -m json.tool
   ```

   The output starts like this:

   ```json
   {
       "message_id": "3150c7b8-65f2-44f1-8a1c-3e88df0a11bb",
       "state": "deleted",
       "source_queue": "orders",
       "current_queue": "orders",
       "in_dead_letter_queue": false,
       "dead_letter_queue_move": null,
       "send_observed": true,
       "deliveries": 1,
       ...
   ```

2. Run `curl -s http://127.0.0.1:4318/api/health`. It returns `"status": "ok"`, the span counts per
   kind, and `"max_receive_counts": {"orders": 3}`.

The fields are described in [`store-api.md`](store-api.md).

## 4. Stop

- Run `docker compose -f demo/compose.yaml down` to stop the demo. The timelines stay on the volume
  `qv2-demo_qv2-data`.
- Run `docker compose -f demo/compose.yaml down -v` to stop the demo and delete the timelines.

## Next steps

- Read [How qv2 works](architecture.md): components, data flow, what is inferred, failure cases.
- Instrument your own services: [Python](../python/README.md), [Node.js](../node/README.md). Then run
  the [store container](../server/README.md) and, optionally, the
  [Collector](../collector/README.md). In 0.1 nothing is published: install everything from this
  repository.
- Run the concurrency check, which compares 200 timelines with what the processes really did:
  [`demo/README.md`](../demo/README.md#concurrency-check).

## Options

### Use another port

1. Run `export QV2_DEMO_PORT=4400`.
2. If you run `docker.exe` from WSL, also run
   `export WSLENV="COMPOSE_PROJECT_NAME:QV2_DEMO_PORT${WSLENV:+:$WSLENV}"`. Windows programs see only
   the WSL variables that `WSLENV` lists. Without this line, `docker.exe` uses port 4318.
3. Run `docker compose -f demo/compose.yaml config | grep published`. It prints `published: "4400"`.
4. Run `docker compose -f demo/compose.yaml up -d --build --wait`.
5. In step 3, use port 4400 in the URLs. The JSON commands read the variable.

### Use your own key

Run the demo with a random receipt-hash key:

```sh
QV2_RECEIPT_HASH_KEY=$(openssl rand -hex 32) docker compose -f demo/compose.yaml up -d --build --wait
```

New spans then use this key, and new message pages show no default-key notice. In your own services,
set the same key on every service that uses the same queues.

## If something goes wrong

| Symptom | Fix |
|---|---|
| `docker: 'compose' is not a docker command` | Install Compose v2. Docker Desktop includes it. On Linux, install the `docker-compose-plugin` package. |
| `Cannot connect to the Docker daemon`, or `failed to connect to the docker API at unix:///var/run/docker.sock` | Start Docker Desktop, or run `sudo systemctl start docker`. |
| The same error in WSL, or `The command 'docker' could not be found in this WSL 2 distro` | Docker Desktop's WSL integration is off for your distribution. Turn it on in Docker Desktop (Settings, Resources, WSL integration). Or use `docker.exe` in place of `docker` in every command. |
| `port is already allocated` on 4318 | [Use another port](#use-another-port). |
| A demo from another qv2 checkout is running | Both checkouts use the Compose project `qv2-demo`. Starting this one replaces the other's containers and uses its volume. Follow [More than one checkout on the same machine](../demo/README.md#more-than-one-checkout-on-the-same-machine). |
| The start page is empty | Wait a few seconds: spans are exported in batches. Then run `docker compose -f demo/compose.yaml logs collector qv2`. |
| A build step fails with `CERTIFICATE_VERIFY_FAILED` | A proxy on your network intercepts TLS, and the build containers do not trust its certificate. The Dockerfiles have no setting for this in 0.1. |
