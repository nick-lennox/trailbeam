# Quickstart: a message's timeline in under 15 minutes

From a clean machine to the timeline of one SQS message, on your laptop. Everything runs locally against
an SQS emulator: no AWS account and no AWS credentials are needed or used.

You need **git** and **Docker with Compose v2** (Docker Desktop on macOS or Windows, or Docker Engine with
the compose plugin on Linux), and about 2 GB of free disk for the images (about 1.5 GB once built). Check:

```sh
git --version
docker compose version        # v2.x or later
docker info                   # contacts the Docker daemon; must print a "Server:" section without errors
```

`docker compose version` only checks that the CLI is installed. `docker info` checks that the CLI can
reach the daemon, which every command below needs. If it fails, see [If something goes wrong](#if-something-goes-wrong).

For now the repository is **private**: you need a GitHub account that has been given access to
`nick-lennox/qv2`, and the [GitHub CLI](https://cli.github.com/) logged in with it (`gh auth login`).

## 1. Get the code (1 minute)

```sh
gh repo clone nick-lennox/qv2
cd qv2
```

A plain `git clone https://github.com/nick-lennox/qv2` fails with
`fatal: could not read Username for 'https://github.com'` while the repository is private, unless git
already has credentials for an account with access.

## 2. Start the demo (1 to 5 minutes the first time, depending on download speed)

```sh
docker compose -f demo/compose.yaml up -d --build --wait
```

The first run downloads the ElasticMQ, Collector, Python and Node.js base images and builds four images from
the repository (the store, the Collector configuration, and the Python
and Node.js demo programs) and starts:

- **ElasticMQ**, an SQS-compatible emulator, with a queue `orders` and its dead-letter queue `orders-dlq`;
- a **producer** in Python, sending one message a second with the `qv2` boto3 hooks installed;
- **consumers** in Python and in Node.js (four each, sharing the queue), and a **deleter** process that
  deletes messages another process received;
- the **OpenTelemetry Collector**, receiving the spans from all of them;
- the **qv2 store**, keeping the spans in SQLite and serving the web screen on `127.0.0.1:4318`.

When the command returns, every container is running. Check:

```sh
docker compose -f demo/compose.yaml ps
```

If port 4318 is taken on your machine, start with `QV2_DEMO_PORT=4400 docker compose -f demo/compose.yaml up -d --build --wait`
and use that port below.

## 3. Look up a message (1 minute)

Wait about a minute for some messages to go through, then open **http://127.0.0.1:4318/**.

The start page lists the most recent messages. Click any message ID, or paste one into the lookup box, to
see its timeline: when it was sent, by which process, each delivery with its receive count and consumer,
and the delete. Steps the store **inferred**, rather than observed, are drawn with a dashed border and an
INFERRED label, for example a visibility-timeout expiry.

Each step shows two times: when its SQS request was sent and when the response came back (for example
`… 12:00:01.204 to … 12:00:01.231 (27.0 ms)`). SQS did the work somewhere in between, and qv2 cannot see
exactly when. So the timeline shows one step above another **only if the first one's response came back
before the second one's request was sent**. When two steps' times overlap, qv2 cannot tell which one SQS did
first, and it does not guess: it draws them side by side in one dashed box with a **CONCURRENT** badge and
the label **concurrent, order unknown**. The order inside the box means nothing.

Expect to see that box often, most of all around a send and its first receive. An idle consumer waits in a
long poll (`WaitTimeSeconds`), so its receive request was usually sent before the message existed, and
its interval contains the send. That is the honest answer, not a fault: the send certainly happened first,
but the times alone do not prove it, and qv2 shows only what the times prove. A Lambda receive shows
**no later than** one time: the wrapper sees the event only after Lambda received it, so it knows an upper
bound and nothing else.

Messages worth opening (the demo sends each kind on a fixed schedule; see
[`demo/README.md`](../demo/README.md#what-to-look-at)):

- one with **2 deliveries** on the start page. The demo makes two kinds, and both show `2 (1 ok)` in the
  Deliveries column:
  - a **`ChangeMessageVisibility` to 0** (`retry_once`): the consumer releases the message on its first
    receive, the timeline shows the visibility change, an inferred visibility-timeout expiry, and a second
    delivery, often to a consumer in the other language, which deletes it. Everything happens within a
    second or two, so **First seen** and **Last seen** are about the same;
  - a **slow consumer** (`slow`): the first consumer works for 13 s, past the 10 s visibility timeout, so
    the timeline shows an inferred visibility-timeout expiry and a second delivery to another consumer,
    which deletes it, and then the first consumer's late delete, `failed` with `ReceiptHandleIsInvalid`
    (ElasticMQ rejects a stale handle). There is no visibility change. **Last seen** is about 13 s after
    **First seen**.

  On the message page, the difference is the **Visibility changed** step (first kind) or the failed delete
  from delivery 1 (second kind);
- **http://127.0.0.1:4318/duplicates**: messages delivered more than once, including ones delivered again
  because the consumer took longer than the 10 s visibility timeout;
- after about a minute, a message with **4 deliveries**: a poison message, failed three times, then an
  inferred move to the dead-letter queue and a receive on `orders-dlq`;
- **http://127.0.0.1:4318/stuck**: messages without a final delete for more than 60 s.

The same data as JSON, for example the newest message's timeline:

```sh
id=$(curl -s 'http://127.0.0.1:4318/api/messages?limit=1' | python3 -c 'import json,sys; print(json.load(sys.stdin)["messages"][0]["message_id"])')
curl -s "http://127.0.0.1:4318/api/messages/$id" | python3 -m json.tool
```

The API is described in [`docs/store-api.md`](store-api.md).

Each message page (`/message?id=…`) shows a notice that the **default receipt-hash key** is in use. The
start page does not. That is expected in the demo. To
use your own key, start the demo with `QV2_RECEIPT_HASH_KEY=$(openssl rand -hex 32)` in front of the
`docker compose` command; in your own services, set the same secret on every service that uses the same
queues.

## 4. Stop

```sh
docker compose -f demo/compose.yaml down          # keeps the stored timelines on the qv2-data volume
docker compose -f demo/compose.yaml down -v       # deletes them too
```

## Next

- **Your own services:** install the hooks ([Python](../python/README.md), [Node.js](../node/README.md)),
  run the [Collector configuration](../collector/README.md) and the [store container](../server/README.md).
  In 0.1 the packages and the image are installed from this repository; nothing is published.
- **The concurrency check** (three processes of four consumers each, 200 messages):
  `demo/check.sh python` or `demo/check.sh node`. In WSL without Docker Desktop's WSL integration, tell it
  to use `docker.exe`: `DOCKER=docker.exe demo/check.sh python`. See
  [`demo/README.md`](../demo/README.md#concurrency-check) for what it checks and its current result.

## If something goes wrong

| Symptom | Fix |
|---|---|
| `docker: 'compose' is not a docker command` | Install Compose v2 (Docker Desktop includes it; on Linux, the `docker-compose-plugin` package). |
| `Cannot connect to the Docker daemon` | Start Docker Desktop, or `sudo systemctl start docker`. |
| `port is already allocated` on 4318 | Use `QV2_DEMO_PORT`, as in step 2. |
| You have another qv2 checkout with the demo running | Both use the Compose project `qv2-demo`, so starting this one replaces the other's containers and uses its timelines volume. Give this one its own name and port: `COMPOSE_PROJECT_NAME=qv2-other QV2_DEMO_PORT=4400 docker compose -f demo/compose.yaml up -d --build --wait`, and keep the same `COMPOSE_PROJECT_NAME` for every later command. See [`demo/README.md`](../demo/README.md#more-than-one-checkout-on-the-same-machine). |
| The start page is empty | Wait a few seconds more; spans are exported in batches. Then `docker compose -f demo/compose.yaml logs collector qv2`. |
| In WSL, `docker info` or any other command fails with `failed to connect to the docker API at unix:///var/run/docker.sock`, or says `The command 'docker' could not be found in this WSL 2 distro` | Docker Desktop's WSL integration is off for your distribution. Turn it on in Docker Desktop's settings (Resources, WSL integration), or run the same commands with `docker.exe` instead of `docker` (`docker.exe compose -f demo/compose.yaml up -d --build --wait`), and `DOCKER=docker.exe demo/check.sh python` for the concurrency check. |
