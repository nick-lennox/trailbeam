# Quickstart: a message's timeline in under 15 minutes

From a clean machine to the timeline of one SQS message, on your laptop. Everything runs locally against
an SQS emulator: no AWS account and no AWS credentials are needed or used.

You need **git** and **Docker with Compose v2** (Docker Desktop on macOS or Windows, or Docker Engine with
the compose plugin on Linux), and about 2 GB of free disk for the images (about 1.5 GB once built). Check:

```sh
git --version
docker compose version        # v2.x or later
```

## 1. Get the code (1 minute)

```sh
git clone https://github.com/nick-lennox/qv2
cd qv2
```

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

Messages worth opening (the demo sends each kind on a fixed schedule; see
[`demo/README.md`](../demo/README.md#what-to-look-at)):

- one with **2 deliveries** on the start page: a `ChangeMessageVisibility` to 0, an inferred
  visibility-timeout expiry, and a second delivery, often to a consumer in the other language;
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

The page shows a notice that the **default receipt-hash key** is in use. That is expected in the demo. To
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
  `demo/check.sh python` or `demo/check.sh node`. See
  [`demo/README.md`](../demo/README.md#concurrency-check) for what it checks and its current result.

## If something goes wrong

| Symptom | Fix |
|---|---|
| `docker: 'compose' is not a docker command` | Install Compose v2 (Docker Desktop includes it; on Linux, the `docker-compose-plugin` package). |
| `Cannot connect to the Docker daemon` | Start Docker Desktop, or `sudo systemctl start docker`. |
| `port is already allocated` on 4318 | Use `QV2_DEMO_PORT`, as in step 2. |
| The start page is empty | Wait a few seconds more; spans are exported in batches. Then `docker compose -f demo/compose.yaml logs collector qv2`. |
| WSL without Docker Desktop's WSL integration | Run the same commands with `docker.exe` instead of `docker`, or turn on the integration for your distribution in Docker Desktop's settings. |
