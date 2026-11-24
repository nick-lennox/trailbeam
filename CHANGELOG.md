# Changelog

## Unreleased

- **`trailbeam --version`** (QV-346). `trailbeam --version` (or `-V`, also after `start`) prints
  `trailbeam <version>` and exits 0; it used to fail with `unrecognized arguments`. `npx trailbeam --version` prints
  the same line (it printed the bare version before).
- **No spans dropped when a Postgres statement times out** (QV-300). An export whose statement ran past
  `statement_timeout` (60 s: a lock pile-up, a very slow disk) was answered `500`, which OTLP exporters don't
  retry, so its spans were lost. It is now answered `503` with `Retry-After: 3` (`store_timeout` in
  `trailbeam_ingest_rejected_requests_total`), nothing stored, and logged at ERROR with the export's size; the
  exporter's retry stores it once the database is faster. An export that is always too slow is retried until the
  exporter gives up. Clients that time out a request before 60 s (most do) were not affected: the server cancels
  an export whose client hangs up, and the client sends it again. No change in the Python or Node.js packages:
  the retry is the OTLP exporter's.
- **Java package, stage 4: the Lambda wrapper** (QV-319, pre-release, not published). For a function invoked by
  an SQS event source mapping, set the handler to `dev.trailbeam.sqs.TrailbeamSqsLambdaWrapper::handleRequest`
  and name your own in `TRAILBEAM_LAMBDA_HANDLER`, or extend `TrailbeamSqsHandler`. Each record gets a
  `trailbeam receive`, exported before the handler runs, and a `trailbeam outcome` after it, with the batch
  fields and opt-in error text (`TRAILBEAM_CAPTURE_ERROR_QUEUES`). `trailbeam.error_type` is the exception's fully
  qualified class name, which is what Lambda's Java runtime reports as `errorType`. After a SnapStart restore the
  jar starts a new call-order scope and `service.instance.id`, through `org.crac` when it is on the classpath. The
  conformance kit has a Java Lambda driver (`drivers/java-lambda`, on `public.ecr.aws/lambda/java:21`), and CI
  runs it in the Java job.
- **`service.version` is kept, so a deploy can be seen** (QV-362, the server half). Each stored span keeps its
  resource's `service.version` beside `service.name` (SQLite schema 16, Postgres schema 10; rows stored before
  have none). A timeline step's `consumer` has `service_version` when it is set, and the Timeline shows it after
  the service name. The list rows' `consumers` (messages, duplicates, stuck) each have `service_version`, null
  when unknown; a service and client seen under two versions is listed once for each. `/api/messages`,
  `/api/duplicates` and `/api/stuck` take `version=`: only messages a consumer of that version received (not
  indexed: read off each candidate's summary; `version` with `body` is a `400`). The conformance suite checks
  the filter on both stores.
- **Java package, stage 3: tenants and body capture** (QV-319, pre-release, not published). The jar now
  implements contract section 15 like the Python and Node.js packages, and passes the same `capture` vectors:
  the tenant from `TRAILBEAM_TENANT_ATTRIBUTE` or `TRAILBEAM_TENANT_BODY_PATH`, opt-in body capture per queue
  (`TRAILBEAM_CAPTURE_BODY_QUEUES`) with in-process redaction (`TRAILBEAM_REDACT_KEYS`,
  `TRAILBEAM_REDACT_PATTERNS`, `TRAILBEAM_CAPTURE_MAX_BYTES`), every capture status, the Extended Client's
  offloaded size, and SNS and EventBridge envelopes. Receives also ask for the tenant attribute and the
  Extended Client's size attributes. A bad redaction setting can't be refused at install time (the jar has none),
  so it fails closed: one warning, no body exported, `redaction_failed` on the listed queues.

- **Users and roles switch on when the first admin exists** (QV-225). Each replica checks the store at least
  every 5 seconds for an admin. Until there is one, nothing changes. After that:
  - The web screen and the API need a person: signing in with an email and password on `/login` (cookie
    `trailbeam_sid`, or `__Host-trailbeam_sid` over HTTPS), or a personal API token (`tbp_...`) as
    `Authorization: Bearer`. Admins and members can both read everything. Every route has a fixed level: open,
    ingest, metrics, member or admin.
  - **Changed:** `TRAILBEAM_TOKEN` keeps working for ingest and `/metrics` only. It no longer opens the web screen
    or the API, and its old `trailbeam_session` cookie stops working. The server warns at every start while it
    is set. Upgrade every replica before creating the first admin: an older replica would still let the token
    into the web screen.
  - **Ingest tokens** (`tbi_...`): named service tokens that can only send spans, as `x-trailbeam-key` (the hosted
    app's header) or `Authorization: Bearer`. `TRAILBEAM_TOKEN` is accepted in `x-trailbeam-key` too. Ingest needs
    a token when `TRAILBEAM_TOKEN` is set or the store holds an ingest token, revoked or not. **Changed:** creating
    the first ingest token closes ingest on a server that had no `TRAILBEAM_TOKEN`.
  - Tokens are stored as SHA-256 and checked through a per-process cache (ingest 30 s, personal 20 s). "Last
    used" is written at most once a minute per token and replica.
  - `GET /api/me` says how a request was let in and as whom. `/api/health` gains `config.ingest_auth`
    (`token` or `open`), and `config.auth` can be `accounts`.
  - **Changed:** `/metrics` without a valid token answers a plain-text `401`, not the HTML sign-in page.
  - **Changed:** with neither a token nor an ingest token, the server now warns at every start that ingest is
    open, also when it listens only on localhost.
- **The first admin, and people and tokens from the command line** (QV-225, part 4 of phase 1).
  - **`trailbeam start` prints a one-time setup link** while the store has no admin:
    `Create your admin account: http://127.0.0.1:4318/setup#code=...`. The form makes the first admin and signs
    them in. The code works once, for an hour, and each start replaces it. It is printed only when the output is
    a terminal (`npx trailbeam start` says so to the container), never in the log, and it sits after `#`, where
    browsers don't send it. `/setup` answers with `Referrer-Policy: no-referrer`. The code is used up and the
    admin created in one transaction, so of two forms sent at once only one makes an admin.
  - **Headless installs:** `TRAILBEAM_ADMIN_EMAIL` with `TRAILBEAM_ADMIN_PASSWORD_FILE` (and optionally
    `TRAILBEAM_ADMIN_NAME`) creates the first admin at start, only while the store has none. Replicas starting
    together make one. A password file that can't be used stops the server at start. Servers with no admin log
    how to make one.
  - **`trailbeam users create | list | set-role | reset-password | disable | enable | remove`** and
    **`trailbeam tokens create | list | revoke`**, with `--personal` for personal API tokens (`--user`,
    `--expires 90d`). They take the server's `--db` / `--store-url` (default: `trailbeam.sqlite` here, else
    `~/.trailbeam/trailbeam.sqlite`), work while the server runs, and refuse a store they would have to create or
    upgrade, and `--db :memory:`. A new token is printed once, alone on stdout. Passwords come from
    `--password-file` or are asked for twice, never as an argument. Creating the first ingest token says first
    that it closes open ingest, and asks (`--yes` without a terminal). `tokens list` shows the legacy shared
    token when `TRAILBEAM_TOKEN` is set. `users create` on Postgres says to upgrade every replica first.
  - **The server keeps an admin:** the last admin who can sign in can't be made a member, disabled or removed.
  - **The web screen** shows who is signed in, with **Sign out**, and goes to the sign-in page when the session
    ends. The Body tab says who can read bodies on this server. The first-run screen shows the token line when
    ingest needs a token.
  - CI runs `test_replicas_e2e.py` on Postgres, now also the switch to accounts on two replicas at once.

- **Conformance kit: the Lambda wrappers** (QV-339, QV-318 stage 2). `trailbeam.aws_lambda.wrap` (Python) and
  `wrapHandler` (Node.js) run in the AWS Lambda base images, invoked through their Runtime Interface Emulator by
  a poller that plays Lambda's SQS event source mapping (`conformance/drivers/lambda-poller`). Cases: a success,
  a reported failure in a batch of two, a redelivery, the move to the dead-letter queue after `maxReceiveCount`
  and its receive there, an invalid `batchItemFailures`, and a thrown exception. The checker's Lambda rules
  (`conformance/SCENARIO-LAMBDA.md`): `trailbeam.source = lambda` and `LambdaEvent`, zero-length receives with
  `trailbeam.start_observed = false` and a `returned` tick only, the four outcome values and `error_type` derived
  from the function's response, `faas.*`, `trailbeam.queue.arn` and the URL built from it, `trailbeam.batch.*`,
  and that every receive span reached the Collector before the handler started (the bench's Collector now
  stamps arrival times). Both wrappers pass in CI, with and without body capture and error text. Drivers can
  have a `setup` command, run once (the Lambda drivers build their image there).

- **Java package, stage 1** (QV-319, pre-release, not published). `java/` is a jar for the AWS SDK for Java v2:
  add it as a dependency and a global `ExecutionInterceptor` (registered through the SDK's classpath file, no code)
  emits the contract's send, receive, delete, change-visibility and purge spans, with the keyed receipt-handle
  hash, receive count, dead-letter source ARN, per-entry results, call-order ticks, body size and FIFO fields. It
  has its own always-on tracer provider with a relocated OpenTelemetry SDK (1.4 MB jar). The parent comes from the
  stock instrumentation's trace header, so interceptor order doesn't matter. A hand registration on top of the
  global one emits each span once. A Java driver for the conformance kit passes it in CI alone, beside the
  OpenTelemetry Java agent 2.34.0 (also sampled to 0) and beside dd-java-agent 1.69.1. Not yet built: the Lambda wrapper, the SDK version matrix, a strict parent check for the agent variants, and
  Maven Central.

- **`trailbeam doctor` reads the token the services send** (QV-343). With no `--token-file` or `TRAILBEAM_TOKEN`,
  it takes the Bearer token from `OTEL_EXPORTER_OTLP_TRACES_HEADERS` or `OTEL_EXPORTER_OTLP_HEADERS` (URL-decoded,
  as the exporters read it) before falling back to `~/.trailbeam/token`, and its header line says
  `token: OTEL_EXPORTER_OTLP_HEADERS`. So on a service's host, with exactly the variables `trailbeam start`
  prints, it no longer reports the token missing. A header with no `Authorization=Bearer%20<token>` is reported
  as such.

- **Conformance kit for Trailbeam packages** (QV-318, stage 1). `conformance/` runs a language package's small
  driver through one scripted scenario (send, batch send, receive, delete, batch delete with a failed entry,
  visibility-timeout retry, dead-letter move, ChangeMessageVisibility and its batch form, a failing delete,
  PurgeQueue) against ElasticMQ, collects its spans through an OpenTelemetry Collector, and checks them against
  the span contract with no per-language logic: one span per message, required attributes and types, the
  receipt-handle HMAC recomputed from the driver's ground truth, call-order ticks, no handle or key exported.
  Each driver runs alone, beside the application's own OpenTelemetry, with the application's sampler at 0, and
  with body capture on; agents are configured per variant. A failing package gets a report per call. The Python
  and Node.js packages pass it in CI. The Lambda wrapper (through the Runtime Interface Emulator) is stage 2.

- **Purged messages no longer show as stuck** (QV-245, contract revision 3.3). The Python and Node.js packages
  record `PurgeQueue` calls (a `trailbeam purge` span, `trailbeam.kind = sqs.purge`, contract section 18), and
  the server stores them per queue (SQLite: no schema change; Postgres: schema 9, a `purges` table). A message
  whose last step ended before a purge SQS accepted started is `purged`, reads "purged (observed: PurgeQueue at
  …)", gets a "Queue purged" step on its timeline and leaves Stuck. One whose last step ended during the purge or
  within 60 s after it keeps its state and stays on Stuck, marked "possibly purged". The API adds the `purged`
  state, an optional `purge` field and the `queue_purged` step. Stuck's footnote now says a message can also
  leave SQS when its retention period ends, which Trailbeam cannot see. A server older than 3.3 rejects the purge
  span (one rejected span in `partial_success`) and stores the rest of the export as before.

- **Alert on stuck messages and dead-letter moves from Datadog, Prometheus or Grafana** (QV-304). `/metrics` has
  four new gauges: `trailbeam_stuck_messages` (as the Stuck page counts them, up to 1000),
  `trailbeam_stuck_oldest_age_seconds`, `trailbeam_dead_lettered_messages_recent` (moves in the last 15 minutes)
  and `trailbeam_stuck_after_seconds`. They are read from the store at most every 10 seconds, and carry no queue
  or tenant label, so the number of series stays fixed. `deploy/monitoring` has the Datadog Agent's OpenMetrics
  check and two monitors, a Prometheus scrape config with alert rules and their unit tests, and a Grafana
  dashboard. CI runs the check in a real Datadog Agent (a dry run: no account or key) and the rest in a real
  Prometheus (`deploy/monitoring/check.sh`).
- **Open trace: from a timeline step to the same trace in your tracing tool** (QV-304). `--trace-link`
  (`TRAILBEAM_TRACE_LINK`) takes a URL template such as `https://app.datadoghq.com/apm/trace/{trace_id}`; each
  recorded step then shows **Open trace**, in the web UI and on the server-rendered page, and has the link as
  `span.trace_url` in `GET /api/messages/{id}`. `/api/health` shows the template under `config.trace_link`. A
  template that isn't an http or https URL, or names an unknown placeholder, stops the server at start.
- **`--trusted-proxy` (`TRAILBEAM_TRUSTED_PROXY`): the addresses of your load balancer or TLS proxy.**
  Trailbeam believes `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` only from them, and uses
  the client address in `X-Forwarded-For` for logs and (with users and roles) the sign-in limit per
  address. **Changed:** `X-Forwarded-Proto: https` was believed from anyone; now it counts only from a
  trusted proxy. If Trailbeam runs behind a TLS proxy, add the proxy's address with `--trusted-proxy`, or
  the login cookie loses its `Secure` flag (it keeps working). [`deploy/replicas`](deploy/replicas/compose.yaml)
  sets it for its nginx. (QV-225)
- **Forms posted from other sites are refused.** `POST /login` (and every later route that changes
  something, but not ingest) answers `403` when the browser says the request came from another site's page
  (`Sec-Fetch-Site: cross-site` or `same-site`, or an `Origin` that isn't the server). curl and scripts,
  which send neither header, are not affected. (QV-225)
- **Passwords and sessions for users and roles, not switched on yet.** scrypt password hashing (N=2^15,
  r=8, p=3, at most 2 hashes at once per process), sessions in the store with a 20-second cache per process,
  a lock per account after 10 failed sign-ins in a row and a limit per client address. Signing in (above)
  uses them. (QV-225)
- **Tables for people and tokens (users and roles, phase 1).** The store gains `users`, `sessions`,
  `ingest_tokens`, `personal_tokens` and `setup_codes`: SQLite schema 15, Postgres schema 8. They are created
  empty and nothing uses them yet, so the server behaves exactly as before. Emails are unique ignoring case.
  Retention and `--vacuum` leave the tables alone. With `--db :memory:`, people and tokens are lost on every
  restart, like the spans. **A database upgraded to this version can't be opened by 0.4.0 or earlier (0.3.0
  included) again**: back it up before upgrading. (QV-225)
- **Helm chart: a release named `trailbeam` no longer crash-loops** (QV-291). In 0.4.0 its Service, named
  `trailbeam`, made Kubernetes set `TRAILBEAM_PORT=tcp://<ip>:4318` in every pod of the namespace, and the server
  exited on it. The pods now have `enableServiceLinks: false` and `TRAILBEAM_PORT=4318`, and CI installs the
  chart as `trailbeam`. The server's error for a `tcp://` value now says it is a service link and how to fix it.
- **No spans dropped when the disk fills up** (QV-238). A full disk under SQLite was answered `500`, which OTLP
  exporters don't retry, so those exports were lost. Both stores now answer `503` with `Retry-After: 3`
  (`store_full` in `trailbeam_ingest_rejected_requests_total`) and nothing stored, and the exports are stored
  once space is freed, with no restart. Also fixed: under heavy concurrent ingest, a SQLite export queued
  behind more than one group commit's worth of others could be answered `500` and stored later anyway. The
  README now says what a `200` survives for each durability setting ("Delivery guarantee"). The fault tests
  behind it (SIGKILL mid-write, Postgres down for a minute, a full disk, Postgres restarted during a retention
  pass, a replica killed, every export sent twice, spans far out of order from skewed clocks) are
  `python/bench/faults.py`; the quick ones run in CI.
- **Backup and restore, documented and tested** (QV-239): the README's SQLite and Postgres sections now say to
  keep the access token and receipt-hash key with the backup (they aren't in the database), and how to restore
  onto a new machine with `trailbeam start`, the Docker volume of `npx trailbeam start`, the team install and the
  Helm chart. CI takes a backup with those commands while the server ingests, restores it, and checks that the
  restored server serves a consistent moment and that deletes sent after the restore join receives from before it
  (`python/smoke/backup_restore_check.py`).
- **The UI no longer says more than the data shows.** A message in its dead-letter queue now counts only the
  receives on its source queue ("after 2 observed receives on orders", not "after 3 receives"); 0.4.0 counted
  the receive from the dead-letter queue too, so with maxReceiveCount 3 it said "after 4 receives" (QV-341). maxReceiveCount
  reads "reached", not "more than", and the move it predicts is marked inferred. A receive with no delete yet
  reads "No delete seen" in amber rather than a red "Not deleted". "Being handled" is marked inferred. The
  Receives tooltip, the Tenants p95 note, the Stuck age note and the body tab's "not opted in" text say what
  they are computed from. The Tenants note no longer mentions a 2,000-message cap that no longer applies. (QV-247)
- FIFO queues: a second send that SQS deduplicated (same MessageId within the 5-minute deduplication window, nothing
  enqueued) is now marked `deduplicated: true` on the timeline, with the reason, instead of showing as a second
  plain "Sent" step. It no longer counts in the Tenants page's `total_sent`, a tenant's `sent` or its share. On
  upgrade, SQLite schema 14 and Postgres schema 7 fix the counts of messages already stored (QV-246).

- **Helm chart with the trial Postgres: the Service, the PDB and the Deployment reach the server only**
  (QV-342). In 0.4.0 the Postgres pod carried the same two labels the server's selectors use, so
  `kubectl port-forward service/<release>-trailbeam 4318:4318` (the documented check) failed with "does not have a
  named port 'http'", `kubectl logs deployment/...` could show Postgres's log, and the PodDisruptionBudget counted
  Postgres, so a drain could evict both server replicas. Server pods now carry `app.kubernetes.io/component:
  server`, and the Service, the PDB and the spread constraint select on it. `helm upgrade` from 0.4.0 needs no extra
  step and fixes the Service and the PDB. The Deployment's selector can't change in place, so a Deployment made by
  0.4.0 keeps its old one, and `kubectl logs deployment/...` may still pick the Postgres pod. To switch it to the
  new selector once (the server pods keep running; the new Deployment adopts their ReplicaSet):

  ```
  kubectl delete deployment <release>-trailbeam --cascade=orphan
  helm upgrade <release> <chart> <your settings>
  kubectl delete replicaset -l 'app.kubernetes.io/instance=<release>,app.kubernetes.io/name=trailbeam,!app.kubernetes.io/component'
  ```

  `deploy/helm/check.sh` now runs the documented port-forward, checks what each selector picks, and upgrades a
  release installed by the 0.4.0 chart.

## 0.4.0: set up in one command

Trailbeam 0.4.0 is about getting a server running and the first message on screen without reading the whole
README. `trailbeam start` (Python) and `npx trailbeam start` (Node.js, with Docker) make the access token and
receipt-hash key for you and print the settings to paste into your services. A first-run screen shows how to
send the first message and says why nothing has arrived yet, and `trailbeam doctor` checks each link from your
service to the store when nothing shows up. Teams get a `docker-compose.yaml` with Postgres, and a Helm chart for Kubernetes.

### What's new

- **`trailbeam start`** and **`npx trailbeam start`**: first-run setup in one command, with the token and key
  kept in `~/.trailbeam` (or the Docker volume `trailbeam-data`) and reused on later runs. The token and key
  are printed only on the run that made them, the data directory is kept private, and an existing
  `TRAILBEAM_RECEIPT_HASH_KEY` is used instead of making a new key.
- **`trailbeam doctor`**: checks that the server answers, the ingest port is open, the token is accepted and a
  test span is stored, and prints the likely cause and fix for a check that fails.
- **A first-run screen** at `/` while the store is empty, and one dark design for every page.
- **A team install from one file**: `docker-compose.yaml` runs the server with Postgres 16.
- **Kubernetes**: a Helm chart in `deploy/helm/trailbeam`, and a `<version>-postgres` image for it.
- **Clearer failures**: a port already in use is one line, not a traceback.
- **Node.js**: a short script no longer loses its last spans, and `trailbeam.uninstall()` no longer fails
  when the server is unreachable. `await trailbeam.forceFlush()` before a script ends is no longer needed,
  except before `process.exit()`.
- **Postgres**: the lookup tables are partitioned by day, so ingest and retention touch only recent data.
- **A down or slow server no longer holds up your service's exit**: the Python and Node.js packages wait at
  most 2 s (`TRAILBEAM_SHUTDOWN_TIMEOUT_MS`), and the drop warning says what to check.
- **Runnable examples** in [`examples/`](examples/README.md) for boto3, AWS SDK v3 and Lambda with an SQS
  trigger, each started with one `docker compose` command.

### Upgrading from 0.3.0

Take a backup ([SQLite](README.md#backup-and-restore-sqlite), [Postgres](README.md#backup-and-restore-postgres)),
stop the 0.3.0 server, then start 0.4.0 on the same SQLite file, Docker volume or Postgres database, with the
same flags and token. It upgrades the database by itself at start. CI checks this upgrade on every change
([Upgrading](README.md#upgrading)).

- **pip:** `pip install -U "trailbeam[server]"` (add `postgres` to the extras for Postgres).
  **Docker:** `docker pull ghcr.io/nick-lennox/trailbeam:0.4.0`, then recreate the container with the same
  volume. **npm** (the package in your services): `npm install trailbeam@0.4.0`. The packages in your services
  and the server can be upgraded in either order.
- **There is no going back without the backup.** SQLite moves from schema 12 to 13 and Postgres from schema 4
  to 6. Afterwards a 0.3.0 server refuses to start on that database (`written by a newer Trailbeam`), so it
  can't damage it.
- **Postgres: stop every 0.3.0 replica first**, then start 0.4.0. See the Postgres notes below.
- **The token and receipt-hash key don't change**, and signed-in browsers stay signed in.
- **Moving to `trailbeam start`** on an existing database: put your token in `DIR/token` and your key in
  `DIR/receipt-hash-key` first (or pass `--token-file`), so `start` doesn't make new ones. If your 0.3.0 server
  ran without a token, `start` turns one on and services that don't send it get 401
  ([Upgrading](README.md#upgrading)).

### Upgrading from 0.3.0 (Postgres)

**Stop every 0.3.0 replica, then start 0.4.0.** At start the server upgrades the database from Postgres
schema 4 to 5, one day partition at a time, then to 6, which only adds the small `probes` table. For schema 5 it copies each day's lookup rows into that day's new
partitions, then drops the old tables. On our test machine that took 8 s for 1M spans, and the first
retention pass after it sealed the 5 older days into the filter (4.5 s). A 0.3.0 replica left running would
write lookup rows the filter doesn't know about, so don't run 0.3.0 and 0.4.0 against one database at
the same time. Once the database is at schema 5 or later, a 0.3.0 server refuses to start on it. If the upgrade is
stopped part way, the next start runs it again; rows already copied are left as they are. A database at an older schema (from
0.2.0) goes through schema 4 first, as the 0.3.0 notes describe.

### All changes

- **Python: a down or slow Trailbeam server no longer holds up your process's exit.** Shutdown
  (`uninstall()`, or interpreter exit) waited for the OTLP exporter's retries and 10 s request timeout, so a
  short script took 6 s longer to exit with the server unreachable and 10 s longer with it slow. It now waits
  at most `TRAILBEAM_SHUTDOWN_TIMEOUT_MS` (default 2000) and counts what is left as `shutdown` drops. The
  Lambda wrapper's flushes now keep to `flush_timeout_millis` (the SDK ignored it). The drop warning now says
  what to check for each reason: for `export_failed`, the server's address and the token in
  `OTEL_EXPORTER_OTLP_HEADERS`, instead of advice about high volume. The Python README lists what happens to
  spans in each case. (QV-169)
- **Node.js: the same.** A short script with the server unreachable took 8 s longer to exit, 10 s with it slow,
  because the event loop waited for the OTLP exporter's retries and request timeout. `uninstall()` and the
  export when the event loop empties now keep to `TRAILBEAM_SHUTDOWN_TIMEOUT_MS` (2 s), the drop warning gives
  the same per-reason advice, and drops held back by the once-a-minute limit are printed at exit instead of
  being lost from the count. The Node.js README lists what happens to spans in each case. (QV-169)
- **Runnable examples.** [`examples/`](examples/README.md) has one small service for each common setup:
  Python with boto3, a Python Lambda function with an SQS trigger, Node.js with AWS SDK v3, and a Node.js
  Lambda function with an SQS trigger. Each runs with one `docker compose` command against ElasticMQ and a
  local Trailbeam, and sends three orders: one processed, one retried after the visibility timeout, one
  moved to the dead-letter queue. CI runs every example and checks the three timelines. (QV-172)
- **The Tenants page no longer slows down after heavy ingest.** It read every raw event of the current hour,
  so right after a burst of traffic it took 1–2 s on Postgres (QV-180). It now reads the current hour from the
  hourly rollup, less the few events newer than the window, and adds up the raw events it still reads in SQL.
  With 2M events in the current hour, the last day went from 2.07 s to 55 ms (p95, Postgres) and from 646 ms
  to 9 ms (SQLite, 626K events), with the same answers. (QV-224)
- **Body search takes plain text.** On the Messages page and in `GET /api/messages?body=`, `ord-1003` is
  searched as `body:ord-1003` instead of being refused with a 400. The **Body** field shows an example, and
  on a server that keeps no bodies (`/api/health` lists no `store_bodies`) it is off, with a line saying how to
  turn bodies on. A term for another field still fails, now as ``Can't read `queue:orders`. Use text, or
  `body.<path>:<value>`.``, without the HTTP status. (QV-215)
- **`trailbeam start` prints the token and key only on the run that created them.** Later runs print the
  command that shows each one (`cat ~/.trailbeam/token`) instead, and the export lines read the files with
  `$(cat ...)`, so a server restarted under systemd, `docker run -d`, nohup or CI no longer writes the token and
  the receipt-hash key into journald, `docker logs` or the build log on every start, and pasted export lines
  keep them out of shell history. A token given with `--token-file` or `TRAILBEAM_TOKEN` is never printed.
  `npx trailbeam start` with the Docker volume prints a `docker run ... cat /data/token` command. The first
  run is unchanged. (QV-203)
- **Kubernetes: a Helm chart.** `deploy/helm/trailbeam` installs the server as a Deployment of 2 replicas
  behind a Service, on a Postgres you run (`store.url` or a Secret), or on a trial Postgres in the cluster
  (`postgresql.enabled=true`). The token and receipt-hash key are generated once into a Secret and kept on
  upgrade, or come from your own Secret. Liveness probe on `/api/health`, readiness on `/api/ready`, and a
  startup probe long enough for a schema upgrade. CI installs it on a kind cluster and sends one span. It
  replaces the example manifest `deploy/kubernetes/trailbeam.yaml`; `helm template` renders plain manifests.
  Releases now also publish `ghcr.io/nick-lennox/trailbeam:<version>-postgres`, the image with the Postgres
  driver, which is the chart's default ([Kubernetes](README.md#kubernetes-helm-chart)). (QV-173)
- **Messages: Receives, not Deliveries.** The column on Messages, Duplicates and Stuck is now **Receives**, with a
  tooltip that defines it (how many times SQS returned the message from `ReceiveMessage`) and what *handled* means
  (a receive that ended in a delete or a successful Lambda record). Cells read `2 · 1 handled` instead of
  `2 (1 ok)`, and the count is Ember when the message was received more than once, so that row stands out. The
  timeline summary says **Receives** `2 (1 handled)` too, on the UI and the no-script pages (QV-209).
- **`trailbeam doctor`: why no data is arriving.** It checks, in plain lines, that the server answers, that the
  ingest port accepts connections, that the token is accepted, that a test span sent now is stored and read
  back, and when the last real span arrived. Each failed check prints the likely cause and the fix, and the exit
  code is 1 when one fails. By default it uses the address and token `trailbeam start` created
  ([README](README.md#troubleshooting-no-data-arriving)): without `--url`, the server of
  `TRAILBEAM_OTLP_TRACES_ENDPOINT`, or the address `trailbeam start` keeps in `address` in its data directory,
  so `trailbeam start --port N` is checked on N and not on 4318. The test span is a *probe* (span contract section 17):
  the server stores it apart from the spans (new table `probes`: SQLite schema 13, Postgres schema 6, both
  applied on start), so it never shows as a message or in a count, and reads it back at
  `GET /api/probes/{id}` (at most 8 probes an export, in one write). `GET /api/health` also gives `data.last_span_ns`, the time of the span stored last.
  A server of 0.3 or before rejects the probe; `trailbeam doctor` says so and the other checks still run.
  Once upgraded, a SQLite file can no longer be opened by 0.3.0, which refuses it at start (restore a backup to go back).
- **Upgrading from 0.3.0 is checked in CI.** A new job starts 0.3.0 (the PyPI package, and the published image),
  stores 1,200 messages' spans, then runs this version on the same SQLite file, volume or Postgres database. It
  checks that every timeline, list and tenant count reads as before, that messages in flight across the upgrade
  are joined, that the token and sessions keep working, and that 0.3.0 then refuses the upgraded SQLite file or
  Postgres database, which it can't read ([Upgrading](README.md#upgrading)).
- **`trailbeam start` on an existing database says what its new token and key change.** On a data directory with
  only a 0.3.0 SQLite file (0.3.0 had no `start`), `start` makes a token and a receipt-hash key, as on a first
  run. Now it also logs a warning: with the token on, services that send without it are refused (401); the
  services keep the key they had until they are given the new one. The warning says how to keep the old token
  and key. Nothing else changes. (QV-170)
- **Team install: `.env` readable by its owner only.** The README, the `docker-compose.yaml` header and
  `.env.example` now say `install -m 600 .env.example .env` instead of `cp`, which left the token and the
  Postgres password readable by every user of the server (QV-204).
- **`npx trailbeam start` says which image it runs; image versions are write-once.** Before the server starts,
  one stderr line names the image: its reference, local image ID and registry digest, and, when
  `TRAILBEAM_IMAGE` is set, that it replaces the default image. The image is pulled (only when missing) and
  inspected first, then run with `--pull never`, so the line names what runs. The release workflow now stops
  before pushing `ghcr.io/nick-lennox/trailbeam:X.Y.Z` when that tag is already published, so re-running a
  release can't change what an already-published npm version runs (QV-205).
- **`trailbeam start` keeps the data directory private.** Others on the machine could read the spans (the
  SQLite file was created with the umask, often 644) or plant a `token` that the server then used. Now an
  existing data directory has to be yours and not writable by every user, else `trailbeam start` (and
  `npx trailbeam start --data-dir`) stops with one line and status 2; one the group or others can use is made
  700. A `token` or `receipt-hash-key` that is a symlink or another user's is refused; one others can read is made
  600 instead of only warned about. The SQLite file and its `-wal` and `-shm` are created 600 (wherever `--db`
  puts them); an existing one of another user's is refused, and one others can read is made 600.
- **A first-run screen, and one design for every page.** While the store holds no span, `/` shows how to send
  the first message: install the package (a Python / Node.js switch), the OTLP endpoint of this server with a
  Copy button, the environment variables (with the token header when the server has a token, never the token),
  and a five-line example. A waiting line polls `/api/health` every 2 s and says why nothing has arrived yet when
  the ingest counters show a reason (spans from other instrumentation set aside, spans rejected, nothing at all
  after 2 minutes). When the first span is stored the page shows that message, a link to its timeline and the
  Messages list, without a reload. Messages, Duplicates and Stuck are now drawn by the shared UI in the same dark
  design as the timeline and Tenants, and on an empty store they say no service has sent spans yet instead of
  "No stuck messages." The Tenants page cites the span contract's section 15.2, with a link. Without the UI
  build, the HTML pages answer as before, with the same empty-store wording.

- **Port already in use: one line, not a traceback.** When the listen port is taken (4318 is the standard
  OTLP/HTTP port, so an OpenTelemetry Collector, Jaeger or another OTLP receiver often holds it), `trailbeam`
  and `trailbeam start` print `trailbeam: port 4318 on 127.0.0.1 is already in use (another OTLP receiver?).
  Stop it or run trailbeam --port 4319 and set TRAILBEAM_OTLP_TRACES_ENDPOINT to match.` and exit with status 2.
  A port it may not use, or a `--host` that isn't an address of the machine, is also one line.
  `npx trailbeam start` checks the port before it runs Docker and prints the same line (with
  `npx trailbeam start --port 4319`) instead of Docker's "port is already allocated".

- **Node.js: a short script no longer loses its spans.** When the event loop empties, the package now
  exports the spans still queued, so a script that sends a message and ends without
  `await trailbeam.uninstall()` delivers them, as the Python package already did. `process.exit()` and
  signals skip that step; spans still queued then are lost, and the package now prints one line to stderr
  with the count and the fix (`await trailbeam.uninstall()` before `process.exit()`)
  ([Node.js README](node/README.md#client-hooks)). (QV-156)

- **Node.js: `trailbeam.uninstall()` no longer fails when the Trailbeam server is unreachable.** With nothing
  listening on the OTLP endpoint, `await trailbeam.uninstall()` (and `trailbeam.forceFlush()`) rejected with
  `ECONNREFUSED`, which ended the process with status 1. Both now resolve; the spans that could not be sent are
  reported only as drops (the `TRAILBEAM_SPANS_DROPPED` warning and `exportStats()`), as in Python.
- **`npx trailbeam start`: the server from npm, without Python.** The npm package has a `trailbeam` command.
  `npx trailbeam start` runs the container image of the same version (`ghcr.io/nick-lennox/trailbeam:X.Y.Z`)
  with its data in the Docker volume `trailbeam-data`, and prints the same first-run output as the Python
  `trailbeam start`: the address, the token and the environment variables for your services. It takes the
  same flags and `TRAILBEAM_` variables; `--host`/`--port` become the published port, and `--data-dir`, `--db`,
  `--config` and `--token-file` name files on your machine (`--store-url` is not available: the image has
  SQLite only). If Docker is missing or not running it says so in one line. `require('trailbeam')` is
  unchanged and there are no new dependencies
  ([Node.js README](node/README.md#run-the-server-npx-trailbeam-start)).
- **`trailbeam start`: first-run setup in one command.** On its first run in a data directory (default
  `~/.trailbeam`, or `--data-dir` / `TRAILBEAM_DATA_DIR`) it creates an access token and a receipt-hash key,
  each in a file only its owner can read, and keeps the SQLite file there. It then starts the server and prints
  the web screen's address, the token, and the environment variables to paste into a Python and a Node.js
  service. Later runs read the same files, so the token and key don't change. Every `trailbeam` flag works
  after `start`; `--db`, `--store-url` and `--token-file` (or `TRAILBEAM_TOKEN`) replace the file in the data
  directory. `trailbeam` with flags, as the container image runs it, is unchanged: it creates no files and
  turns on no token.
- **A team install from one file: `docker-compose.yaml`.** `install -m 600 .env.example .env`, fill in the token, the
  receipt-hash key and the Postgres password, then `docker compose up -d --build --wait`: the Trailbeam server
  and Postgres 16, a named volume for each, health checks, `restart: unless-stopped`, and
  `max_locks_per_transaction = 256` on Postgres. Compose refuses to start without the token. CI starts it from
  `.env.example`, sends a span and checks the data survives `docker compose down && up`
  ([Team install](README.md#team-install-docker-compose-with-postgres)).
- **Postgres: the lookup tables are time-bucketed (schema 5).** `mid_places` and `hash_places` are partitioned
  by day like the spans, so ingest writes only to recent partitions and retention drops them whole (before, it
  deleted their rows in batches). Older days are sealed into a Bloom filter, about 2.5 bytes per key, kept by
  each replica. A lookup reads an older day only for the keys the filter can't rule out: a late settle, a
  message older than three days, or about 1 in 15,000 other keys. Sealing runs at the start of every retention
  pass, also when every queue is kept forever: with Postgres the passes now run then too, deleting nothing
  (they also create the coming days' partitions ahead of ingest, which they didn't before). Joins and summaries are unchanged; late
  settles still join their receive from any day. At 1M spans over 7 days, ingest touches 25 MB of lookup
  indexes instead of 92 MB. Upgrading a 1M-span database took 8 s. The effect at 4M–100M spans hasn't been
  measured.

## 0.3.0: Postgres for teams that outgrow SQLite

The first published release: `pip install trailbeam`, `npm install trailbeam`, and the container image
`ghcr.io/nick-lennox/trailbeam`. 0.2.0 was tagged but never published.

- **The Tenants page reads an hourly rollup per tenant, on SQLite and Postgres.** Before, it added up every
  tenant event in its range on each request, so it got slower as traffic grew (Postgres: p95 1.5 s at 4M spans,
  2.8 s at 7M). Now each store keeps, per hour and tenant, the sends, first deliveries with their times in
  queue, and dead-letter moves, kept up to date as spans arrive and taken out when retention deletes them. The
  page reads the rollup for the whole hours in its range and the raw events for the part-hours at either end.
  At 1M spans on our desktop, idle, p95 for the last day went from 78 ms to 17 ms on SQLite and from 184 ms to
  44 ms on Postgres; for the last 7 days from 553 ms to 72 ms and from 670 ms to 193 ms. Every count is still
  exact. Ingest time per export was the same within the noise of the measurement.
- **Contract change: `p95_in_queue_ms` is within 1% of the exact value** (`contract/tenants.md`). The rollup
  keeps the times in queue as counts per bucket (a sketch, after DDSketch), which can be added up across hours
  but not sorted. Times up to 100 ms come back exact; above that the p95 is at most 0.91% off the nearest-rank
  value. The same spans always give the same p95. `contract/fixtures/tenants-expected.json` keeps the exact
  values and gains `p95_relative_error` (0.01), which a result must be within.
- **Postgres: the connection pools are settings.** `--pg-ingest-connections` (`TRAILBEAM_PG_INGEST_CONNECTIONS`,
  default 4) and `--pg-read-connections` (`TRAILBEAM_PG_READ_CONNECTIONS`, default 8) set how many exports and
  reads one replica runs on Postgres at once. Before, they were fixed at 4 and 8.
- **Postgres: an export no longer locks every day's partitions.** Before, each export locked about 25
  partitions and indexes per day partition of history until it committed, so a store with months of history
  could fill Postgres's lock table ("out of shared memory", `max_locks_per_transaction`). Now an export locks
  only the partitions its own messages are in: 85 lock entries for a typical export, with 10 day partitions or
  400. Reading one message's timeline is bounded the same way. Two small lookup tables make this possible:
  one row per message ID and one per receipt handle. On our test machine, seeding 205,000 spans wrote 29% more
  WAL (852 MB against 658 MB), the database grew 6% (33 MB), and the seed took 48 s instead of 90 s.
- **Postgres: a list page no longer locks every day's partitions.** The message, duplicate and stuck lists,
  tenant search and activity, `/api/stats` and `/api/health` read 8 day partitions per query. Each query runs
  in its own transaction, so it releases its locks before the next one starts. Newest-first lists start with
  the newest day and stop once the page is full. With 400 day partitions, the most lock entries any page held
  at once fell from 3,586 to 124. With 10 it is about the same as before (124 against 103). The answers are
  unchanged. With 400 partitions, the first page of messages takes 1.6 ms instead of 24 ms and tenant search
  151 ms instead of 490 ms. The lists that must read every partition got slower: stuck messages take 62 ms instead of 32 ms,
  and `/api/health` 55 ms instead of 48 ms.
- **Postgres: keeping a message's summary up to date no longer scans the day's partition.** Three summary
  tables get an index on the message ID (schema 3). On a day partition of 500,000 rows, each of those deletes
  took 8–26 ms and now takes 0.1 ms.
- **Postgres: a new day's partition no longer stalls ingest behind a `pg_dump`.** A partition is now created as
  a table of its own and then attached, which needs no lock that an open transaction or an export holds, so a
  `pg_dump` can run at any time of day. Retention creates the partitions for today and the next two days. Before,
  a read transaction open for 25 s made exports wait 24 s. Attaching still waits at most 1 second for a schema
  change running at the same time; an export that needs a partition that can't be created then tries 3 times,
  then is answered `503` with `Retry-After`. Exports that need new partitions at the same moment, on one
  replica or several, take turns, and one whose partitions another has made stops waiting. Before, the one that
  lost the race could fail ("type ... already exists"), and under load one that waited more than 2 s for another
  export of its replica was answered `503`.
- **Postgres: a new day's partition is analyzed once a replica has written 1,000 spans to it.** Until then
  Postgres has no statistics for it and plans it as empty. On a 200,000-row partition, that made the
  receipt-hash update take 55–68 ms instead of 0.6 ms.
- **Postgres: the server says when `max_locks_per_transaction` is too low.** At start and as partitions are
  added, the server works out the value it needs and logs a warning with it. Since list pages read 8 day
  partitions a query, that value no longer grows with the partitions: a stock Postgres is enough for one replica
  with up to about 80 read connections. A request that fails on a full lock table is logged as that, and an
  export gets 503 with `Retry-After`, not "Postgres is unreachable". README: Postgres lock table.

### Upgrading from 0.2.0 (SQLite)

At start the server upgrades the file to schema 12, which builds the hourly tenant rollup from the stored
message summaries, without reading the spans: 7 seconds for a 1M-span file on our test machine, during which
the server doesn't accept spans. Messages moved to a dead-letter queue more than once get their summary
rewritten first (rare). If it is stopped part way, the next start runs it again.

### Upgrading from 0.2.0 (Postgres)

**Stop every 0.2.0 replica, then start the new version.** At start the server upgrades the database to
Postgres schema 4, one day partition at a time. Schema 4 builds the hourly tenant rollup of each partition
from its message summaries: 3.9 seconds for 1M spans on our test machine. A replica of an earlier build left
running would store spans without adding them to the rollup, and the Tenants page would undercount them, so
this applies to builds of `main` at schema 2 or 3 too. Schema 2 fills the lookup tables from the stored spans. On our
test machine that took 1 second for 205,000 spans. Schema 3 builds the message-ID indexes: 0.4 s for 500,000
rows in each of the three tables. Each partition's index build blocks writes to that partition only, and no
replica is writing during the upgrade anyway. Reads (and a `pg_dump`) carry on. A database already at schema 2
(from a build of `main`) gets only the schema-3 step, and the same rule applies: stop every replica first.
Don't run 0.2.0 and this version against one database at the same time. A 0.2.0 replica doesn't write the lookup tables, so the spans it stores are missing from
joins and timelines. Once the database is upgraded, a 0.2.0 server refuses to start on it. If the upgrade
is stopped part way, the database stays at the last schema it finished, and the next start goes on from
there.

## 0.2.0: production single-node

The first tagged release. One Trailbeam server on SQLite, sized and documented for production: retention,
pagination, backpressure, metrics, a shared access token, backups, and a CI check that fails the build on the
kind of slowdown that made inserts 100 to 10,000 times slower before this release. Postgres is an optional,
bring-your-own store, and several replicas can share one Postgres.

Version numbers: the Python package (`trailbeam`, SDK and server) is 0.2.0. The Node package and
`@trailbeam/ui` keep their own versions.

### Server

- **Retention, 7 days by default**, per queue, or `forever`; a background purge that holds the writer at most a
  quarter of the time. New files give deleted space back to the disk. `--vacuum` rewrites an older file
  once so it does too.
- **Pages read per-message summaries**, so a page costs about the same at any size. `/api/messages`,
  `/api/duplicates` and `/api/stuck` page with `limit` and `next_cursor` and take `from_ms`/`to_ms`. Tenant
  search and body search are bounded by a time window (default the last day).
- **Ingest:** the receipt-hash joins are index lookups at any size. Schema 11 cuts the disk writes per span
  by about a third. Group commit across concurrent exports. Reads use their own WAL connections and no
  longer wait for the writer.
- **Backpressure:** ingest answers 429 or 503 with `Retry-After` instead of queueing without limit
  (`--ingest-max-pending`, `--ingest-max-wait`).
- **`GET /metrics`** (Prometheus), **`GET /api/ready`** (fails while the store is unreachable), and a cheap
  `GET /api/health`. Exact counts moved to `GET /api/stats`.
- **One shared access token** (`TRAILBEAM_TOKEN` or `--token-file`), off by default, for ingest, the API and
  the web screen.
- **Configuration file** (`--config trailbeam.toml`) with per-queue settings, checked at start.
- **Message bodies are stored once** per message, not on every span that carries them.
- **`--durability relaxed`**, opt-in: skips the sync per export. An OS crash or power loss can lose the last
  moments of exports answered 200. `full` stays the default.
- **Postgres store** (`--store-url`), partitioned by day and retention class, and **several replicas** on one
  Postgres; retention runs on one replica at a time.
- **Versioned schema:** the server upgrades an older file at start and refuses a newer one.
- `contract/openapi.yaml` describes the API and the ingest endpoint; a test keeps it and the server in step.

### SDK

- Counts and logs dropped spans; the default export queue is 16,384 spans. The Collector docs show a persistent
  queue.

### Docs and tests

- README: [sizing one SQLite server](README.md#sizing-one-sqlite-server) (disk per span, the retention limit,
  the measured range, when to move to Postgres) and [backup and restore](README.md#backup-and-restore-sqlite).
- `python/bench/`: load generator, harness, Postgres smoke run, and `ci_check.py`, which runs in CI on every
  pull request: the join plans must use the hash indexes, with insert, page and ingest limits on a 100k-span
  file. CI also runs it on a deliberately broken plan, which it must catch.
- A store conformance suite from `contract/fixtures`, run against SQLite and Postgres.

### Upgrading from an earlier build

Back up the database file first ([Backup and restore](README.md#backup-and-restore-sqlite)). The upgrade is
one way: an older Trailbeam refuses the upgraded file.

1. **Retention is now on, at 7 days.** The first start deletes every message whose newest span is older
   than that. To keep everything, start with `TRAILBEAM_RETENTION=forever` (or `--retention forever`).
2. **The file is upgraded when the server starts**, up to schema 11. Two steps take time on a large file:
   - *Schema 10* moves message bodies into a table of their own, one copy per body.
   - *Schema 11* rewrites the spans table and its summaries. That took about 5 minutes at 1M spans on our
     test machine (305 s with the disk busy), and we estimate about an hour at 10M. While it runs, the server
     doesn't accept spans. It needs free disk about the size of the file, and afterwards the file is larger by
     about that much (2.1 GB to 3.7 GB at 1M spans); new spans reuse that space. If it's stopped part way,
     the file stays at the version before, and the next start runs it again.
3. **New duplicate rule (SQLite):** a span is skipped as a duplicate only when its trace ID, span ID **and
   end time** match one already stored. Exporter retries resend the span unchanged, so they are still
   skipped. A span sent again with the same IDs but a different end time is now stored.
4. **`GET /api/health` no longer counts the table.** Its `data` is now `{"last_row": n}`. The old
   `data` object (`spans` by kind, `messages`, `unjoined_settles`) is at `GET /api/stats`, unchanged. That
   call reads the whole table, so it takes seconds on a large file. The container's `HEALTHCHECK` only
   checks the status and is not affected.
5. **List endpoints page.** `/api/messages`, `/api/duplicates` and `/api/stuck` return at most `limit`
   messages (default 100, at most 1,000). Follow `next_cursor` for more.
6. **Ingest can answer 429 or 503 with `Retry-After`** when the server is overloaded. OTLP exporters and
   the Collector retry these.
7. **Disk space:** a file created before schema 8 reuses freed space but doesn't shrink. Run
   `trailbeam --db <file> --vacuum` once, with the server stopped, if you want the space back.
