"""The Postgres schema (PostgresStore, postgres.py): versions, the ordered migrations, and the per-place partitions.

Versioning follows migrations.py (the SQLite schema): an integer version, a ``schema_version`` table with one row
per version the database has been at, every migration after the database's version run in order, each in its
own transaction, and a database at a newer version than ``LATEST`` refused (``SchemaTooNew``). The migrations run
under a session advisory lock, so two servers starting at once against one database don't both migrate.
Adding a migration: append a ``Migration``, update ``CREATE`` to the schema it produces, extend
tests/server/test_postgres.py. Never edit a migration that has been on ``main``.

**Places.** Every table holding spans or what is derived from them is partitioned by ``(rclass, bucket)``,
a *place*: ``rclass`` the retention class (the retention of the queue, in seconds; FOREVER_CLASS for forever;
0 when the store was given no retention), ``bucket`` a span time bucket (``time_ns // G``). One place is one
partition of each of ``spans``, ``messages``, ``message_queues``, ``message_tenants``, ``tenant_events``,
``bodies`` and ``tenant_rollup``, created when a span first needs it (``place_ddl``) and removed whole by retention (postgres.py).
``places`` lists them; ``place_queues`` the queues each holds; ``strays`` the messages with rows in a place
other than their own (postgres.py, "Placement").

**Lookups** (version 2, QV-113). ``mid_places`` and ``hash_places`` are not partitioned: they say which places
may hold rows of a message ID, and spans of a receipt hash. A statement that names its places is pruned when it
is planned, and locks those places' partitions only; one that doesn't (a message ID or a hash across all places)
locks every place's partitions and indexes until its transaction ends (postgres.py, "Locks"). They are written
with the rows, may name a place that no longer holds any (a message moved on, a sweep deleted it), and lose a
place's entries when retention drops it.

**Lookups by place** (version 5, QV-127). ``mid_places`` and ``hash_places`` are partitioned by place too, so
ingest writes only to recent partitions, and retention drops a place's lookups with it. A place more than
``hot_buckets`` old is *sealed*: its lookup keys go into one Bloom filter (``lookup_filters``, its bits in
``lookup_filter``; ``places.sealed_in``: the first filter version holding them), and a lookup row written to it
afterwards also goes to ``mid_places_late`` / ``hash_places_late``, which every lookup reads. A lookup reads
the recent partitions and the late tables, and a sealed partition only for the keys the filter may hold
(postgres.py, "Lookups").

Text keys that pages sort or compare by are ``COLLATE "C"``: byte order, as SQLite compares, so ties and
cursors come out the same on both.

**Tenant rollup** (version 4, QV-128). ``tenant_rollup``: what the place's messages add to the Tenants page per
hour and tenant (rollup.py), one row per writing connection (postgres.py, "Tenant rollup"); and a partial
index on the tenant events of repeat dead-letter moves. Partitioned by place like the rest, so retention drops a
place's rollup with it.

**Deduplicated sends** (version 7, QV-246). No table changes: the tenant events of FIFO messages with more than
one send are worked out again, without the sends SQS deduplicated, and the rollup with them.

**Probes** (version 6, QV-148). ``probes``: the test spans ``trailbeam doctor`` sends, kept apart from the spans
so they never show as messages.

**People and tokens** (version 8, QV-225). ``users``, ``sessions``, ``ingest_tokens``, ``personal_tokens`` and
``setup_codes`` (account_tables.py). Not partitioned and not spans: retention never touches them. A Trailbeam
before this version (0.3.0 and earlier) refuses the database from then on. Version 11 (QV-227) adds
``users.proxy_linked_at_ms``: who may sign in through a proxy (proxy_auth.py).
**Purges** (version 9, QV-245). ``purges``: the PurgeQueue calls (contract section 18), not partitioned: they
belong to no message, and every list page reads them by queue.

**Queue events** (version 11, QV-363). ``queue_events``: what each message adds to each queue's page (queues.py),
in the message's place like its tenant events, keyed by (queue key, time) so one queue's range is an index range
of each partition; ``messages.qevents_json`` lists a message's. ``queue_rollup``: those events added up per hour,
queue key, kind, service, Lambda flag and peer, for the flows, one row per writing connection as ``tenant_rollup``.
``place_queue_keys``: the queue keys each place has events of (not partitioned, so the list of queues is read
without touching every place).

**SNS fan-out** (version 13, QV-313). ``sns_publishes`` and ``sns_receives``: the SNS publishes (contract section
20) and, by key, the receives they reached; not partitioned, like ``purges``.

**Single sign-on** (version 14, QV-228). ``oidc_logins`` and ``oidc_identities`` (account_tables.py): OpenID
Connect sign-ins in progress, and which person each identity provider account is. Not spans either.

**Steps** (version 16, QV-468). ``/api/events`` (events.py) reads a page of spans newest first and counts them per
time bucket. The spans' unique key becomes ``spans_identity`` (rclass, bucket, time_ns, trace_id, span_id), as
SQLite's has been since its schema 11: still the exporter-retry check (a retry has the span's end time), and now
also a page's order within a place, so a page is a backward range of one index per place instead of a sort of
every span in the window. It replaces the (rclass, bucket, trace_id, span_id) key, so ingest keeps as many indexes
as before, and its new entries go to the right edge of the B-tree. A span sent again with another end time is now
stored twice, as SQLite stores it. ``step_rollup``: the steps per hour, queue, service and kind, one row per
writing connection as ``tenant_rollup``, in the spans' place (a span's own, not its message's), so retention drops
a place's steps with it. The migration builds each place's index and rollup from one scan of its spans.

**Message-ID indexes** (version 3, QV-118). ``message_queues``, ``message_tenants`` and ``tenant_events`` are
indexed by ``mid``, so rewriting a message's summary deletes its old list rows by index instead of scanning its
place's partition.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from . import __version__
from . import account_tables, alerts_store, dashboards_store
from .migrations import SchemaError, SchemaTooNew

# Partitioned tables: one partition of each per place.
PARTITIONED = ("spans", "messages", "message_queues", "message_tenants", "tenant_events", "bodies", "tenant_rollup",
               "mid_places", "hash_places", "queue_events", "queue_rollup", "step_rollup")
LOOKUPS = ("mid_places", "hash_places")  # partitioned since version 5

# Version 11 (QV-363): the queue page's events and their hourly rollup (queues.py, postgres.py "Queue events").
QUEUE_CREATE = """
CREATE TABLE IF NOT EXISTS queue_events (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    queue_key TEXT COLLATE "C" NOT NULL, -- account/region/name, else the URL, else the name (queues.key)
    at_ms BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,
    seq BIGINT NOT NULL,                 -- its place in the message's qevents_json
    kind TEXT NOT NULL,                  -- queues.KINDS
    service TEXT,                        -- the span's service.name
    lambda BIGINT NOT NULL,              -- the span came from a Lambda function
    value BIGINT,                        -- receive count, time in queue, time in handler or body size (queues.py)
    peer TEXT COLLATE "C",               -- dlq_receive: the source queue's key
    version TEXT,                        -- the span's service.version (QV-362)
    PRIMARY KEY (rclass, bucket, queue_key, at_ms, mid, seq)
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS queue_events_mid ON queue_events (mid, seq);
CREATE TABLE IF NOT EXISTS queue_rollup (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    hour BIGINT NOT NULL,                -- at_ms // 3,600,000
    queue_key TEXT COLLATE "C" NOT NULL,
    kind TEXT NOT NULL,
    service TEXT NOT NULL,               -- '' for none
    lambda BIGINT NOT NULL,
    peer TEXT COLLATE "C" NOT NULL,      -- '' for none
    writer BIGINT NOT NULL,              -- pg_backend_pid() of the connection that owns the row (as tenant_rollup)
    n BIGINT NOT NULL,                   -- the events; a writer's row can be below 0 (it took out another's)
    vn BIGINT NOT NULL,                  -- of them with a value
    vsum BIGINT NOT NULL,                -- the sum of those values
    UNIQUE (rclass, bucket, hour, queue_key, kind, service, lambda, peer, writer)
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS queue_rollup_dlq ON queue_rollup (queue_key, peer) WHERE kind = 'dlq_receive';
CREATE TABLE IF NOT EXISTS place_queue_keys (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    queue_key TEXT COLLATE "C" NOT NULL, -- a queue key with queue events in the place
    PRIMARY KEY (rclass, bucket, queue_key)
);
"""

# Version 16 (QV-468): the steps /api/events counts, per hour, queue, service and kind (events.py), in the place of
# the spans they count; one row per writing connection, as queue_rollup. Purges and SNS publishes are not steps.
STEP_CREATE = """
CREATE TABLE IF NOT EXISTS step_rollup (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    hour BIGINT NOT NULL,                -- time_ns / 3,600 s
    queue TEXT COLLATE "C" NOT NULL,
    service TEXT NOT NULL,               -- the span's service.name, '' for none
    kind TEXT NOT NULL,                  -- events.KINDS
    writer BIGINT NOT NULL,              -- pg_backend_pid() of the connection that owns the row (as tenant_rollup)
    n BIGINT NOT NULL,                   -- the spans; a writer's row can be below 0 (it took out another's)
    UNIQUE (rclass, bucket, hour, queue, service, kind, writer)
) PARTITION BY RANGE (rclass, bucket);
"""
# The spans' unique key since version 16: the retry check, and a page's order in a place (events.py).
SPANS_IDENTITY = ('CREATE UNIQUE INDEX IF NOT EXISTS spans_identity ON {only}spans '
                  '(rclass, bucket, time_ns, trace_id COLLATE "C", span_id COLLATE "C")')

# Version 5's other tables (QV-127): the late lookup rows and the filter of the sealed places.
# Version 6 (QV-148): the test spans of trailbeam doctor (contract section 17), an ID and when a replica stored it.
# Not partitioned and not spans: no page, count, rollup or retention pass reads it; record_probes keeps at least a day of them.
PROBES = """
CREATE TABLE IF NOT EXISTS probes (id TEXT PRIMARY KEY, received_ns BIGINT NOT NULL);
"""

# Version 9 (QV-245): the PurgeQueue calls (contract section 18), one row a call. A purge belongs to no message and
# is read by queue on every list page, so it is not partitioned: few rows, one index. Retention deletes a purge by its
# own time against its queue's cutoff (postgres.py, purge_step). ``id`` orders purges as ``row`` orders spans.
PURGES = """
CREATE TABLE IF NOT EXISTS purges (
    id BIGSERIAL PRIMARY KEY,
    trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
    start_ns BIGINT, time_ns BIGINT NOT NULL,
    service_name TEXT, source TEXT, operation TEXT,
    queue TEXT NOT NULL, queue_url TEXT,  -- the collation of messages.current_queue, which it is compared with
    result TEXT NOT NULL, error_code TEXT,
    client_id TEXT, pid BIGINT, thread_name TEXT,
    order_scope TEXT, order_issued BIGINT, order_returned BIGINT,
    attributes TEXT NOT NULL,
    UNIQUE (trace_id, span_id, time_ns)
);
CREATE INDEX IF NOT EXISTS purges_queue ON purges (queue, start_ns);
"""

# Version 12 (QV-313): SNS fan-out (contract section 21). ``sns_publishes``: one row per SNS message a Publish or
# PublishBatch sent, with the keys a receive joins it by (``topic_arn`` and ``message_id``, ``publish_id``).
# ``sns_receives``: one row per (key, message) for each receive carrying a key, ``key`` being ``e:<topic ARN>
# <SNS message ID>`` or ``p:<publish ID>``: the receives a publish reached, which the spans' places can't be searched
# for. Neither is partitioned: a publish belongs to no message and its receives to any place. Retention deletes both
# by their own time against their topic's or queue's cutoff (postgres.py, purge_step); a receive's row expires no
# later than its message. ``id`` orders publishes as ``row`` orders spans.
SNS = """
CREATE TABLE IF NOT EXISTS sns_publishes (
    id BIGSERIAL PRIMARY KEY,
    trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
    start_ns BIGINT, time_ns BIGINT NOT NULL,
    service_name TEXT, source TEXT, operation TEXT,
    queue TEXT NOT NULL,                  -- the topic's name
    message_id TEXT COLLATE "C" NOT NULL, -- the SNS message ID
    topic_arn TEXT COLLATE "C" NOT NULL,
    publish_id TEXT COLLATE "C",
    batch_entry_id TEXT,
    client_id TEXT, pid BIGINT, thread_name TEXT,
    order_scope TEXT, order_issued BIGINT, order_returned BIGINT,
    attributes TEXT NOT NULL,
    UNIQUE (trace_id, span_id, time_ns)
);
CREATE INDEX IF NOT EXISTS sns_publishes_message ON sns_publishes (topic_arn, message_id);
CREATE INDEX IF NOT EXISTS sns_publishes_publish_id ON sns_publishes (publish_id) WHERE publish_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS sns_publishes_time ON sns_publishes (time_ns);
CREATE TABLE IF NOT EXISTS sns_receives (
    key TEXT COLLATE "C" NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,
    queue TEXT NOT NULL,
    time_ns BIGINT NOT NULL,              -- the newest receive of the message with the key
    PRIMARY KEY (key, mid)
);
CREATE INDEX IF NOT EXISTS sns_receives_time ON sns_receives (time_ns);
"""

# Version 17 (QV-489): EventBridge PutEvents (contract section 25). ``eventbridge_events``: one row per entry of a
# PutEvents call, accepted or failed, with the event ID a receive joins it by (``message_id``, NULL for a failed
# entry) and the bus's name in ``queue``. Not partitioned, like ``sns_publishes``. The receives an entry reached are
# rows of ``sns_receives`` with the key ``b:<event ID>``, so retention and the lookup are the SNS ones.
EVENTBRIDGE = """
CREATE TABLE IF NOT EXISTS eventbridge_events (
    id BIGSERIAL PRIMARY KEY,
    trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
    start_ns BIGINT, time_ns BIGINT NOT NULL,
    service_name TEXT, source TEXT, operation TEXT,
    queue TEXT NOT NULL,                  -- the event bus's name
    message_id TEXT COLLATE "C",          -- the event ID; NULL for a failed entry
    batch_entry_id TEXT,
    client_id TEXT, pid BIGINT, thread_name TEXT,
    order_scope TEXT, order_issued BIGINT, order_returned BIGINT,
    attributes TEXT NOT NULL,
    result TEXT NOT NULL, error_code TEXT,
    UNIQUE (trace_id, span_id, time_ns)
);
CREATE INDEX IF NOT EXISTS eventbridge_events_message ON eventbridge_events (message_id) WHERE message_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS eventbridge_events_time ON eventbridge_events (time_ns);
"""

# Version 18 (QV-511): S3 uploads (contract section 27). ``s3_uploads``: one row per PutObject,
# CompleteMultipartUpload or CopyObject call, ok or failed, with the bucket in ``queue`` and the object key in
# ``message_id`` (NULL when the package did not record it, or for a failed call). ``s3_receives``: one row per message
# and S3 notification its receives got with the object key (section 23.2: only on queues whose body is captured), the
# key URL-decoded, for the reached list. Neither is partitioned, like ``sns_publishes``.
S3 = """
CREATE TABLE IF NOT EXISTS s3_uploads (
    id BIGSERIAL PRIMARY KEY,
    trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
    start_ns BIGINT, time_ns BIGINT NOT NULL,
    service_name TEXT, source TEXT, operation TEXT,
    queue TEXT COLLATE "C" NOT NULL,      -- the bucket
    message_id TEXT COLLATE "C",          -- the object key; NULL when not recorded
    batch_entry_id TEXT,
    client_id TEXT, pid BIGINT, thread_name TEXT,
    order_scope TEXT, order_issued BIGINT, order_returned BIGINT,
    attributes TEXT NOT NULL,
    result TEXT NOT NULL, error_code TEXT,
    UNIQUE (trace_id, span_id, time_ns)
);
CREATE INDEX IF NOT EXISTS s3_uploads_key ON s3_uploads (queue, message_id, time_ns) WHERE message_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS s3_uploads_time ON s3_uploads (time_ns);
CREATE TABLE IF NOT EXISTS s3_receives (
    bucket TEXT COLLATE "C" NOT NULL,
    object_key TEXT COLLATE "C" NOT NULL, -- URL-decoded
    event_time_ms BIGINT NOT NULL,
    operation TEXT NOT NULL,              -- the operation the event name says (s3.OPERATIONS)
    mid TEXT COLLATE "C" NOT NULL,
    queue TEXT NOT NULL,
    time_ns BIGINT NOT NULL,              -- the newest receive of the message with the notification
    PRIMARY KEY (bucket, object_key, event_time_ms, operation, mid)
);
CREATE INDEX IF NOT EXISTS s3_receives_time ON s3_receives (time_ns);
"""

# Version 19 (QV-562): the eTag and versionId of a receive's S3 notification (section 23.2, revision 3.7), which the
# join compares with the upload's; NULL when the receive did not record them.
S3_IDS = """
ALTER TABLE s3_receives ADD COLUMN IF NOT EXISTS etag TEXT;
ALTER TABLE s3_receives ADD COLUMN IF NOT EXISTS version_id TEXT;
"""

SEALING = """
CREATE TABLE IF NOT EXISTS mid_places_late (       -- lookup rows written to a place once it was sealed
    mid TEXT COLLATE "C" NOT NULL,
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    PRIMARY KEY (mid, rclass, bucket)
);
CREATE INDEX IF NOT EXISTS mid_places_late_place ON mid_places_late (rclass, bucket);
CREATE TABLE IF NOT EXISTS hash_places_late (
    receipt_hmac TEXT NOT NULL,
    key_id TEXT NOT NULL,
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    PRIMARY KEY (receipt_hmac, key_id, rclass, bucket)
);
CREATE INDEX IF NOT EXISTS hash_places_late_place ON hash_places_late (rclass, bucket);
CREATE TABLE IF NOT EXISTS lookup_filters (        -- the Bloom filter of the sealed places' lookup keys
    version BIGINT PRIMARY KEY,
    m BIGINT NOT NULL,                   -- bits
    k INTEGER NOT NULL,                  -- bits set per key
    capacity BIGINT NOT NULL,            -- keys it was sized for
    keys BIGINT NOT NULL,                -- keys added
    made_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS lookup_filter (
    version BIGINT NOT NULL,
    chunk INTEGER NOT NULL,
    bits BYTEA NOT NULL,
    PRIMARY KEY (version, chunk)
);
ALTER TABLE lookup_filter ALTER COLUMN bits SET STORAGE EXTERNAL;
"""

CREATE = """
CREATE TABLE IF NOT EXISTS places (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'live',  -- live | dropping (retention is removing it; ingest writes elsewhere)
    swept TEXT,                          -- JSON [queue, ...]: the expired queues of its last sweep (postgres.py)
    sealed_in BIGINT,                    -- NULL: not sealed; 0: being sealed; v: in lookup filter v and later ones
    lookup_keys BIGINT,                  -- the lookup keys sealing took from it
    PRIMARY KEY (rclass, bucket)
);
CREATE TABLE IF NOT EXISTS place_queues (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    queue TEXT COLLATE "C" NOT NULL,     -- a queue with a span, or a message, in the place
    PRIMARY KEY (rclass, bucket, queue)
);
CREATE TABLE IF NOT EXISTS strays (
    rclass BIGINT NOT NULL,              -- the place the rows are in
    bucket BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,       -- a message placed elsewhere with rows here
    PRIMARY KEY (rclass, bucket, mid)
);
CREATE SEQUENCE IF NOT EXISTS span_rows;
CREATE SEQUENCE IF NOT EXISTS body_ids;
CREATE TABLE IF NOT EXISTS spans (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    "row" BIGINT NOT NULL DEFAULT nextval('span_rows'),
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    time_ns BIGINT NOT NULL,
    start_ns BIGINT,
    service_name TEXT,
    source TEXT, operation TEXT, queue TEXT COLLATE "C" NOT NULL, queue_url TEXT,
    message_id TEXT COLLATE "C",
    receipt_hmac TEXT, key_id TEXT,
    receive_count BIGINT, sent_ts_ms BIGINT, first_receive_ts_ms BIGINT,
    dlq_source_arn TEXT,
    result TEXT, error_code TEXT, visibility_timeout BIGINT,
    outcome TEXT, error_type TEXT, batch_entry_id TEXT,
    client_id TEXT, pid BIGINT, thread_name TEXT,
    faas_name TEXT, faas_invocation_id TEXT,
    order_scope TEXT, order_issued BIGINT, order_returned BIGINT,
    attributes TEXT NOT NULL,
    mid TEXT COLLATE "C",
    mid_source TEXT,
    receive_row BIGINT,
    body_dropped TEXT,
    body_id BIGINT,
    service_version TEXT,                -- the resource's service.version (version 10, QV-362)
    seq BIGINT                           -- arrival on the server's clock, store.next_seq (version 23, QV-593)
) PARTITION BY RANGE (rclass, bucket);
""" + SPANS_IDENTITY.format(only="") + """;
CREATE INDEX IF NOT EXISTS spans_row ON spans ("row");
CREATE INDEX IF NOT EXISTS spans_mid ON spans (mid, time_ns);
CREATE INDEX IF NOT EXISTS spans_hash ON spans (receipt_hmac, key_id);
CREATE INDEX IF NOT EXISTS spans_receive_row ON spans (receive_row) WHERE receive_row IS NOT NULL;
CREATE INDEX IF NOT EXISTS spans_loose ON spans (queue) WHERE mid IS NULL AND receive_row IS NULL;
CREATE INDEX IF NOT EXISTS spans_body ON spans (mid) WHERE body_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS messages (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,
    first_ns BIGINT NOT NULL,
    first_clock TEXT NOT NULL,
    last_ns BIGINT NOT NULL,
    source_queue TEXT NOT NULL,
    current_queue TEXT NOT NULL,
    in_dlq BIGINT NOT NULL,
    moved BIGINT NOT NULL,
    source_receive_count BIGINT NOT NULL,
    deliveries BIGINT NOT NULL,
    successes BIGINT NOT NULL,
    consumers TEXT NOT NULL,
    final_delete BIGINT NOT NULL,
    lambda_ok BIGINT NOT NULL,
    lambda_wait_ns BIGINT,
    tenant TEXT COLLATE "C",
    dup_received BIGINT NOT NULL,
    dup_succeeded BIGINT NOT NULL,
    stuck_first_ns BIGINT,
    stuck_sqs_clock BIGINT NOT NULL,
    stuck_open BIGINT NOT NULL,
    queues_json TEXT NOT NULL,
    tenants_json TEXT NOT NULL,
    events_json TEXT NOT NULL,
    strays_json TEXT NOT NULL DEFAULT '[]',  -- JSON [[rclass, bucket], ...]: other places with rows of it
    qevents_json TEXT NOT NULL DEFAULT '[]', -- JSON [[at_ms, kind, queue_key, service, lambda, value, peer,
                                         -- version], ...]: its queue_events rows, seq = index (version 11)
    UNIQUE (rclass, bucket, mid)
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS messages_mid ON messages (mid);
CREATE INDEX IF NOT EXISTS messages_last ON messages (last_ns DESC, mid);
CREATE INDEX IF NOT EXISTS messages_duplicates ON messages (last_ns DESC, mid) WHERE dup_received <> 0 OR dup_succeeded <> 0;
CREATE INDEX IF NOT EXISTS messages_stuck ON messages (stuck_first_ns, mid)
    WHERE stuck_open <> 0 AND stuck_first_ns IS NOT NULL;
CREATE INDEX IF NOT EXISTS messages_strays ON messages (mid) WHERE strays_json <> '[]';
CREATE TABLE IF NOT EXISTS message_queues (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    queue TEXT COLLATE "C" NOT NULL,
    last_ns BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS message_queues_key ON message_queues (queue, last_ns DESC, mid);
CREATE INDEX IF NOT EXISTS message_queues_mid ON message_queues (mid);
CREATE TABLE IF NOT EXISTS message_tenants (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    tenant TEXT COLLATE "C" NOT NULL,
    last_ns BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,
    tenant_lower TEXT COLLATE "C" NOT NULL,
    named_ns BIGINT NOT NULL
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS message_tenants_key ON message_tenants (tenant, last_ns DESC, mid);
CREATE INDEX IF NOT EXISTS message_tenants_search ON message_tenants (tenant_lower, tenant, named_ns);
CREATE INDEX IF NOT EXISTS message_tenants_mid ON message_tenants (mid);
CREATE TABLE IF NOT EXISTS tenant_events (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    at_ms BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,
    seq BIGINT NOT NULL,
    kind TEXT NOT NULL,
    tenant TEXT COLLATE "C",
    wait_ms BIGINT
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS tenant_events_key ON tenant_events (at_ms, mid, seq);
CREATE INDEX IF NOT EXISTS tenant_events_mid ON tenant_events (mid);
CREATE INDEX IF NOT EXISTS tenant_events_again ON tenant_events (at_ms) WHERE kind = 'dead_lettered_again';
CREATE TABLE IF NOT EXISTS tenant_rollup (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    hour BIGINT NOT NULL,                -- at_ms // 3,600,000 (rollup.py)
    tenant TEXT COLLATE "C" NOT NULL,    -- the messages' tenant, '' for none
    writer BIGINT NOT NULL,              -- pg_backend_pid() of the connection that owns the row (postgres.py)
    sent BIGINT NOT NULL,
    first_deliveries BIGINT NOT NULL,
    dead_letter_moves BIGINT NOT NULL,
    waits JSONB NOT NULL,                -- {bucket: count}: the first deliveries' times in queue (rollup.Sketch)
    UNIQUE (rclass, bucket, hour, tenant, writer)
) PARTITION BY RANGE (rclass, bucket);
CREATE TABLE IF NOT EXISTS bodies (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    id BIGINT NOT NULL DEFAULT nextval('body_ids'),
    mid TEXT COLLATE "C" NOT NULL,
    sha BYTEA NOT NULL,
    body BYTEA NOT NULL                  -- trailbeam.body as the span carried it, UTF-8 (surrogatepass)
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS bodies_mid ON bodies (mid, sha);
CREATE TABLE IF NOT EXISTS mid_places (
    mid TEXT COLLATE "C" NOT NULL,       -- a message with rows (spans, summary rows, bodies) in the place
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    PRIMARY KEY (mid, rclass, bucket)
) PARTITION BY RANGE (rclass, bucket);
CREATE TABLE IF NOT EXISTS hash_places (
    receipt_hmac TEXT NOT NULL,          -- a receive or settle span with this receipt hash is in the place
    key_id TEXT NOT NULL,
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    PRIMARY KEY (receipt_hmac, key_id, rclass, bucket)
) PARTITION BY RANGE (rclass, bucket);
""" + SEALING + PROBES + account_tables.PG_CREATE + account_tables.PG_ADD_PROXY + ";\n" + PURGES + QUEUE_CREATE + SNS + account_tables.PG_OIDC + STEP_CREATE + EVENTBRIDGE + S3 + S3_IDS + alerts_store.PG_CREATE + dashboards_store.PG_CREATE + account_tables.PG_PASSKEYS + """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_ms BIGINT,
    name TEXT NOT NULL,
    trailbeam_version TEXT
);
"""


def statements(script: str) -> list[str]:
    """A script as single statements: ``--`` comments removed, then split at semicolons (no statement here
    has one in a string)."""
    text = "\n".join(line.split("--", 1)[0] for line in script.splitlines())
    return [s.strip() for s in text.split(";") if s.strip()]


@dataclass(frozen=True)
class Migration:
    """``statements`` run in one transaction. ``per_place``, when given, then run once for every place, each place
    in a transaction of its own (``%(rclass)s``, ``%(bucket)s``: the place), so that the migration locks one
    place's partitions at a time; the version is recorded after the last. A ``per_place`` entry is SQL, or a
    function ``f(conn, rclass, bucket)``; a ``statements`` or ``after`` entry is SQL or a function ``f(conn)``.
    ``after`` runs in the transaction that records the version. All must be safe to run again: a migration
    stopped part way is run again from the start."""
    version: int
    name: str
    statements: tuple
    per_place: tuple = ()
    after: tuple = ()


# The summary list tables that version 3 indexes by message ID.
MID_INDEXED = ("message_queues", "message_tenants", "tenant_events")


def _mid_indexes(conn, rclass: int, bucket: int) -> None:
    """Version 3, one place: build each list table's ``(mid)`` index on the place's partition and attach it to
    the parent's (``ON ONLY``, made by the migration's statements). ``CREATE INDEX`` on the parent itself would
    build every partition's in one transaction, locking them all against writes until the last is built; this
    locks one place's at a time (SHARE: reads go on). The parent's index becomes valid when the last partition's
    is attached; a partition created later gets its own as Postgres creates it. A partition being detached by
    retention is skipped: the next pass drops it."""
    for table in MID_INDEXED:
        part = partition_name(table, rclass, bucket)
        attached = conn.execute(
            """SELECT NOT i.inhdetachpending FROM pg_inherits i
               JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
               WHERE ch.relname = %s AND pa.relname = %s
                 AND ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())""",
            (part, table)).fetchone()
        if not attached or not attached[0]:
            continue
        conn.execute(f"CREATE INDEX IF NOT EXISTS {part}_mid_idx ON {part} (mid)")
        conn.execute(f"ALTER INDEX {table}_mid ATTACH PARTITION {part}_mid_idx")  # attached already: nothing happens


def _is_partition(conn, table: str, name: str) -> bool:
    return conn.execute(
        """SELECT 1 FROM pg_inherits i JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
           WHERE ch.relname = %s AND pa.relname = %s
             AND ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())""",
        (name, table)).fetchone() is not None


def _set_lookups_aside(conn) -> None:
    """Version 5, first: the unpartitioned lookup tables of version 2 become ``<table>_v4`` (with their indexes),
    so the partitioned ones can take their names. Done already (a migration run again): nothing."""
    for table in LOOKUPS:
        kind = conn.execute(
            "SELECT relkind FROM pg_class WHERE relname = %s AND relnamespace = current_schema()::regnamespace",
            (table,)).fetchone()
        if kind is None or kind[0] != "r":
            continue
        conn.execute(f"ALTER TABLE {table} RENAME TO {table}_v4")
        conn.execute(f"ALTER INDEX {table}_pkey RENAME TO {table}_v4_pkey")
        conn.execute(f"ALTER INDEX {table}_place RENAME TO {table}_v4_place")


def _lookups_by_place(conn, rclass: int, bucket: int) -> None:
    """Version 5, one place: its partitions of the two lookup tables, made and attached as ``place_ddl`` makes
    them, then filled with its rows of the old tables (found by their ``(rclass, bucket)`` index)."""
    for table, name, create, attach in place_ddl(rclass, bucket, LOOKUPS):
        if not _is_partition(conn, table, name):
            conn.execute(create)
            conn.execute(attach)
    if conn.execute("SELECT to_regclass('mid_places_v4')").fetchone()[0] is None:
        return  # run again after the old tables were dropped: nothing to copy
    conn.execute("""INSERT INTO mid_places (mid, rclass, bucket) SELECT mid, rclass, bucket FROM mid_places_v4
                    WHERE rclass = %s AND bucket = %s ON CONFLICT DO NOTHING""", (rclass, bucket))
    conn.execute("""INSERT INTO hash_places (receipt_hmac, key_id, rclass, bucket)
                    SELECT receipt_hmac, key_id, rclass, bucket FROM hash_places_v4
                    WHERE rclass = %s AND bucket = %s ON CONFLICT DO NOTHING""", (rclass, bucket))


def _rollup_place(conn, rclass: int, bucket: int) -> None:
    """Version 4, one place: its tenant_rollup partition (made and attached as place_ddl does; its unique index is
    built empty), the repeat-move index on its tenant_events partition (as _mid_indexes), the ``dead_lettered_again``
    events of its messages moved more than once (rollup.py: derived from their dead_lettered events, as
    summary.summarize derives them), and its rollup rows from its messages' summaries. Safe to run again: the
    place's rollup is built from nothing each time."""
    from . import rollup  # noqa: PLC0415  (not needed to open a current schema)
    import json  # noqa: PLC0415

    events_part = partition_name("tenant_events", rclass, bucket)
    attached = conn.execute(
        """SELECT NOT i.inhdetachpending FROM pg_inherits i
           JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
           WHERE ch.relname = %s AND pa.relname = 'tenant_events'
             AND ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())""",
        (events_part,)).fetchone()
    if not attached or not attached[0]:
        return  # being dropped by retention: the next pass removes it
    for table, name, create, attach in place_ddl(rclass, bucket):
        if table == "tenant_rollup":
            conn.execute(create)
            if not conn.execute("SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass(%s)", (name,)).fetchone():
                conn.execute(attach)
    if not conn.execute(
            """SELECT 1 FROM pg_inherits i JOIN pg_index x ON x.indexrelid = i.inhrelid
               WHERE i.inhparent = 'tenant_events_again'::regclass AND x.indrelid = to_regclass(%s)""",
            (events_part,)).fetchone():  # a partition made after the parent index has its own already
        conn.execute(f"CREATE INDEX IF NOT EXISTS {events_part}_again_idx ON {events_part} (at_ms) "
                     "WHERE kind = 'dead_lettered_again'")
        conn.execute(f"ALTER INDEX tenant_events_again ATTACH PARTITION {events_part}_again_idx")
    place = {"c": rclass, "b": bucket}
    for mid, tenant, events_json in conn.execute(
            "SELECT mid, tenant, events_json FROM messages WHERE rclass = %(c)s AND bucket = %(b)s AND moved <> 0",
            place).fetchall():
        events = [e for e in json.loads(events_json) if e[1] != rollup.DEAD_LETTERED_AGAIN]
        moves = sorted(e[0] for e in events if e[1] == rollup.DEAD_LETTERED)
        events = sorted(events + [[b, rollup.DEAD_LETTERED_AGAIN, b - a] for a, b in zip(moves, moves[1:])])
        text = json.dumps(events)
        if text == events_json:
            continue
        conn.execute("UPDATE messages SET events_json = %s WHERE rclass = %s AND bucket = %s AND mid = %s",
                     (text, rclass, bucket, mid))
        conn.execute("DELETE FROM tenant_events WHERE rclass = %s AND bucket = %s AND mid = %s", (rclass, bucket, mid))
        conn.cursor().executemany(
            """INSERT INTO tenant_events (rclass, bucket, at_ms, mid, seq, kind, tenant, wait_ms)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            [(rclass, bucket, at, mid, i, kind, tenant, wait) for i, (at, kind, wait) in enumerate(events)])
    conn.execute("DELETE FROM tenant_rollup WHERE rclass = %(c)s AND bucket = %(b)s", place)
    deltas = rollup.Deltas()
    cur = conn.cursor(name=f"rollup_{rclass}_{bucket}".replace("-", "m"))
    cur.execute("SELECT tenant, events_json FROM messages WHERE rclass = %(c)s AND bucket = %(b)s "
                "AND events_json <> '[]'", place)
    for tenant, events_json in cur:
        deltas.add(None, rollup.contributions(tenant, json.loads(events_json)))
    cur.close()
    conn.cursor().executemany(
        """INSERT INTO tenant_rollup (rclass, bucket, hour, tenant, writer, sent, first_deliveries,
           dead_letter_moves, waits) VALUES (%s, %s, %s, %s, 0, %s, %s, %s, %s)""",
        [(rclass, bucket, h, t) + a.row() for (_, h, t), a in deltas.items()])


def _dedup_place(conn, rclass: int, bucket: int) -> None:
    """Version 7 (QV-246), one place: a send SQS deduplicated on a FIFO queue (timeline.deduplicated_sends) is no
    longer a ``sent`` tenant event. Messages here with a FIFO queue and more than one sent event get their events
    worked out again from their send spans (in this place and its strays), their tenant_events rows rewritten and
    the difference taken out of the rollup, as an export's refresh does. Rare: other messages are skipped on their
    summary row. Safe to run again: a message already right is left alone."""
    from . import rollup, timeline  # noqa: PLC0415  (not needed to open a current schema)
    from .postgres import _apply_rollup  # noqa: PLC0415
    from psycopg.rows import dict_row  # noqa: PLC0415
    import json  # noqa: PLC0415

    was = conn.row_factory
    conn.row_factory = dict_row  # as the store's connections: timeline and _apply_rollup read rows by name
    try:
        _dedup_place_rows(conn, rclass, bucket, rollup, timeline, _apply_rollup, json)
    finally:
        conn.row_factory = was


def _dedup_place_rows(conn, rclass, bucket, rollup, timeline, _apply_rollup, json) -> None:
    deltas = rollup.Deltas()
    for m in conn.execute(
            """SELECT mid, tenant, events_json, strays_json FROM messages
               WHERE rclass = %s AND bucket = %s AND queues_json LIKE '%%.fifo%%'""", (rclass, bucket)).fetchall():
        mid, tenant, events_json, strays_json = m["mid"], m["tenant"], m["events_json"], m["strays_json"]
        old = json.loads(events_json)
        if sum(e[1] == rollup.SENT for e in old) < 2:
            continue
        sends = []
        for pc, pb in [(rclass, bucket)] + [tuple(p) for p in json.loads(strays_json or "[]")]:
            sends += conn.execute(
                """SELECT kind, queue, time_ns, "row" FROM spans
                   WHERE rclass = %s AND bucket = %s AND mid = %s AND kind = 'send'""", (pc, pb, mid)).fetchall()
        dup = timeline.deduplicated_sends(sends)
        events = sorted([e for e in old if e[1] != rollup.SENT]
                        + [[r["time_ns"] // 1_000_000, rollup.SENT, None] for r in sends if r["row"] not in dup])
        if events == sorted(old):
            continue
        conn.execute("UPDATE messages SET events_json = %s WHERE rclass = %s AND bucket = %s AND mid = %s",
                     (json.dumps(events), rclass, bucket, mid))
        conn.execute("DELETE FROM tenant_events WHERE rclass = %s AND bucket = %s AND mid = %s", (rclass, bucket, mid))
        conn.cursor().executemany(
            """INSERT INTO tenant_events (rclass, bucket, at_ms, mid, seq, kind, tenant, wait_ms)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            [(rclass, bucket, at, mid, i, kind, tenant, wait) for i, (at, kind, wait) in enumerate(events)])
        deltas.change((rclass, bucket), rollup.contributions(tenant, old),
                      (rclass, bucket), rollup.contributions(tenant, events))
    _apply_rollup(conn, deltas)


def _queue_place(conn, rclass: int, bucket: int) -> None:
    """Version 11, one place: its queue_events and queue_rollup partitions (as _rollup_place makes its rollup's),
    then every message placed here: its queue events from its spans (here and in its strays), as an export's
    refresh works them out, in qevents_json, queue_events and the rollup (writer 0), and its keys in
    place_queue_keys. Safe to run again: the place's rows are built from nothing each time."""
    from . import timeline  # noqa: PLC0415  (not needed to open a current schema)
    from .queues import events as queue_events, rollup_sums  # noqa: PLC0415
    from psycopg.rows import dict_row  # noqa: PLC0415
    import json  # noqa: PLC0415

    attached = conn.execute(
        """SELECT NOT i.inhdetachpending FROM pg_inherits i
           JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
           WHERE ch.relname = %s AND pa.relname = 'messages'
             AND ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())""",
        (partition_name("messages", rclass, bucket),)).fetchone()
    if not attached or not attached[0]:
        return  # being dropped by retention: the next pass removes it
    for table, name, create, attach in place_ddl(rclass, bucket, ("queue_events", "queue_rollup")):
        conn.execute(create)
        if not conn.execute("SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass(%s)", (name,)).fetchone():
            conn.execute(attach)
    place = (rclass, bucket)
    for t in ("queue_events", "queue_rollup", "place_queue_keys"):
        conn.execute(f"DELETE FROM {t} WHERE rclass = %s AND bucket = %s", place)
    was = conn.row_factory
    conn.row_factory = dict_row  # queues.events reads span rows by name
    try:
        msgs = conn.execute("SELECT mid, strays_json FROM messages WHERE rclass = %s AND bucket = %s ORDER BY mid",
                            place).fetchall()
        rollup: list = []
        keys: set = set()
        for k in range(0, len(msgs), 500):
            batch = msgs[k:k + 500]
            spans: dict = {}
            wheres = {place} | {tuple(p) for m in batch for p in json.loads(m["strays_json"] or "[]")}
            for pc, pb in sorted(wheres):
                for r in conn.execute('SELECT * FROM spans WHERE rclass = %s AND bucket = %s AND mid = ANY(%s)',
                                      (pc, pb, [m["mid"] for m in batch])).fetchall():
                    spans.setdefault(r["mid"], []).append(r)
            put, events_of = [], []
            for m in batch:
                rows = sorted(spans.get(m["mid"], []), key=lambda r: (r["time_ns"], r["row"]))
                ev = queue_events(rows, timeline.deduplicated_sends(rows)) if rows else []
                if not ev:
                    continue
                events_of.append((json.dumps(ev), rclass, bucket, m["mid"]))
                put += [(rclass, bucket, e[2], e[0], m["mid"], i, e[1], e[3], e[4], e[5], e[6], e[7])
                        for i, e in enumerate(ev)]
                rollup += ev
                keys.update(e[2] for e in ev)
            cur = conn.cursor()
            cur.executemany("UPDATE messages SET qevents_json = %s WHERE rclass = %s AND bucket = %s AND mid = %s",
                            events_of)
            cur.executemany("""INSERT INTO queue_events (rclass, bucket, queue_key, at_ms, mid, seq, kind, service,
                               lambda, value, peer, version)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", put)
        conn.cursor().executemany(
            """INSERT INTO queue_rollup (rclass, bucket, hour, queue_key, kind, service, lambda, peer, writer, n, vn,
               vsum) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s)""",
            [place + k + tuple(c) for k, c in rollup_sums(rollup).items()])
        conn.cursor().executemany(
            "INSERT INTO place_queue_keys (rclass, bucket, queue_key) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            [place + (k,) for k in sorted(keys)])
    finally:
        conn.row_factory = was


def _steps_place(conn, rclass: int, bucket: int) -> None:
    """Version 16, one place: spans_identity on its spans partition (attached to the parent's, as _mid_indexes),
    and its step_rollup partition (made and attached as place_ddl does) built from its spans, writer 0. Safe to run
    again: the index is made only if missing, the place's rollup from nothing each time."""
    from .events import HOUR_NS, KINDS  # noqa: PLC0415  (not needed to open a current schema)

    part = partition_name("spans", rclass, bucket)
    attached = conn.execute(
        """SELECT NOT i.inhdetachpending FROM pg_inherits i
           JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
           WHERE ch.relname = %s AND pa.relname = 'spans'
             AND ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())""",
        (part,)).fetchone()
    if not attached or not attached[0]:
        return  # being dropped by retention: the next pass removes it
    if not conn.execute(
            """SELECT 1 FROM pg_inherits i JOIN pg_index x ON x.indexrelid = i.inhrelid
               WHERE i.inhparent = 'spans_identity'::regclass AND x.indrelid = to_regclass(%s)""",
            (part,)).fetchone():  # a partition attached after the parent index was made has its own already
        conn.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS {part}_identity_idx ON {part} '
                     '(rclass, bucket, time_ns, trace_id COLLATE "C", span_id COLLATE "C")')
        conn.execute(f"ALTER INDEX spans_identity ATTACH PARTITION {part}_identity_idx")
    for table, name, create, attach in place_ddl(rclass, bucket, ("step_rollup",)):
        conn.execute(create)
        if not conn.execute("SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass(%s)", (name,)).fetchone():
            conn.execute(attach)
    conn.execute("DELETE FROM step_rollup WHERE rclass = %s AND bucket = %s", (rclass, bucket))
    conn.execute(
        """INSERT INTO step_rollup (rclass, bucket, hour, queue, service, kind, writer, n)
           SELECT rclass, bucket, time_ns / %s, queue, COALESCE(service_name, ''), kind, 0, COUNT(*) FROM spans
           WHERE rclass = %s AND bucket = %s AND kind = ANY(%s) GROUP BY 1, 2, 3, 4, 5, 6""",
        (HOUR_NS, rclass, bucket, list(KINDS)))


# Version 1 is the first Postgres schema. Later versions go here, one at a time.
MIGRATIONS: tuple = (
    Migration(2, "lookup tables: the places of a message ID and of a receipt hash (QV-113)", (
        """CREATE TABLE IF NOT EXISTS mid_places (mid TEXT COLLATE "C" NOT NULL, rclass BIGINT NOT NULL,
           bucket BIGINT NOT NULL, PRIMARY KEY (mid, rclass, bucket))""",
        "CREATE INDEX IF NOT EXISTS mid_places_place ON mid_places (rclass, bucket)",
        """CREATE TABLE IF NOT EXISTS hash_places (receipt_hmac TEXT NOT NULL, key_id TEXT NOT NULL,
           rclass BIGINT NOT NULL, bucket BIGINT NOT NULL, PRIMARY KEY (receipt_hmac, key_id, rclass, bucket))""",
        "CREATE INDEX IF NOT EXISTS hash_places_place ON hash_places (rclass, bucket)",
    ), per_place=(
        """INSERT INTO mid_places (mid, rclass, bucket)
           SELECT mid, %(rclass)s, %(bucket)s FROM spans
               WHERE rclass = %(rclass)s AND bucket = %(bucket)s AND mid IS NOT NULL
           UNION SELECT mid, %(rclass)s, %(bucket)s FROM messages WHERE rclass = %(rclass)s AND bucket = %(bucket)s
           UNION SELECT mid, %(rclass)s, %(bucket)s FROM bodies WHERE rclass = %(rclass)s AND bucket = %(bucket)s
           ON CONFLICT DO NOTHING""",
        """INSERT INTO hash_places (receipt_hmac, key_id, rclass, bucket)
           SELECT DISTINCT receipt_hmac, key_id, %(rclass)s, %(bucket)s FROM spans
               WHERE rclass = %(rclass)s AND bucket = %(bucket)s AND receipt_hmac IS NOT NULL AND key_id IS NOT NULL
                 AND kind IN ('receive', 'delete', 'change_visibility')
           ON CONFLICT DO NOTHING""",
    )),
    Migration(3, "message-ID indexes on the summary list tables (QV-118)", tuple(
        f"CREATE INDEX IF NOT EXISTS {t}_mid ON ONLY {t} (mid)" for t in MID_INDEXED
    ), per_place=(_mid_indexes,)),
    Migration(4, "hourly tenant rollup (QV-128)", (
        """CREATE TABLE IF NOT EXISTS tenant_rollup (rclass BIGINT NOT NULL, bucket BIGINT NOT NULL,
           hour BIGINT NOT NULL, tenant TEXT COLLATE "C" NOT NULL, writer BIGINT NOT NULL,
           sent BIGINT NOT NULL, first_deliveries BIGINT NOT NULL, dead_letter_moves BIGINT NOT NULL,
           waits JSONB NOT NULL, UNIQUE (rclass, bucket, hour, tenant, writer)) PARTITION BY RANGE (rclass, bucket)""",
        "CREATE INDEX IF NOT EXISTS tenant_events_again ON ONLY tenant_events (at_ms) WHERE kind = 'dead_lettered_again'",
    ), per_place=(_rollup_place,)),
    Migration(5, "lookup tables partitioned by place, and the filter of the sealed places (QV-127)", (
        _set_lookups_aside,
        """CREATE TABLE IF NOT EXISTS mid_places (mid TEXT COLLATE "C" NOT NULL, rclass BIGINT NOT NULL,
           bucket BIGINT NOT NULL, PRIMARY KEY (mid, rclass, bucket)) PARTITION BY RANGE (rclass, bucket)""",
        """CREATE TABLE IF NOT EXISTS hash_places (receipt_hmac TEXT NOT NULL, key_id TEXT NOT NULL,
           rclass BIGINT NOT NULL, bucket BIGINT NOT NULL, PRIMARY KEY (receipt_hmac, key_id, rclass, bucket))
           PARTITION BY RANGE (rclass, bucket)""",
        "ALTER TABLE places ADD COLUMN IF NOT EXISTS sealed_in BIGINT",
        "ALTER TABLE places ADD COLUMN IF NOT EXISTS lookup_keys BIGINT",
    ) + tuple(statements(SEALING)), per_place=(_lookups_by_place,), after=(
        "DROP TABLE IF EXISTS mid_places_v4, hash_places_v4",
    )),
    Migration(6, "probes: the test spans of trailbeam doctor (QV-148)", tuple(statements(PROBES))),
    Migration(7, "FIFO deduplicated sends out of the tenant events (QV-246)", (), per_place=(_dedup_place,)),
    Migration(8, "users, sessions and tokens (QV-225)", tuple(statements(account_tables.PG_CREATE))),
    Migration(9, "purges: the PurgeQueue calls (QV-245)", tuple(statements(PURGES))),
    # Version 10 (QV-362): the resource's service.version on each span, NULL on rows stored before it. Adding a
    # nullable column with no default to the partitioned parent adds it to every place without rewriting it.
    Migration(10, "spans.service_version (QV-362)", ("ALTER TABLE spans ADD COLUMN IF NOT EXISTS service_version TEXT",)),
    # Version 11 (QV-363): a column with a constant default is added without rewriting a partition; then each
    # place gets its two new partitions and its messages' queue events, worked out from their spans.
    Migration(11, "queue events and their hourly rollup (QV-363)", (
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS qevents_json TEXT NOT NULL DEFAULT '[]'",
    ) + tuple(statements(QUEUE_CREATE)), per_place=(_queue_place,)),
    # Version 12 (QV-227): who may sign in through a proxy (proxy_auth.py); NULL, nobody, for the people before it.
    Migration(12, "users.proxy_linked_at_ms (QV-227)", (account_tables.PG_ADD_PROXY,)),
    Migration(13, "SNS publishes and the receives they reached (QV-313)", tuple(statements(SNS))),
    Migration(14, "OpenID Connect sign-ins and identities (QV-228)", tuple(statements(account_tables.PG_OIDC))),
    # Version 15 (QV-306): RabbitMQ spans (schema version 4). Retention takes every span of no message by its own
    # time and queue: unjoined settle spans as before, and now also the RabbitMQ spans that belong to no message (a
    # cancel, a span without a message ID), of any kind. spans_unjoined, on the settle kinds, becomes spans_loose,
    # on every row with no message. Built on each place's partition, from one scan of it. No database before it holds
    # a RabbitMQ span, and an SQS message's key is its message ID as before (systems.py): no span data changes.
    Migration(15, "RabbitMQ spans: the index of spans of no message (QV-306)", (
        "DROP INDEX IF EXISTS spans_unjoined",
        "CREATE INDEX IF NOT EXISTS spans_loose ON spans (queue) WHERE mid IS NULL AND receive_row IS NULL",
    )),
    # Version 16 (QV-468): the spans' key becomes spans_identity, and step_rollup is built ("Steps" above). The parent
    # index is made ON ONLY the parent, each place's built and attached in that place's transaction, then the old
    # key is dropped, once every place has the new one.
    Migration(16, "the spans' key by time, and the hourly step rollup (QV-468)", (
        SPANS_IDENTITY.format(only="ONLY "),
    ) + tuple(statements(STEP_CREATE)), per_place=(_steps_place,), after=(
        "ALTER TABLE spans DROP CONSTRAINT IF EXISTS spans_rclass_bucket_trace_id_span_id_key",
    )),
    # Version 17 (QV-489): EventBridge PutEvents entries (EVENTBRIDGE above). A new table, empty: no database before
    # it holds a put_event span (a Trailbeam before 3.6 rejects them). Receives already stored keep no ``b:`` key, so
    # an entry recorded after the upgrade reaches only the messages received after it.
    Migration(17, "EventBridge PutEvents entries (QV-489)", tuple(statements(EVENTBRIDGE))),
    # Version 18 (QV-511): S3 uploads and the receives of their notifications (S3 above). New tables, empty: no
    # database before it holds an s3_upload span (a Trailbeam before 3.7 rejects them). Receives already stored get no
    # s3_receives row, so an upload recorded after the upgrade reaches only the messages received after it.
    Migration(18, "S3 uploads (QV-511)", tuple(statements(S3))),
    # Version 19 (QV-562): the eTag and versionId of a receive's S3 notification, which the join compares with the
    # upload's. Rows stored before it have neither: their join is 3.7's, by bucket, key and time.
    Migration(19, "S3 notification eTag and versionId (QV-562)", tuple(statements(S3_IDS))),
    # Version 20 (QV-650): the alert tables (alerts_store.py: rules, channels, state, checks and leases), new and
    # empty. Not partitioned: they hold no spans, and retention never touches them.
    Migration(20, "alert rules, channels, state and checks (QV-650)", tuple(statements(alerts_store.PG_CREATE))),
    # Version 21 (QV-652): the dashboard tables (dashboards_store.py: dashboards and their widgets), new and empty.
    # Not partitioned, for the same reason.
    Migration(21, "dashboards and their widgets (QV-652)", tuple(statements(dashboards_store.PG_CREATE))),
    # Version 22 (QV-665): passkeys and their challenges (account_tables.py), new and empty.
    Migration(22, "passkeys and their challenges (QV-665)", tuple(statements(account_tables.PG_PASSKEYS))),
    # Version 23 (QV-593): when each span reached the server (store.next_seq), for triage's "failures after the
    # resolve". NULL on rows stored before it, which never count as new failures. A nullable column with no default
    # is added to every place without rewriting it, as version 10's.
    Migration(23, "spans.seq: when each span reached the server (QV-593)",
              ("ALTER TABLE spans ADD COLUMN IF NOT EXISTS seq BIGINT",)),
)
LATEST = 23
MIGRATION_LOCK = 0x7472_6169_6c62_6d01  # pg_advisory_lock key: "trailbm" + 1, held while the schema is opened


def open_schema(conn) -> list[int]:
    """Bring the database to ``LATEST``. ``conn``: a psycopg connection in autocommit. Returns the versions
    migrated to (empty for a new or current database). Raises SchemaError, SchemaTooNew."""
    done: list[int] = []
    conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK,))
    try:
        while True:
            with conn.transaction():
                version = current_version(conn)
                if version is None:
                    for sql in statements(CREATE):
                        conn.execute(sql)
                    _record(conn, LATEST, "create")
                    return done
                if version == LATEST:
                    return done
                if version > LATEST:
                    raise SchemaTooNew(
                        f"the database is at Postgres schema version {version}, written by a newer Trailbeam; this "
                        f"one ({__version__}) knows versions up to {LATEST}. Run the newer Trailbeam. Refusing to "
                        "start, so nothing is lost")
                m = MIGRATIONS[version - 1]
                _run(conn, m.statements)
                if not m.per_place:
                    _run(conn, m.after)
                    _record(conn, m.version, m.name)
                    done.append(m.version)
                    continue
            places = conn.execute("SELECT rclass, bucket FROM places ORDER BY bucket, rclass").fetchall()
            for rclass, bucket in places:
                with conn.transaction():
                    for step in m.per_place:
                        if callable(step):
                            step(conn, rclass, bucket)
                        else:
                            conn.execute(step, {"rclass": rclass, "bucket": bucket})
            with conn.transaction():
                if current_version(conn) == m.version - 1:
                    _run(conn, m.after)
                    _record(conn, m.version, m.name)
            done.append(m.version)
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK,))


def _run(conn, steps) -> None:
    for step in steps:
        if callable(step):
            step(conn)
        else:
            conn.execute(step)


def current_version(conn):
    """The schema version; None for a database (schema) with no Trailbeam tables."""
    tables = {r[0] for r in conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()}
    if "schema_version" in tables:
        v = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        if v is None:
            raise SchemaError("the schema_version table is empty")
        return v
    if tables & {"spans", "messages", "places"}:
        raise SchemaError("the database has Trailbeam tables but no schema version")
    return None


def _record(conn, version: int, name: str) -> None:
    conn.execute("INSERT INTO schema_version (version, applied_at_ms, name, trailbeam_version) VALUES (%s, %s, %s, %s)",
                 (version, time.time_ns() // 1_000_000, name, __version__))


def partition_name(table: str, rclass: int, bucket: int) -> str:
    return f"{table}_p{rclass}_{'m' if bucket < 0 else ''}{abs(bucket)}"


def place_ddl(rclass: int, bucket: int, tables=PARTITIONED) -> list[tuple[str, str, str, str]]:
    """The partitions of one place, one per partitioned table: (table, partition, create, attach). Made as a
    table of its own, then attached: ``ATTACH PARTITION`` locks the parent SHARE UPDATE EXCLUSIVE, which
    exports' writes and any reader (a ``pg_dump``) don't conflict with, where ``CREATE TABLE … PARTITION OF``
    needs ACCESS EXCLUSIVE, so it waits for every open transaction and every export queues behind it (QV-118).
    Attaching builds the parent's indexes on it (the same names) and checks its range (it is empty)."""
    out = []
    for t in tables:
        name = partition_name(t, rclass, bucket)
        out.append((t, name,
                    f"CREATE TABLE IF NOT EXISTS {name} (LIKE {t} INCLUDING DEFAULTS INCLUDING CONSTRAINTS "
                    f"INCLUDING STORAGE)",
                    f"ALTER TABLE {t} ATTACH PARTITION {name} "
                    f"FOR VALUES FROM ({int(rclass)}, {int(bucket)}) TO ({int(rclass)}, {int(bucket) + 1})"))
    return out


# ---- the lock table (QV-107) ----------------------------------------------------------------------------------
#
# A statement on a partitioned table whose WHERE can't prune by place (a message ID, a receipt hash, a page
# across all of time) locks every partition and every partition's index until its transaction ends: about one
# lock per table and index of each place, per connection. Those locks live in Postgres's shared lock table,
# sized at server start (src/backend/storage/lmgr/lock.c NLOCKENTS and postinit.c InitializeMaxBackends, REL_14_STABLE):
# max_locks_per_transaction x (max_connections + autovacuum_max_workers + 1 + max_worker_processes
# + max_wal_senders + max_prepared_transactions) locked objects, and twice that (object, backend) pairs. When it
# is full, any statement fails with "out of shared memory". On a stock server (64 x 122 = 7,808 objects) the
# lock count of Trailbeam's own statements grows with the number of places, so the check below says what
# setting a store's places and pools need.

LOCK_SETTINGS = ("max_locks_per_transaction", "max_connections", "autovacuum_max_workers", "max_worker_processes",
                 "max_wal_senders", "max_prepared_transactions")


@dataclass(frozen=True)
class LockNeed:
    """What one replica can lock at once, against what the server has room for. ``settings``: LOCK_SETTINGS,
    as ints; ``relations_per_place``: the partitioned tables and indexes (one partition of each per place);
    ``connections``: this replica's connections that can run a page at once (the read pool); ``page_places``:
    the most places one page statement names (PostgresStore.PAGE_PLACES: since QV-116 a page walks the places
    a batch a statement, each its own transaction, so it locks no more than that many at once)."""
    places: int
    relations_per_place: int
    connections: int
    settings: dict
    page_places: int = 8

    @property
    def backends(self) -> int:
        s = self.settings
        return (s["max_connections"] + s["autovacuum_max_workers"] + 1 + s["max_worker_processes"]
                + s["max_wal_senders"] + s["max_prepared_transactions"])

    @property
    def objects(self) -> int:
        """Relations one page may hold a lock on at once: every table and index of one batch of places."""
        return min(self.places, self.page_places) * self.relations_per_place

    @property
    def holders(self) -> int:
        """(relation, connection) pairs, with every read connection of the replica in a page at once."""
        return self.objects * self.connections

    @property
    def required(self) -> int:
        """The least max_locks_per_transaction with room for both (ignoring Postgres's 10 % margin)."""
        b = self.backends
        return max(-(-self.objects // b), -(-self.holders // (2 * b)), 1)

    @property
    def short(self) -> bool:
        return self.required > self.settings["max_locks_per_transaction"]

    def message(self) -> str:
        have = self.settings["max_locks_per_transaction"]
        return (f"Postgres lock table: this replica's statements can lock up to {self.holders:,} tables and indexes "
                f"at once ({min(self.places, self.page_places)} places a statement x {self.relations_per_place} tables and "
                f"indexes x {self.connections} read connections), but max_locks_per_transaction = {have} leaves room for "
                f"{2 * have * self.backends:,} ({2 * have} x {self.backends} server processes). Under concurrent "
                f"pages, statements (exports too) fail with \"out of shared memory\". Set max_locks_per_transaction to "
                f"at least {self.required} (and restart Postgres); with more replicas on this database, multiply by "
                f"their number (README: Postgres lock table)")


def lock_need(conn, connections: int, page_places: int = 8) -> LockNeed:
    """LockNeed for the current schema's places and the server's settings. ``conn``: any psycopg connection."""
    rows = conn.execute("SELECT name, setting FROM pg_settings WHERE name = ANY(%s)", (list(LOCK_SETTINGS),)).fetchall()
    settings = {(r["name"] if isinstance(r, dict) else r[0]): int(r["setting"] if isinstance(r, dict) else r[1])
                for r in rows}
    places = _one(conn.execute("SELECT COUNT(*) FROM places").fetchone())
    per_place = _one(conn.execute(
        "SELECT COUNT(*) FROM pg_class WHERE relnamespace = current_schema()::regnamespace AND relkind IN ('p', 'I')"
    ).fetchone())
    return LockNeed(places, per_place, connections, settings, page_places)


def _one(row):
    return next(iter(row.values())) if isinstance(row, dict) else row[0]
