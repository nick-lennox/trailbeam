"""S3 uploads (contract section 27, revision 3.7), through the Store interface only: every backend stores s3_upload
spans apart from messages, finds them by bucket, object key and time, lists the receives an upload's notification
reached, and drops them by their own time on a retention pass. What a join means for a timeline is s3.py and
timeline.py, the same for every backend."""
import sys
from pathlib import Path

import pytest

from trailbeam.server import queries
from trailbeam.server.otlp import decode
from trailbeam.server.store import Cutoffs

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))
from spans import SEC, T0, Consumer, to_proto  # noqa: E402

NOW = T0 + 3600 * SEC
LT = 900
FOREVER = 2 ** 63 - 1
MS = 1_000_000
T0_MS = T0 // MS

KEY = "a dir/red flower+(1).jpg"
ENCODED = "a+dir%2Fred+flower%2B%281%29.jpg"  # as S3 writes it in the notification (section 23.2)

U = Consumer("uploader", service="photos-api")
C = Consumer()                                       # queue orders
B = Consumer("thumbs-worker", queue="thumbs")        # queue thumbs: a second notification configuration


# The server stores an object key only for a bucket, and a queue, in its store_bodies (QV-564): the join needs both.
OPTED = frozenset({"photos", "videos", "orders", "thumbs"})


@pytest.fixture
def new_store(new_store):
    """conftest's new_store, opted in on the server for this file's buckets and queues unless told otherwise."""
    def make(store_bodies=OPTED, name="a"):
        return new_store(store_bodies, name)
    return make


def put(s, *spans):
    d = decode(to_proto(list(spans)).SerializeToString(), "application/x-protobuf")
    assert d.rejected == []
    return s.insert(d.records)


def types(t):
    return [e["type"] for e in t["events"]]


def upload(t=T0 + SEC, **kw):
    """An upload from T0 to ``t``; S3 finished it at T0 + 800 ms."""
    kw.setdefault("key", KEY)
    return U.s3_upload(t, start=T0, **kw)


def received(c, mid, t=T0 + 3 * SEC, et=T0_MS + 800, **kw):
    kw.setdefault("key", ENCODED)
    return c.s3_receive(t, mid, f"rh-{mid}", event_time_ms=et, sent_ms=T0_MS + 1100, start=t - SEC, **kw)


def test_an_upload_is_not_a_message(new_store):
    s = new_store()
    put(s, upload(), upload(error="NoSuchBucket", key=None))
    assert s.recent_messages(100) == []
    assert s.counts()["messages"] == 0
    assert s.counts()["spans"] == {"s3_upload": 2}
    assert queries.message(s, KEY, NOW, LT) is None


def test_both_queues_join_the_one_upload(new_store):
    s = new_store()
    put(s, upload(version_id="v1"), received(C, "sqs-a"), received(B, "sqs-b", t=T0 + 4 * SEC))
    a, b = queries.message(s, "sqs-a", NOW, LT), queries.message(s, "sqs-b", NOW, LT)
    # the joined upload replaces the inferred "S3 object created" step (section 23.3)
    assert types(a) == ["s3_upload", "received"] and types(b) == ["s3_upload", "received"]
    ea, eb = a["events"][0], b["events"][0]
    assert ea["span"] == eb["span"]
    assert ea["inferred"] is False and ea["source"] == "s3_upload" and ea["joined_by"] == "bucket_key_time"
    assert (ea["bucket"], ea["object_key"], ea["operation"], ea["s3_event_name"]) == (
        "photos", KEY, "PutObject", "ObjectCreated:Put")
    assert ea["etag"] == '"9dd4e461268c8034f5c8564e155c67a6"' and ea["version_id"] == "v1"
    assert ea["queue"] == "photos" and ea["consumer"]["service_name"] == "photos-api"
    want = [{"queue": "orders", "message_ids": ["sqs-a"]}, {"queue": "thumbs", "message_ids": ["sqs-b"]}]
    assert ea["reached"] == want and eb["reached"] == want and ea["reached_truncated"] is False
    # S3 dwell time: SentTimestamp minus the record's eventTime
    assert ea["s3_delivery_ms"] == 300 and ea["event_time_ms"] == T0_MS + 800
    assert a["first_seen_clock"] == "sqs"  # the upload is the bucket's step: first seen is still SQS's


def test_the_upload_may_arrive_after_the_receive(new_store):
    s = new_store()
    put(s, received(C, "sqs-a"))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["s3_event", "received"]
    put(s, upload())
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["s3_upload", "received"]


@pytest.mark.parametrize("why,up,rec", [
    ("body capture off on the queue: the receive has no key", {}, {"key": None}),
    ("body capture off for the bucket: the upload has no key", {"key": None}, {}),
    ("another key", {"key": "other.jpg"}, {}),
    ("another bucket", {"bucket": "videos"}, {}),
    ("the event of another operation", {"operation": "CopyObject"}, {}),
    ("an event time outside the call", {}, {"et": T0_MS + 1000 + 2001}),
    ("an event time before the call", {}, {"et": T0_MS - 2001}),
    ("a failed upload", {"error": "AccessDenied"}, {}),
    ("no event time", {}, {"et": None}),
])
def test_what_does_not_join(new_store, why, up, rec):
    s = new_store()
    put(s, upload(**up), received(C, "sqs-a", **rec))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["s3_event", "received"], why


ETAG = "9dd4e461268c8034f5c8564e155c67a6"  # the upload's ETag as the event has it: no quotes


def test_another_process_s_write_of_the_key_is_not_joined(new_store):
    # QV-562: a write of the same key the packages did not see (the console, a Lambda without Trailbeam), finished
    # within the recorded call's window. Its event names another version, so it keeps its inferred step, and the
    # upload's reached list has only the recorded write's message.
    s = new_store()
    put(s, upload(version_id="v1"), received(C, "sqs-a", etag=ETAG, version_id="v1"),
        received(C, "sqs-other", t=T0 + 5 * SEC, et=T0_MS + 2_500, etag="0cc175b9c0f1b6a831c399e269772661",
                 version_id="v2"))
    other = queries.message(s, "sqs-other", NOW, LT)
    assert types(other) == ["s3_event", "received"]
    a = queries.message(s, "sqs-a", NOW, LT)["events"][0]
    assert a["type"] == "s3_upload" and a["version_id"] == "v1"
    assert a["reached"] == [{"queue": "orders", "message_ids": ["sqs-a"]}]


@pytest.mark.parametrize("why,rec,joins", [
    ("the same ETag and version ID", {"etag": ETAG, "version_id": "v1"}, True),
    ("the ETag only (an unversioned bucket's event)", {"etag": ETAG}, True),
    ("neither (a receive from a package that does not read them)", {}, True),
    ("another version ID", {"etag": ETAG, "version_id": "v2"}, False),
    ("another ETag, no version ID", {"etag": "0cc175b9c0f1b6a831c399e269772661"}, False),
    ("the ETag with its quotes is still the same", {"etag": f'"{ETAG}"', "version_id": "v1"}, True),
])
def test_the_event_s_etag_and_version_id(new_store, why, rec, joins):
    s = new_store()
    put(s, upload(version_id="v1"), received(C, "sqs-a", **rec))
    t = queries.message(s, "sqs-a", NOW, LT)
    assert types(t) == (["s3_upload", "received"] if joins else ["s3_event", "received"]), why


def test_two_keys_that_redact_alike_join_by_etag(new_store):
    # Different objects whose keys redact to the same text (section 15.3) share a join key; their ETags tell them
    # apart, so each event joins its own upload, and neither reaches the other's message.
    s = new_store()
    other = "0cc175b9c0f1b6a831c399e269772661"
    put(s, upload(etag=f'"{ETAG}"'), U.s3_upload(T0 + 2 * SEC, start=T0 + SEC, key=KEY, etag=f'"{other}"'),
        received(C, "sqs-a", etag=ETAG), received(C, "sqs-b", t=T0 + 4 * SEC, et=T0_MS + 1_800, etag=other))
    a, b = (queries.message(s, m, NOW, LT)["events"][0] for m in ("sqs-a", "sqs-b"))
    assert a["type"] == b["type"] == "s3_upload" and a["span"] != b["span"]
    assert a["etag"] == f'"{ETAG}"' and b["etag"] == f'"{other}"'
    assert a["reached"] == [{"queue": "orders", "message_ids": ["sqs-a"]}]
    assert b["reached"] == [{"queue": "orders", "message_ids": ["sqs-b"]}]


@pytest.mark.parametrize("why,store_bodies", [
    ("no opt-in on the server", frozenset()),
    ("the queue only: the upload's key is not stored", {"orders"}),
    ("the bucket only: the receive's key is not stored", {"photos"}),
])
def test_the_server_gate_keeps_the_object_key_out(new_store, why, store_bodies):
    # The packages capture both sides; the server's TRAILBEAM_STORE_BODIES is the second gate (QV-564).
    s = new_store(store_bodies)
    put(s, upload(), received(C, "sqs-a"))
    t = queries.message(s, "sqs-a", NOW, LT)
    assert types(t) == ["s3_event", "received"], why
    assert s.counts()["spans"] == {"s3_upload": 1, "receive": 1}
    if "photos" not in store_bodies:
        assert s.s3_uploads("photos", KEY, 0, FOREVER) == [], why
    if "orders" not in store_bodies:
        assert t["events"][0].get("object_key") is None, why
        assert s.s3_receivers("photos", KEY, 0, FOREVER) == [], why
    if hasattr(s, "conn"):  # SQLite: the key is in no column or attribute of either row
        for row in s.conn.execute("SELECT queue, message_id, attributes FROM spans"):
            if row[0] not in store_bodies:
                assert row[1] != KEY and KEY not in row[2] and ENCODED not in row[2], (why, tuple(row))


def test_every_queue_opted_in_joins(new_store):
    s = new_store({"*"})
    put(s, upload(), received(C, "sqs-a"))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["s3_upload", "received"]


def test_the_skew_allowance_joins_just_inside_it(new_store):
    s = new_store()
    put(s, upload(), received(C, "sqs-a", et=T0_MS + 1000 + 2000), received(B, "sqs-b", et=T0_MS - 2000))
    assert types(queries.message(s, "sqs-a", NOW, LT))[0] == "s3_upload"
    assert types(queries.message(s, "sqs-b", NOW, LT))[0] == "s3_upload"


def test_two_uploads_of_the_key_within_the_window_join_neither(new_store):
    # The same key written twice a second apart: the notification can't tell which, so it shows the inferred step.
    s = new_store()
    put(s, upload(), U.s3_upload(T0 + 2 * SEC, key=KEY, start=T0 + SEC + 1), received(C, "sqs-a", et=T0_MS + 1500))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["s3_event", "received"]


def test_an_overwrite_later_joins_its_own_upload(new_store):
    s = new_store()
    later = U.s3_upload(T0 + 31 * SEC, key=KEY, etag='"second"', start=T0 + 30 * SEC)
    put(s, upload(), later, received(C, "sqs-a"),
        received(C, "sqs-b", t=T0 + 33 * SEC, et=T0_MS + 30_800))
    a, b = queries.message(s, "sqs-a", NOW, LT)["events"][0], queries.message(s, "sqs-b", NOW, LT)["events"][0]
    assert a["etag"] == '"9dd4e461268c8034f5c8564e155c67a6"' and b["etag"] == '"second"'
    assert a["reached"] == [{"queue": "orders", "message_ids": ["sqs-a"]}]
    assert b["reached"] == [{"queue": "orders", "message_ids": ["sqs-b"]}]


def test_multipart_and_copy_join_their_own_event_names(new_store):
    s = new_store()
    put(s, upload(operation="CompleteMultipartUpload", key="big.mp4"), upload(operation="CopyObject", key="copy.jpg"),
        received(C, "sqs-m", key="big.mp4", event_name="ObjectCreated:CompleteMultipartUpload"),
        received(C, "sqs-c", key="copy.jpg", event_name="ObjectCreated:Copy"),
        received(C, "sqs-p", key="big.mp4", event_name="ObjectCreated:Put"))  # no PutObject of big.mp4
    assert queries.message(s, "sqs-m", NOW, LT)["events"][0]["operation"] == "CompleteMultipartUpload"
    assert queries.message(s, "sqs-c", NOW, LT)["events"][0]["operation"] == "CopyObject"
    assert types(queries.message(s, "sqs-p", NOW, LT)) == ["s3_event", "received"]


def test_a_message_with_a_send_still_joins(new_store):
    # As 25.3: a process re-sent the body (a DLQ redrive); the notification still came from that upload.
    s = new_store()
    put(s, upload(), C.send(T0 + 2 * SEC, "sqs-a", start=T0 + SEC + 1), received(C, "sqs-a", t=T0 + 4 * SEC))
    assert types(queries.message(s, "sqs-a", NOW, LT)) == ["s3_upload", "sent", "received"]


def test_an_exporter_retry_of_an_upload_changes_nothing(new_store):
    s = new_store()
    sp = upload()
    assert put(s, sp) == (1, 0)
    assert put(s, sp) == (0, 1)
    assert s.counts()["spans"] == {"s3_upload": 1}


def test_retention_drops_uploads_by_their_own_time(new_store):
    s = new_store()
    put(s, upload(), upload(error="SlowDown"), received(C, "sqs-a"))
    resume = None
    while True:
        step = s.purge_step(Cutoffs(FOREVER), resume, 500)
        resume = step.resume
        if resume is None:
            break
    assert s.counts()["spans"] == {} and s.counts()["messages"] == 0
    assert s.s3_uploads("photos", KEY, 0, FOREVER) == []
    assert s.s3_receivers("photos", KEY, 0, FOREVER) == []


@pytest.mark.parametrize("change,why", [
    (lambda a: a.pop("messaging.destination.name"), "s3_upload span without queue"),
    (lambda a: a.pop("trailbeam.result"), "s3_upload span without result"),
    (lambda a: a.update({"messaging.destination.name": "arn:aws:s3:us-west-2:1:accesspoint/x"}),
     "s3_upload span without a valid bucket name"),
    (lambda a: a.update({"trailbeam.result": "maybe"}), "trailbeam.result 'maybe' is not ok or failed"),
    (lambda a: a.update({"messaging.operation.name": "GetObject"}), "unknown trailbeam.kind 's3.upload'"),
])
def test_an_s3_upload_span_without_its_fields_is_rejected(change, why):
    sp = upload()
    change(sp.attrs)
    d = decode(to_proto([sp]).SerializeToString(), "application/x-protobuf")
    assert d.records == [] and len(d.rejected) == 1 and d.rejected[0].startswith(why), d.rejected
