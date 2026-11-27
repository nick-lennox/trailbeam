"""S3 uploads (contract section 27, revision 3.7): which recorded S3 uploads a message's receives join.

A ``trailbeam s3_upload`` span (kind ``s3_upload``) is one ``PutObject``, ``CompleteMultipartUpload`` or ``CopyObject``
call, ok or failed. It is stored apart from messages, like an SNS publish (sns.py): one upload's notification can go
to several queues (one notification configuration per queue), each as a different SQS message, so it belongs to none
of them. Its row has the bucket in ``queue``, the object key in ``message_id`` (NULL when the package did not record
it, or for a failed call) and every attribute in ``attributes``; its ``mid`` is always NULL.

**The join** (section 27.3). A receive joins an upload when all of these hold:

- the receive's body is an S3 event notification (section 23.2) with the object key, which a receive records only on
  a queue whose body is captured (the board's QV-314 decision), and the upload has the key too, which the package
  records only when body capture is on for the bucket;
- the bucket is the same, and the key is the same once the receive's is URL-decoded (S3 writes it URL-encoded);
- the event name is the one the operation raises (``PutObject`` -> ``ObjectCreated:Put``, ...);
- the event's ``eventTime`` (when S3 finished the request, S3's clock) is within the call's interval, widened by
  ``SKEW_MS`` on both sides for the distance between the host's clock and S3's;
- the event's ``eTag`` and ``versionId``, which a receive records with the key (QV-562), are the
  upload's ETag (quotes aside) and VersionId, each where both sides have it: another process's write of the same key
  in the window (one the packages did not see) has another version ID, or another ETag unless its bytes are the same;
  and where the upload has an ETag, the event has one (QV-565): an event without one is not S3's;
- and exactly one recorded upload meets all of the above. Two (the same key written twice within a few seconds) join
  neither: the receive keeps its inferred "S3 object created" step rather than show the wrong sender.

Without the key on both sides nothing is joined: the bucket and the event time alone can't tell this upload from
another process's write to the same bucket, and a receive reads the ETag and version ID only where it reads the key
(section 23.2). Such a message keeps its inferred step (section 23.3).
"""
from __future__ import annotations

import re
from typing import Iterable, Mapping, NamedTuple, Optional
from urllib.parse import unquote_plus

from . import origin as origin_mod
from . import sns as sns_mod

KIND = "s3_upload"
# The operation an upload span names, and the event name its notification has (section 27.3).
EVENT_NAMES = {
    "PutObject": "ObjectCreated:Put",
    "CompleteMultipartUpload": "ObjectCreated:CompleteMultipartUpload",
    "CopyObject": "ObjectCreated:Copy",
}
OPERATIONS = {v: k for k, v in EVENT_NAMES.items()}

OBJECT_KEY = "trailbeam.s3.object_key"
ETAG = "trailbeam.s3.etag"
VERSION_ID = "trailbeam.s3.version_id"

BY = "bucket_key_time"  # how a receive joined an upload, in the API (``joined_by``)

# The row's columns: an SNS publish's (sns.COLUMNS; ``batch_entry_id`` always NULL), with the call's result.
COLUMNS = sns_mod.COLUMNS + ("result", "error_code")

# The packages' checks (27.1), applied again: a key the server joins on is one a package could have written.
BUCKET_RE = re.compile(r"[A-Za-z0-9._-]{1,255}")
MAX_KEY_BYTES = 1024

SKEW_MS = 2_000  # how far the uploading host's clock and S3's may be apart [judgment; not measured]
MAX_CALL_MS = 3_600_000  # the longest upload call the store is asked for: a store query's bound, not a rule

MAX_UPLOADS = sns_mod.MAX_PUBLISHES
MAX_REACHED = sns_mod.MAX_REACHED


def valid_bucket(v) -> bool:
    return isinstance(v, str) and BUCKET_RE.fullmatch(v) is not None


def valid_key(v) -> bool:
    return isinstance(v, str) and 0 < len(v.encode("utf-8", "surrogatepass")) <= MAX_KEY_BYTES


class Notification(NamedTuple):
    """What one receive's S3 notification says, as the join needs it."""
    bucket: str
    key: str  # URL-decoded
    event_time_ms: int
    operation: str
    etag: Optional[str] = None  # the event's eTag, as S3 writes it (no quotes)
    version_id: Optional[str] = None


def notification(r) -> Optional[Notification]:
    """The notification one receive row joins uploads by, or None: an ObjectCreated event of one of the three
    operations, with the object key and the event time."""
    o = origin_mod.origin(r)
    if o is None or o["kind"] != origin_mod.S3 or o["test_event"]:
        return None
    op, key, et = OPERATIONS.get(o["s3_event_name"]), o["object_key"], o["event_time_ms"]
    if op is None or not key or et is None:
        return None
    try:
        decoded = unquote_plus(key, errors="strict")
    except UnicodeDecodeError:
        return None
    return Notification(o["bucket"], decoded, et, op, o["etag"], o["version_id"]) if valid_key(decoded) else None


def message_notifications(rows: Iterable) -> list[Notification]:
    """The distinct notifications a message's receives join uploads by, earliest receive first."""
    out: list[Notification] = []
    for r in sorted((r for r in rows if r["kind"] == "receive"), key=lambda r: (r["time_ns"], r["row"])):
        n = notification(r)
        if n is not None and n not in out:
            out.append(n)
    return out


def search_range_ns(times_ms: Iterable[int]) -> tuple[int, int]:
    """The ``time_ns`` (call end) range in which an upload can join a notification at one of ``times_ms``."""
    times = list(times_ms)
    return (min(times) - SKEW_MS) * 1_000_000, (max(times) + SKEW_MS + MAX_CALL_MS) * 1_000_000


def _within(u: Mapping, event_time_ms: int) -> bool:
    start = u["start_ns"] if u["start_ns"] is not None else u["time_ns"]
    return start // 1_000_000 - SKEW_MS <= event_time_ms <= u["time_ns"] // 1_000_000 + SKEW_MS


def _unquoted(etag: str) -> str:
    """An ETag without the double quotes the response has around it and the event does not."""
    return etag[1:-1] if len(etag) >= 2 and etag[0] == etag[-1] == '"' else etag


def _same_write(u: Mapping, n: Notification) -> bool:
    """False when the upload's ETag or VersionId and the event's differ: another write of the key. False too when the
    upload has an ETag and the event has none (QV-565): S3 writes ``eTag`` in every ObjectCreated record, and a
    receive that records the key records it too (23.2), so an event without one is not S3's (a notification made by
    hand and sent to the queue). A missing VersionId does not rule the upload out: an unversioned bucket's event has
    none, and moto's has none even in a versioned bucket (27.4)."""
    a = sns_mod.attributes(u)
    if isinstance(a.get(ETAG), str) and not isinstance(n.etag, str):
        return False
    for theirs, ours, norm in ((n.etag, a.get(ETAG), _unquoted), (n.version_id, a.get(VERSION_ID), str)):
        if isinstance(theirs, str) and isinstance(ours, str) and norm(theirs) != norm(ours):
            return False
    return True


def candidates(n: Notification, uploads: Iterable[Mapping]) -> list[Mapping]:
    """The ok uploads that notification ``n`` could have come from, one per row."""
    seen, out = set(), []
    for u in uploads:
        if (u["row"] not in seen and u["result"] == "ok" and u["queue"] == n.bucket and u["message_id"] == n.key
                and u["operation"] == n.operation and _within(u, n.event_time_ms) and _same_write(u, n)):
            seen.add(u["row"])
            out.append(u)
    return out


def match(n: Notification, uploads: Iterable[Mapping]) -> Optional[Mapping]:
    """The one upload ``n`` joins, or None: no candidate, or more than one (section 27.3)."""
    c = candidates(n, uploads)
    return c[0] if len(c) == 1 else None


def reach_range_ms(u: Mapping) -> tuple[int, int]:
    """The event times of the notifications upload ``u`` can be joined to."""
    start = u["start_ns"] if u["start_ns"] is not None else u["time_ns"]
    return start // 1_000_000 - SKEW_MS, u["time_ns"] // 1_000_000 + SKEW_MS


def delivery_ms(receives: list) -> Optional[int]:
    """S3 delivery time (dwell, 27.3): the first such receive's SentTimestamp minus the record's ``eventTime``, both
    AWS clocks, in ms; None without both. ``receives``: the message's receives."""
    for r in sorted(receives, key=lambda r: (r["receive_count"] or 0, r["time_ns"], r["row"])):
        n = notification(r)
        if n is not None:
            return None if r["sent_ts_ms"] is None else r["sent_ts_ms"] - n.event_time_ms
    return None


def as_row(u: Mapping) -> dict:
    """An upload row in the shape of a span row (store.py), for the timeline's order: kind ``s3_upload``, no message,
    and a ``row`` below every span's, purge's, SNS publish's and PutEvents entry's (-(2^46) - row)."""
    out = sns_mod.as_row(u)
    out.update(kind=KIND, result=u["result"], error_code=u["error_code"])
    out["row"] = -(1 << 46) - int(u["row"])
    return out
