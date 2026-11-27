"""EventBridge and S3 notification origins (contract section 23, revision 3.5): the receive attributes that say a
message's body is an EventBridge event or an S3 event notification, and the inferred first step they give.

The attributes are stored with the receive like every other attribute, so no store schema changes. What they show is
worked out when the message is read (timeline.py): the earliest receive carrying them gives an inferred step before it,
unless the message has a send, a joined SNS publish or an SNS envelope (section 23.3).
"""
from __future__ import annotations

import re
from typing import Mapping, Optional

from . import sns as sns_mod

EVENTBRIDGE = "eventbridge"
S3 = "s3"
S3_TEST_EVENT = "s3:TestEvent"

EB_EVENT_ID = "trailbeam.eventbridge.event_id"
EB_SOURCE = "trailbeam.eventbridge.source"
EB_DETAIL_TYPE = "trailbeam.eventbridge.detail_type"
EB_ACCOUNT = "trailbeam.eventbridge.account"
EB_REGION = "trailbeam.eventbridge.region"
EB_TIME_MS = "trailbeam.eventbridge.time_ms"
S3_EVENT_NAME = "trailbeam.s3.event_name"
S3_BUCKET = "trailbeam.s3.bucket"
S3_RECORD_COUNT = "trailbeam.s3.record_count"
S3_REGION = "trailbeam.s3.region"
S3_EVENT_TIME_MS = "trailbeam.s3.event_time_ms"
S3_CONFIGURATION_ID = "trailbeam.s3.configuration_id"
S3_SEQUENCER = "trailbeam.s3.sequencer"
S3_OBJECT_KEY = "trailbeam.s3.object_key"
S3_ETAG = "trailbeam.s3.etag"
S3_VERSION_ID = "trailbeam.s3.version_id"

MAX_TEXT = 256
MAX_KEY = 3072
MAX_S3_ID = 1024
_CONTROL_RE = re.compile("[\u0000-\u001f\u007f-\u009f]")
_ID_RE = re.compile(r"[A-Za-z0-9-]{1,100}")
_ACCOUNT_RE = re.compile(r"[0-9]{12}")
_REGION_RE = re.compile(r"[a-z0-9-]{1,32}")
_SEQUENCER_RE = re.compile(r"[0-9A-Fa-f]{1,64}")


def _text(a: Mapping, name: str, limit: int = MAX_TEXT) -> Optional[str]:
    v = a.get(name)
    return v if isinstance(v, str) and 0 < len(v) <= limit else None


def _match(a: Mapping, name: str, rx) -> Optional[str]:
    v = a.get(name)
    return v if isinstance(v, str) and rx.fullmatch(v) else None


def _int(a: Mapping, name: str) -> Optional[int]:
    v = a.get(name)
    if isinstance(v, str) and v.isdigit():
        v = int(v)
    return v if isinstance(v, int) and not isinstance(v, bool) and v >= 0 else None


def _s3_id(a: Mapping, name: str) -> Optional[str]:
    """An eTag or versionId (sections 23.2, 27.1): 1 to 1,024 characters with no control character."""
    v = _text(a, name, MAX_S3_ID)
    return v if v is not None and not _CONTROL_RE.search(v) else None


def origin(r) -> Optional[dict]:
    """What one receive row's attributes say about where its message came from: a dict with ``kind``
    (EVENTBRIDGE or S3) and the checked fields, or None. The packages' checks (23.1, 23.2), applied again."""
    a = sns_mod.attributes(r)
    eid, src, dt = _match(a, EB_EVENT_ID, _ID_RE), _text(a, EB_SOURCE), _text(a, EB_DETAIL_TYPE)
    if eid and src and dt:
        out = {"kind": EVENTBRIDGE, "event_id": eid, "eventbridge_source": src, "detail_type": dt,
               "account": _match(a, EB_ACCOUNT, _ACCOUNT_RE), "region": _match(a, EB_REGION, _REGION_RE),
               "event_time_ms": _int(a, EB_TIME_MS)}
        return out
    name, bucket = _text(a, S3_EVENT_NAME), _text(a, S3_BUCKET)
    if name and bucket:
        count = _int(a, S3_RECORD_COUNT)
        return {"kind": S3, "s3_event_name": name, "bucket": bucket, "test_event": name == S3_TEST_EVENT,
                "region": _match(a, S3_REGION, _REGION_RE), "event_time_ms": _int(a, S3_EVENT_TIME_MS),
                "configuration_id": _text(a, S3_CONFIGURATION_ID), "sequencer": _match(a, S3_SEQUENCER, _SEQUENCER_RE),
                "object_key": _text(a, S3_OBJECT_KEY, MAX_KEY),
                "etag": _s3_id(a, S3_ETAG), "version_id": _s3_id(a, S3_VERSION_ID),
                "record_count": count if count is not None and count > 1 else None}
    return None


def arrived_from(receives: list) -> Optional[tuple[Mapping, dict]]:
    """(the earliest receive whose body is an EventBridge event or S3 notification, what it says), or None."""
    for r in sorted(receives, key=lambda r: (r["time_ns"], r["row"])):
        o = origin(r)
        if o is not None:
            return r, o
    return None
