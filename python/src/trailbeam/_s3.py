"""S3 uploads (contract section 27, revision 3.7): one ``trailbeam s3_upload`` span per ``PutObject``,
``CompleteMultipartUpload`` or ``CopyObject`` call, the three S3 calls that raise an ``ObjectCreated`` event notification.

Trailbeam joins the span to the SQS receives of the notification it caused (section 27.3). The hooks read only the
request's ``Bucket`` and ``Key`` and the response's ``ETag`` and ``VersionId``: never the object's contents (``Body``),
its user metadata (``Metadata``), ``CopySource``, tags, checksums, encryption keys, or the response's ``Location``. The
object key is recorded only when body capture is on for the bucket (section 15.1: the bucket's name, or ``*``, in
``capture.body_queues``), and redacted like a body (section 15.3), as the receive records it only on a queue whose body
is captured (section 23.2, the board's QV-314 decision).
"""
from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from . import _capture

SERVICE = "s3"
UPLOAD_OPS = frozenset({"PutObject", "CompleteMultipartUpload", "CopyObject"})

# A bucket name as the request gives it: S3's rules, with the upper case and underscores of old us-east-1 buckets. An
# access point or Object Lambda ARN (or anything else) is not a bucket's name, so the call gets no span (27.1).
_BUCKET_RE = re.compile(r"^[A-Za-z0-9._-]{1,255}\Z")
MAX_ETAG = 1024
MAX_VERSION_ID = 1024


_CONTROL_RE = re.compile("[\u0000-\u001f\u007f-\u009f]")


def _short_text(v: Any, limit: int) -> Optional[str]:
    """Text of 1 to ``limit`` characters (code points) with no control character, else None."""
    return v if isinstance(v, str) and 0 < len(v) <= limit and not _CONTROL_RE.search(v) else None


def upload_attrs(config: _capture.Config, operation: str, request: Any, response: Any,
                 call_error: Optional[str]) -> Optional[dict]:
    """Section 27.1: the attributes of the one trailbeam s3_upload of a call, with ``messaging.destination.name`` (the
    bucket), or None if the call gets no span. ``response`` is ignored when ``call_error`` is set (the call failed)."""
    if operation not in UPLOAD_OPS or not isinstance(request, Mapping):
        return None
    bucket = request.get("Bucket")
    if not isinstance(bucket, str) or not _BUCKET_RE.match(bucket):
        return None
    a: dict = {"messaging.destination.name": bucket}
    if call_error is not None:
        a["trailbeam.result"], a["trailbeam.error_code"] = "failed", call_error
    else:
        a["trailbeam.result"] = "ok"
        r = response if isinstance(response, Mapping) else {}
        # CopyObject returns the new object's ETag inside CopyObjectResult; the other two at the top level.
        inner = r.get("CopyObjectResult") if operation == "CopyObject" else r
        etag = _short_text(inner.get("ETag") if isinstance(inner, Mapping) else None, MAX_ETAG)
        if etag is not None:
            a["trailbeam.s3.etag"] = etag
        version = _short_text(r.get("VersionId"), MAX_VERSION_ID)
        if version is not None:
            a["trailbeam.s3.version_id"] = version
    key = request.get("Key")
    if (config.body_on(bucket) and isinstance(key, str)
            and 0 < _capture._utf8_len(key) <= _capture.MAX_S3_KEY_BYTES):
        a["trailbeam.s3.object_key"] = _capture.redact_text(key, config)[0]
    return a
