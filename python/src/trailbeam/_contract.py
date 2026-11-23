"""Names and values from contract/span-contract.md (schema version 3), and the receipt-handle hash."""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import threading
from typing import Optional

SCHEMA_VERSION = 3
SCOPE_NAME = "trailbeam"

DEFAULT_KEY = "trailbeam-default-receipt-hash-key"
KEY_ENV = "TRAILBEAM_RECEIPT_HASH_KEY"
ENDPOINT_ENV = "TRAILBEAM_OTLP_TRACES_ENDPOINT"
DEFAULT_ENDPOINT = "http://localhost:4318/v1/traces"
MIN_KEY_BYTES = 16

# Span names (section 1). They never contain a variable part.
SPAN_SEND = "trailbeam send"
SPAN_RECEIVE = "trailbeam receive"
SPAN_DELETE = "trailbeam delete"
SPAN_CHANGE_VISIBILITY = "trailbeam change_visibility"
SPAN_OUTCOME = "trailbeam outcome"
SPAN_PURGE = "trailbeam purge"  # revision 3.3, section 18

# (trailbeam.kind, messaging.operation.type) per span name.
KINDS = {
    SPAN_SEND: ("sqs.send", "send"),
    SPAN_RECEIVE: ("sqs.receive", "receive"),
    SPAN_DELETE: ("sqs.settle", "settle"),
    SPAN_CHANGE_VISIBILITY: ("sqs.settle", "settle"),
    SPAN_OUTCOME: ("sqs.outcome", "process"),
    SPAN_PURGE: ("sqs.purge", "purge"),  # a custom operation type: semconv defines none for a purge
}

OUTCOME_SUCCESS = "success"
OUTCOME_FAILED_REPORTED = "failed_reported"
OUTCOME_FAILED_INVALID_RESPONSE = "failed_invalid_response"
OUTCOME_FAILED_EXCEPTION = "failed_exception"

log = logging.getLogger("trailbeam")


def _hex(key: bytes, data: str) -> str:
    return hmac.new(key, data.encode("utf-8"), hashlib.sha256).hexdigest()


class ReceiptHasher:
    """Keyed hash of receipt handles (section 7). Holds the key; never logs or exports it."""

    __slots__ = ("_key", "key_id")

    def __init__(self, key: str):
        self._key = key.encode("utf-8")
        self.key_id = _hex(self._key, "trailbeam key id")[:8]

    def __call__(self, receipt_handle: str) -> str:
        return _hex(self._key, receipt_handle)[:32]

    def __repr__(self) -> str:  # never show the key
        return f"ReceiptHasher(key_id={self.key_id!r})"


_warned: set[str] = set()
_warn_lock = threading.Lock()


def _warn_once(tag: str, msg: str) -> None:
    with _warn_lock:
        if tag in _warned:
            return
        _warned.add(tag)
    log.warning(msg)


def resolve_hasher(key: Optional[str] = None) -> ReceiptHasher:
    """Explicit key, then $TRAILBEAM_RECEIPT_HASH_KEY, then the published default key (with one warning)."""
    if key is None:
        key = os.environ.get(KEY_ENV) or None
    if key is None:
        _warn_once(
            "default-key",
            "trailbeam: no receipt-hash key configured (TRAILBEAM_RECEIPT_HASH_KEY); using the published default key, "
            "which gives no protection. Set the same key on every service that uses these queues.",
        )
        key = DEFAULT_KEY
    elif len(key.encode("utf-8")) < MIN_KEY_BYTES:
        _warn_once("short-key", f"trailbeam: the receipt-hash key is shorter than {MIN_KEY_BYTES} bytes")
    return ReceiptHasher(key)


def queue_name_from_url(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1]


def put(attrs: dict, name: str, value) -> None:
    """Set an attribute unless it has no value (section 3: absent values are omitted)."""
    if value is None or value == "":
        return
    attrs[name] = value


def int_or_none(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
