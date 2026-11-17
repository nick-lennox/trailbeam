"""Message contents (contract section 15, revision 3.2): body size, tenant, FIFO fields, opt-in body
capture with redaction, and Lambda error text.

Nothing here makes an SQS call. Everything is read from the parameters and responses of calls the
application made, or from the Lambda event. The Node.js package mirrors this module exactly
(``node/src/capture.js``); both are tested against ``contract/span-contract-vectors.json``.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

# -- statuses (section 15.4) ------------------------------------------------------------------------

STATUS_CAPTURED = "captured"
STATUS_OFF = "off"
STATUS_MD5_MISMATCH = "md5_mismatch"
STATUS_REDACTION_FAILED = "redaction_failed"
STATUS_EXCEEDS_LIMIT = "exceeds_capture_limit"
STATUS_OFFLOADED = "offloaded_to_s3"

REDACTED = "[REDACTED]"
DEFAULT_MAX_BODY_BYTES = 16 * 1024
HARD_MAX_BODY_BYTES = 256 * 1024
MAX_ERROR_MESSAGE_CHARS = 1024
MAX_TENANT_CHARS = 256
MAX_JSON_DEPTH = 100
S3_POINTER_CLASS = "software.amazon.payloadoffloading.PayloadS3Pointer"
# Message attributes in which the SQS Extended Client puts the offloaded payload's size, current name first
# (section 15.2). The receive hooks ask for both.
OFFLOADED_SIZE_ATTRIBUTES = ("ExtendedPayloadSize", "SQSLargePayloadSize")
_SIZE_RE = re.compile(r"^[0-9]{1,15}$")

DEFAULT_REDACT_KEYS = (
    "password", "passwd", "secret", "token", "access_token", "refresh_token", "authorization",
    "api_key", "apikey", "ssn", "card_number", "cardnumber", "cvv", "cvc",
)

# Built-in text patterns (section 15.3). Written so Python's ``re`` and JavaScript's RegExp match the
# same strings: ASCII classes only, no \d, \w, \s or \b.
PATTERNS = {
    "aws_access_key": r"(?<![A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?![A-Z0-9])",
    "bearer": r"(?<![A-Za-z])[Bb]earer [A-Za-z0-9._~+/=-]+",
    "email": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}",
    "long_number": r"(?<![0-9])[0-9](?:[ -]?[0-9]){8,}(?![0-9])",
}
DEFAULT_PATTERNS = tuple(PATTERNS)
_KEY_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")

ENV_TENANT_ATTRIBUTE = "TRAILBEAM_TENANT_ATTRIBUTE"
ENV_TENANT_BODY_PATH = "TRAILBEAM_TENANT_BODY_PATH"
ENV_BODY_QUEUES = "TRAILBEAM_CAPTURE_BODY_QUEUES"
ENV_ERROR_QUEUES = "TRAILBEAM_CAPTURE_ERROR_QUEUES"
ENV_REDACT_KEYS = "TRAILBEAM_REDACT_KEYS"
ENV_REDACT_PATTERNS = "TRAILBEAM_REDACT_PATTERNS"
ENV_MAX_BODY_BYTES = "TRAILBEAM_CAPTURE_MAX_BYTES"


def _lower(s: str) -> str:
    return s.translate(_ASCII_LOWER)


def _csv(value: Optional[str]) -> list[str]:
    return [p.strip() for p in (value or "").split(",") if p.strip()]


class RedactionError(Exception):
    pass


# -- config -----------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """What the package reads from message contents. Off unless configured."""

    tenant_attribute: Optional[str] = None
    tenant_body_path: Optional[str] = None
    body_queues: frozenset = frozenset()
    error_queues: frozenset = frozenset()
    redact_keys: tuple = DEFAULT_REDACT_KEYS
    redact_patterns: tuple = DEFAULT_PATTERNS
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES
    _key_value_re: Any = field(default=None, compare=False, repr=False)
    _pattern_res: tuple = field(default=(), compare=False, repr=False)

    def __post_init__(self):
        for k in self.redact_keys:
            if not _KEY_RE.match(k):
                raise ValueError(f"trailbeam: redact key {k!r} may contain only A-Z a-z 0-9 _ . -")
        for p in self.redact_patterns:
            if p not in PATTERNS:
                raise ValueError(f"trailbeam: unknown redact pattern {p!r}; known: {', '.join(PATTERNS)}")
        if not 0 < self.max_body_bytes <= HARD_MAX_BODY_BYTES:
            raise ValueError(f"trailbeam: max_body_bytes must be 1 to {HARD_MAX_BODY_BYTES}")
        names = [k for k in self.redact_keys if "." not in k]
        kv = None
        if names:
            alt = "|".join(re.escape(_lower(k)) for k in names)
            kv = re.compile(r"(?<![A-Za-z0-9_])(" + alt + r")([\"']?[ \t]*[:=][ \t]*)(\"[^\"]*\"|'[^']*'|[^ \t\r\n,;&]+)",
                            re.IGNORECASE | re.ASCII)
        object.__setattr__(self, "_key_value_re", kv)
        object.__setattr__(self, "_pattern_res", tuple(re.compile(PATTERNS[p]) for p in self.redact_patterns))

    def body_on(self, queue_name: str) -> bool:
        return bool(queue_name) and ("*" in self.body_queues or queue_name in self.body_queues)

    def errors_on(self, queue_name: str) -> bool:
        return bool(queue_name) and ("*" in self.error_queues or queue_name in self.error_queues)


def resolve_config(tenant: Optional[Mapping] = None, capture: Optional[Mapping] = None) -> Config:
    """Explicit options, else environment variables (section 15.1). Each option group replaces its
    environment variables as a whole."""
    env = os.environ
    if tenant is None:
        tenant = {"message_attribute": env.get(ENV_TENANT_ATTRIBUTE) or None,
                  "body_path": env.get(ENV_TENANT_BODY_PATH) or None}
    if capture is None:
        capture = {"body_queues": _csv(env.get(ENV_BODY_QUEUES)),
                   "error_queues": _csv(env.get(ENV_ERROR_QUEUES)),
                   "redact_keys": _csv(env.get(ENV_REDACT_KEYS)),
                   "redact_patterns": _csv(env.get(ENV_REDACT_PATTERNS)) if env.get(ENV_REDACT_PATTERNS) is not None else None,
                   "max_body_bytes": int(env[ENV_MAX_BODY_BYTES]) if env.get(ENV_MAX_BODY_BYTES) else None}
    extra_keys = tuple(capture.get("redact_keys") or ())
    patterns = capture.get("redact_patterns")
    return Config(
        tenant_attribute=tenant.get("message_attribute") or None,
        tenant_body_path=tenant.get("body_path") or None,
        body_queues=frozenset(capture.get("body_queues") or ()),
        error_queues=frozenset(capture.get("error_queues") or ()),
        redact_keys=DEFAULT_REDACT_KEYS + tuple(k for k in extra_keys if k not in DEFAULT_REDACT_KEYS),
        redact_patterns=tuple(patterns) if patterns is not None else DEFAULT_PATTERNS,
        max_body_bytes=capture.get("max_body_bytes") or DEFAULT_MAX_BODY_BYTES,
    )


# -- redaction (section 15.3) -----------------------------------------------------------------------

def redact_text(text: str, config: Config) -> tuple[str, int]:
    """Key-value pairs for the configured key names, then the built-in patterns."""
    count = 0
    if config._key_value_re is not None:
        text, n = config._key_value_re.subn(lambda m: m.group(1) + m.group(2) + REDACTED, text)
        count += n
    for rx in config._pattern_res:
        text, n = rx.subn(REDACTED, text)
        count += n
    return text, count


def _encode_string(s: str) -> str:
    out = json.dumps(s, ensure_ascii=False)
    try:
        out.encode("utf-8")
    except UnicodeEncodeError as e:  # a lone surrogate cannot be exported as UTF-8: fail closed
        raise RedactionError("lone surrogate") from e
    return out


class _Json:
    """A strict JSON scanner that copies the input and rewrites only the values it redacts, so the
    output keeps the input's formatting and number text. Raises ValueError if the input is not JSON."""

    def __init__(self, s: str, config: Config):
        self.s, self.config = s, config
        self.out: list[str] = []
        self.count = 0
        self.names = {_lower(k) for k in config.redact_keys if "." not in k}
        self.paths = {_lower(k) for k in config.redact_keys if "." in k}

    def run(self) -> str:
        i = self._ws(0)
        i = self._value(i, (), 0, emit=True)
        if self._ws(i) != len(self.s):
            raise ValueError("trailing data")
        return "".join(self.out)

    def _ws(self, i: int) -> int:
        s = self.s
        while i < len(s) and s[i] in " \t\n\r":
            self.out.append(s[i])
            i += 1
        return i

    def _skip_ws(self, i: int) -> int:  # whitespace inside a value being replaced: not copied
        while i < len(self.s) and self.s[i] in " \t\n\r":
            i += 1
        return i

    def _value(self, i: int, path: tuple, depth: int, emit: bool) -> int:
        if depth > MAX_JSON_DEPTH:
            raise RedactionError("JSON nested too deeply")
        s = self.s
        if i >= len(s):
            raise ValueError("unexpected end")
        c = s[i]
        if c == "{":
            return self._object(i, path, depth, emit)
        if c == "[":
            return self._array(i, path, depth, emit)
        if c == '"':
            j = self._string_end(i)
            if emit:
                self.out.append(self._redact_string(s[i:j]))
            return j
        m = _SCALAR.match(s, i)
        if not m:
            raise ValueError("bad value")
        if emit:
            self.out.append(m.group(0))
        return m.end()

    def _object(self, i: int, path: tuple, depth: int, emit: bool) -> int:
        s = self.s
        put = self.out.append if emit else (lambda _x: None)
        put("{")
        i += 1
        i = self._ws_e(i, emit)
        if i < len(s) and s[i] == "}":
            put("}")
            return i + 1
        while True:
            if i >= len(s) or s[i] != '"':
                raise ValueError("expected key")
            j = self._string_end(i)
            key = json.loads(s[i:j])
            put(s[i:j])
            i = self._ws_e(j, emit)
            if i >= len(s) or s[i] != ":":
                raise ValueError("expected :")
            put(":")
            i = self._ws_e(i + 1, emit)
            kpath = path + (_lower(key),)
            if emit and (kpath[-1] in self.names or ".".join(kpath) in self.paths):
                self.out.append('"' + REDACTED + '"')
                self.count += 1
                i = self._value(i, kpath, depth + 1, emit=False)
            else:
                i = self._value(i, kpath, depth + 1, emit)
            i = self._ws_e(i, emit)
            if i < len(s) and s[i] == ",":
                put(",")
                i = self._ws_e(i + 1, emit)
                continue
            if i < len(s) and s[i] == "}":
                put("}")
                return i + 1
            raise ValueError("expected , or }")

    def _array(self, i: int, path: tuple, depth: int, emit: bool) -> int:
        s = self.s
        put = self.out.append if emit else (lambda _x: None)
        put("[")
        i = self._ws_e(i + 1, emit)
        if i < len(s) and s[i] == "]":
            put("]")
            return i + 1
        while True:
            i = self._value(i, path, depth + 1, emit)
            i = self._ws_e(i, emit)
            if i < len(s) and s[i] == ",":
                put(",")
                i = self._ws_e(i + 1, emit)
                continue
            if i < len(s) and s[i] == "]":
                put("]")
                return i + 1
            raise ValueError("expected , or ]")

    def _ws_e(self, i: int, emit: bool) -> int:
        return self._ws(i) if emit else self._skip_ws(i)

    def _string_end(self, i: int) -> int:
        m = _STRING.match(self.s, i)
        if not m:
            raise ValueError("bad string")
        return m.end()

    def _redact_string(self, token: str) -> str:
        value = json.loads(token)
        stripped = value.lstrip(" \t\n\r")
        if stripped[:1] in ("{", "["):
            # A JSON document inside a string (an SNS envelope's Message, for one): redact it as JSON.
            try:
                inner = _Json(value, self.config)
                new = inner.run()
            except ValueError:
                pass
            else:
                if inner.count:
                    self.count += inner.count
                    return _encode_string(new)
                return token
        new, n = redact_text(value, self.config)
        if not n:
            return token
        self.count += n
        return _encode_string(new)


_STRING = re.compile(r'"(?:[^"\\\x00-\x1f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"')
_SCALAR = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null")


def redact_body(body: str, config: Config) -> tuple[str, int, str]:
    """(redacted body, number of values replaced, "json" or "text").

    An Extended Client pointer of exactly the shape the clients write holds no payload, only a bucket and a
    key, so it is not redacted (section 15.3): the patterns would rewrite uuid keys and account IDs in bucket
    names, and the pointer would then name an object that does not exist. It is written back in one compact
    form, so nothing in the input beyond the bucket and the key (a duplicate member, say) is exported."""
    pointer = s3_pointer(body)
    if pointer is not None:
        return ('["' + S3_POINTER_CLASS + '",{"s3BucketName":' + _encode_string(pointer[0])
                + ',"s3Key":' + _encode_string(pointer[1]) + "}]"), 0, "json"
    try:
        scanner = _Json(body, config)
        return scanner.run(), scanner.count, "json"
    except RecursionError as e:  # pragma: no cover - MAX_JSON_DEPTH stops this first
        raise RedactionError(str(e)) from e
    except RedactionError:
        raise
    except ValueError:
        new, n = redact_text(body, config)
        return new, n, "text"


# -- reading the body -------------------------------------------------------------------------------

def body_size(body: Any) -> Optional[int]:
    return len(body.encode("utf-8", "surrogatepass")) if isinstance(body, str) else None


def _parse(body: str):
    try:
        return json.loads(body)
    except (ValueError, RecursionError):
        return None


def _is_pointer_doc(doc) -> bool:
    return isinstance(doc, list) and len(doc) == 2 and doc[0] == S3_POINTER_CLASS


def s3_pointer(body: Any) -> Optional[tuple[str, str]]:
    """(bucket, key) if the body is exactly an Extended Client pointer: the two-item array whose second item
    is an object with only the string members ``s3BucketName`` and ``s3Key``. Otherwise None."""
    if not isinstance(body, str) or S3_POINTER_CLASS not in body:
        return None
    doc = _parse(body)
    if not _is_pointer_doc(doc) or not isinstance(doc[1], dict) or set(doc[1]) != {"s3BucketName", "s3Key"}:
        return None
    bucket, key = doc[1]["s3BucketName"], doc[1]["s3Key"]
    return (bucket, key) if isinstance(bucket, str) and isinstance(key, str) else None


def offloaded_size(attributes: Any, lambda_record: bool = False) -> Optional[int]:
    """The offloaded payload's size from the Extended Client's message attribute, if it is a whole number."""
    if not isinstance(attributes, Mapping):
        return None
    for name in OFFLOADED_SIZE_ATTRIBUTES:
        a = attributes.get(name)
        v = a.get("stringValue" if lambda_record else "StringValue") if isinstance(a, Mapping) else None
        if isinstance(v, str) and _SIZE_RE.match(v):
            return int(v)
    return None


def _envelope(doc) -> Optional[str]:
    if isinstance(doc, dict):
        if doc.get("Type") == "Notification" and "TopicArn" in doc:
            return "sns"
        if "detail-type" in doc and "detail" in doc and "source" in doc:
            return "eventbridge"
    return None


def _md5_matches(body: str, md5: Any) -> bool:
    if not isinstance(md5, str) or not md5:
        return True  # nothing to check against
    return hashlib.md5(body.encode("utf-8", "surrogatepass")).hexdigest() == md5.lower()


def tenant_from(config: Config, attributes: Any, body: Any, lambda_record: bool = False) -> Optional[tuple[str, str]]:
    """(tenant, source) from the configured message attribute, else the configured body path."""
    if config.tenant_attribute and isinstance(attributes, Mapping):
        a = attributes.get(config.tenant_attribute)
        if isinstance(a, Mapping):
            v = a.get("stringValue" if lambda_record else "StringValue")
            if isinstance(v, str) and v and len(v) <= MAX_TENANT_CHARS:
                return v, "message_attribute"
    if config.tenant_body_path and isinstance(body, str):
        doc = _parse(body)
        for part in config.tenant_body_path.split("."):
            doc = doc.get(part) if isinstance(doc, dict) else None
        if isinstance(doc, bool) or doc is None:
            return None
        if isinstance(doc, (int, float)):
            doc = json.dumps(doc)
        if isinstance(doc, str) and doc and len(doc) <= MAX_TENANT_CHARS:
            return doc, "body_path"
    return None


def content_attrs(config: Config, queue_name: str, body: Any, attributes: Any, md5: Any,
                  lambda_record: bool = False) -> dict:
    """Section 15 attributes for one message on a send or receive span."""
    out: dict = {}
    size = body_size(body)
    if size is not None:
        out["messaging.message.body.size"] = size
    t = tenant_from(config, attributes, body, lambda_record)
    if t:
        out["trailbeam.tenant"], out["trailbeam.tenant.source"] = t
    offloaded_bytes = offloaded_size(attributes, lambda_record)
    if offloaded_bytes is not None and isinstance(body, str) and _is_pointer_doc(_parse(body)):
        out["trailbeam.body.offloaded_size"] = offloaded_bytes
    if not isinstance(body, str):
        return out
    if not config.body_on(queue_name):
        out["trailbeam.body.capture_status"] = STATUS_OFF
        return out
    if not _md5_matches(body, md5):
        out["trailbeam.body.capture_status"] = STATUS_MD5_MISMATCH
        return out
    try:
        redacted, count, fmt = redact_body(body, config)
        redacted.encode("utf-8")
    except Exception:
        out["trailbeam.body.capture_status"] = STATUS_REDACTION_FAILED
        return out
    if len(redacted.encode("utf-8")) > config.max_body_bytes:
        out["trailbeam.body.capture_status"] = STATUS_EXCEEDS_LIMIT
        return out
    doc = _parse(body) if fmt == "json" else None
    out["trailbeam.body.capture_status"] = STATUS_OFFLOADED if _is_pointer_doc(doc) else STATUS_CAPTURED
    out["trailbeam.body"] = redacted
    out["trailbeam.body.format"] = fmt
    out["trailbeam.body.redacted_count"] = count
    env = _envelope(doc)
    if env:
        out["trailbeam.body.envelope"] = env
    return out


def error_message(config: Config, queue_name: str, err: BaseException) -> Optional[str]:
    """The exception's message, redacted and cut to 1,024 characters, if error text is on for the queue."""
    if not config.errors_on(queue_name):
        return None
    try:
        text = str(err)
    except Exception:
        return None
    if not text:
        return None
    try:
        text, _ = redact_text(text, config)
        text = text[:MAX_ERROR_MESSAGE_CHARS]
        text.encode("utf-8")
    except Exception:
        return None  # fail closed: no text rather than unredacted text
    return text


def fifo_attrs(group_id: Any, sequence_number: Any) -> dict:
    out: dict = {}
    if isinstance(group_id, str) and group_id:
        out["trailbeam.message_group_id"] = group_id
    if isinstance(sequence_number, str) and sequence_number:
        out["trailbeam.sequence_number"] = sequence_number
    return out
