"""Why messages didn't finish, grouped by cause: the Needs attention page's groups (QV-367).

A port of the hosted app's ``lib/app/causes.ts`` (design QV-195), kept to the same rules and the same group shape,
so the shared Needs attention view in ``@trailbeam/ui`` reads either. ``ui/test/fixtures/causes-parity.json`` holds
timelines and the groups both must make of them: ``python/tests/server/test_causes.py`` checks this module against
it, and the app checks its own against the same file (it gets it as ``@trailbeam/ui/fixtures/causes-parity.json``).

One classifier, :func:`cause_of`, gives each unfinished message one signature: what went wrong on its latest
delivery that shows a failure. :func:`group` folds those into groups::

    key = v1 | source queue key | consumer | signature      id = the first 12 hex characters of its sha256

The outcome (dead-lettered, stuck, filtered) is not in the key, so one bug leaving some messages retrying and some
in the dead-letter queue is one row. A group's outcome is its worst one.

Self-hosted Trailbeam has spans only. The rules that need the AWS account's records (an event filter's drop from
CloudTrail, a timeout or out-of-memory from the function's log) are kept, so the port stays whole, but no timeline
the server builds has the ``aws_step`` events they read. Nor does the server know FIFO message groups or queues'
visibility timeouts: no message is FIFO-blocked here, and a received, undeleted message's timer is SQS's default,
which the group's ``why`` says.

Nothing is stored: :func:`read_unfinished` works the groups out at read time from the stuck messages.
"""
from __future__ import annotations

import functools
import hashlib
import math
import re
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional
from urllib.parse import unquote, urlsplit

from . import summary, timeline

KEY_VERSION = "v1"
UNFINISHED_LIMIT = 5000
MAX_FINGERPRINTS = 10  # more error texts than this for one (queue, consumer, exception class) merge into one group
DEFAULT_VISIBILITY_S = 30  # SQS's default visibility timeout, for a queue whose own isn't known
EXAMPLES = 50

NAN = float("nan")

BASIS = {
    "filter": "CloudTrail", "fn_timeout": "function logs", "fn_oom": "function logs", "exc": "spans",
    "invalid_batch_response": "spans", "reported_failure": "spans", "no_outcome": "inferred", "delete_failed": "spans",
    "not_deleted": "inferred", "in_flight": "spans", "fifo_blocked": "inferred", "never_received": "inferred",
}

WHY = {
    "no_outcome": "A Lambda receive with no outcome within the Lambda timeout: the invocation most likely timed out or "
                  "ran out of memory.",
    "not_deleted": "The consumer received it and no delete or outcome followed before its visibility timeout ran out, "
                   "so SQS makes it visible again.",
    "fifo_blocked": "SQS delivers a FIFO group's messages in order, and an earlier message of the same group hasn't "
                    "finished.",
    "never_received": "No instrumented consumer reported a receive. A consumer without Trailbeam, or none at all, may "
                      "be reading the queue.",
}

ASSUMED_VISIBILITY = (f"The queue's visibility timeout wasn't read from AWS, so SQS's default of {DEFAULT_VISIBILITY_S} s "
                      "was assumed for when the message becomes visible again.")

EARLIER = {
    "fn_timeout": "timed out", "fn_oom": "ran out of memory", "exc": "raised an exception",
    "invalid_batch_response": "invalid batch response", "reported_failure": "reported failed", "no_outcome": "no outcome",
    "delete_failed": "delete failed",
}

FAILED = {"fn_timeout", "fn_oom", "exc", "invalid_batch_response", "reported_failure", "no_outcome", "delete_failed"}
RANK = ["dead_lettered", "stuck", "filtered"]


def worse(a: str, b: str) -> str:
    return a if RANK.index(a) <= RANK.index(b) else b


def cause_id(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()[:12]


# ---- where a queue lives (the app's lib/app/queue-url.ts) -------------------------------------------------

_REGION = re.compile(r"(?:^|\.)((?:[a-z]{2}(?:-gov|-iso[a-z]?)?)-[a-z]+-\d+)(?=\.|$)", re.ASCII)


def parse_queue_url(url: Optional[str]) -> Optional[dict]:
    """``https://sqs.<region>.amazonaws.com/<account>/<name>`` (and the older, VPC endpoint, China and emulator
    forms) as its name, account and region; None if it isn't a URL with a path."""
    if not url:
        return None
    try:
        u = urlsplit(url)
        host = (u.hostname or "").lower()
    except ValueError:
        return None
    if not u.scheme or not u.netloc:
        return None
    parts = [p for p in u.path.split("/") if p]
    if not parts:
        return None
    before = parts[-2] if len(parts) > 1 else None
    account = before if before and re.fullmatch(r"\d{12}", before, re.ASCII) else None
    m = _REGION.search(host)
    region = m.group(1) if m else ("us-east-1" if host == "queue.amazonaws.com" else None)
    return {"name": unquote(parts[-1]), "account": account, "region": region}


def queue_key(name: str, url: Optional[str]) -> str:
    """The key a queue is grouped under: account, region and name where the URL gives them, else the URL or name."""
    p = parse_queue_url(url)
    if p and p["account"]:
        return f"{p['account']}/{p['region'] or ''}/{name}"
    return url or name


# ---- the error-text fingerprint (design section 3.2) ------------------------------------------------------

_PRINT = [
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I | re.A), "<uuid>"),
    (re.compile(r"\b(?=[0-9a-f]*[a-f])(?=[0-9a-f]*\d)[0-9a-f]{8,}\b", re.I | re.A), "<hex>"),
    (re.compile(r"\b\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?\b", re.A), "<time>"),
    (re.compile(r"\b\d{1,2}:\d{2}:\d{2}(?:\.\d+)?\b", re.A), "<time>"),
    (re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+", re.A), "<email>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", re.A), "<ip>"),
    (re.compile(r"(?<![\w<])[-+]?\d+(?:\.\d+)?(?:ms|s|kb|mb|gb|b|%)?(?![\w>])", re.I | re.A), "<n>"),
]
_SPACE = re.compile(r"\s+")


def fingerprint(text: Optional[str]) -> str:
    """The error text with what varies per event replaced, so the same failure reads the same. Quoted strings stay."""
    if not text:
        return ""
    for rx, to in _PRINT:
        text = rx.sub(to, text)
    return _SPACE.sub(" ", text).strip()[:200]


# ---- the classifier (design section 3) --------------------------------------------------------------------

def _iso_ms(v) -> float:
    if not isinstance(v, str):
        return NAN
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return NAN
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return float(math.floor(dt.timestamp() * 1000 + 1e-6))


def _end_ms(e: Optional[dict]) -> float:
    if not e or not e.get("end_unix_nano"):
        return NAN
    return float(int(str(e["end_unix_nano"])) // 1_000_000)


def _svc(e: Optional[dict]) -> str:
    return str(((e or {}).get("consumer") or {}).get("service_name") or "")


def _ver(e: Optional[dict]) -> Optional[str]:
    return ((e or {}).get("consumer") or {}).get("service_version")


def _finite(x) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


def cause_of(t: dict, now_ms: float, *, visibility_s: Optional[dict] = None, queue_url: Optional[str] = None,
             tenant: Optional[str] = None, group_id: Optional[str] = None) -> dict:
    """The cause of one unfinished message, from its timeline (timeline.build). The latest delivery that shows a
    failure decides; with none, the latest delivery; never received, the send. A FIFO message blocked behind an
    earlier one comes out as never_received here: :func:`group` tells the two apart."""
    evs = t["events"]
    source = str(t["source_queue"])
    base = {
        "mid": str(t["message_id"]), "queue": source, "queueKey": queue_key(source, queue_url), "tenant": tenant,
        "group": group_id, "errorType": None, "errorCode": None, "fn": None, "error": None, "earlier": [],
        "dlqInferred": t.get("dead_letter_queue_move") == "not_seen_on_dlq_yet" and not t.get("in_dead_letter_queue"),
        "deliveries": int(t.get("deliveries") or 0), "version": None,
    }
    outcome = "dead_lettered" if t.get("in_dead_letter_queue") or t.get("dead_letter_queue_move") else "stuck"

    def make(kind: str, sig: str, consumer: str, evidence_ms: float, **extra) -> dict:
        return {**base, "kind": kind, "signature": sig, "consumer": consumer, "outcome": outcome,
                "evidenceMs": evidence_ms, "earlier": [], **extra}

    # 1: removed by an event filter, from CloudTrail
    drop = next((e for e in evs if e.get("type") == "aws_step" and e.get("aws_kind") == "filter_drop"), None)
    if drop:
        fn = str(drop.get("function_name") or "")
        return {**make("filter", f"filter:{fn}", fn, _iso_ms(drop.get("time")), fn=fn), "outcome": "filtered"}

    receives = sorted((e for e in evs if e.get("type") == "received"), key=lambda e: e.get("delivery") or 0)

    def failures(r: dict) -> list:
        """The failure signals of one delivery, most specific first (rules 2 to 8)."""
        d = r.get("delivery")
        on = [e for e in evs if e.get("delivery") == d]
        c, v = _svc(r), _ver(r)
        out = []
        for s in on:
            if s.get("type") == "aws_step" and s.get("aws_kind") in ("timeout", "out_of_memory"):
                fn = str(s["function_name"]) if s.get("function_name") is not None else c
                kind = "fn_timeout" if s["aws_kind"] == "timeout" else "fn_oom"
                out.append(make(kind, f"{kind}:{fn}", fn, _iso_ms(s.get("time")), fn=fn))
        for x in on:
            if x.get("type") != "lambda_outcome" or x.get("outcome") == "success":
                continue
            if x.get("outcome") == "failed_exception":
                typ = str(x["error_type"]) if x.get("error_type") is not None else "Error"
                text = x.get("error_message") if isinstance(x.get("error_message"), str) else None
                out.append(make("exc", f"exc:{typ}:{fingerprint(text)}", c, _end_ms(x), errorType=typ, error=text))
            elif x.get("outcome") == "failed_invalid_response":
                out.append(make("invalid_batch_response", "invalid_batch_response", c, _end_ms(x)))
            else:
                out.append(make("reported_failure", "reported_failure", c, _end_ms(x)))
        if not out:
            for x in on:
                if x.get("type") == "lambda_invocation_ended_without_outcome":
                    out.append(make("no_outcome", "no_outcome", c, _end_ms(r)))
        for x in on:
            if x.get("type") == "deleted" and x.get("result") != "ok":
                code = str(x["error_code"]) if x.get("error_code") is not None else "unknown"
                out.append(make("delete_failed", f"delete_failed:{code}", c, _end_ms(x), errorCode=code))
        for x in out:
            x["version"] = v
        return out

    for i in range(len(receives) - 1, -1, -1):
        f = failures(receives[i])
        if f:
            decided = f[0]
            decided["earlier"] = [g[0]["kind"] for g in (failures(r) for r in receives[:i]) if g]
            return decided

    if receives:
        latest = receives[-1]
        c = _svc(latest)
        v = _ver(latest)
        # 9 and 10: received, not settled. Its timer is the queue's visibility timeout, or the last accepted change
        changes = [e for e in evs if e.get("type") == "visibility_changed" and e.get("delivery") == latest.get("delivery")
                   and e.get("result") == "ok"]
        change = changes[-1] if changes else None
        from_ms = _end_ms(change) if change else _end_ms(latest)
        vts = change.get("visibility_timeout_s") if change else None
        known = vts if isinstance(vts, (int, float)) and not isinstance(vts, bool) else (visibility_s or {}).get(
            str(latest.get("queue")))
        vt = DEFAULT_VISIBILITY_S if known is None else known
        # a Lambda invocation still running is in flight whatever the timer says
        if t.get("state") == "processing":
            return make("in_flight", "in_flight", c, _end_ms(latest), version=v)
        extra = {"version": v, **({"assumedVisibility": True} if known is None else {})}
        if now_ms < from_ms + vt * 1000:
            return make("in_flight", "in_flight", c, _end_ms(latest), **extra)
        return make("not_deleted", "not_deleted", c, _end_ms(latest), **extra)

    # 12: never received (11, FIFO-blocked, is decided in group())
    send = next((e for e in evs if e.get("type") == "sent"), None)
    return make("never_received", "never_received", "", _end_ms(send) if send else _iso_ms(t.get("first_seen")))


# ---- labels (design section 4) ----------------------------------------------------------------------------

def _cut(s: str, n: int) -> str:
    return s[:n - 1] + "…" if len(s) > n else s


def _who(c: str) -> str:
    return c or "An unnamed consumer"


def cause_label(kind: str, consumer: str, fn: Optional[str], error_type: Optional[str], first_error: Optional[str],
                error_code: Optional[str], group_id: Optional[str], merged: int) -> str:
    """The title of a cause, from its kind and what its key holds. Inferred ones say so in the text."""
    f = fn or consumer or "The function"
    if kind == "filter":
        whose = f"{fn}'s" if fn else "a Lambda trigger's"
        return f"Removed by {whose} event filter. The function never ran"
    if kind == "fn_timeout":
        return f"{f} timed out (Lambda's log)"
    if kind == "fn_oom":
        return f"{f} ran out of memory (Lambda's log)"
    if kind == "exc":
        if merged:
            return f"{_who(consumer)} raised {error_type} ({merged} different messages)"
        if first_error:
            return f"{_who(consumer)} raised {error_type}: {_cut(first_error, 120)}"
        return f"{_who(consumer)} raised {error_type}"
    if kind == "invalid_batch_response":
        return f"{_who(consumer)} returned a batchItemFailures ID that isn't in the batch, so Lambda failed the whole batch"
    if kind == "reported_failure":
        return f"{_who(consumer)} reported the message in batchItemFailures"
    if kind == "no_outcome":
        return f"{_who(consumer)}: no outcome within the Lambda timeout, most likely a timeout or out of memory (inferred)"
    if kind == "delete_failed":
        return f"{_who(consumer)}'s DeleteMessage failed: {error_code}"
    if kind == "not_deleted":
        return f"{_who(consumer)} received it and didn't delete it before its visibility timeout ran out (inferred)"
    if kind == "in_flight":
        return f"{_who(consumer)} received it over 5 minutes ago and hasn't deleted it yet"
    if kind == "fifo_blocked":
        return f"Not received: an earlier message in FIFO group {group_id} hasn't finished (inferred)"
    return "Sent, never received by an instrumented consumer (inferred)"


# ---- grouping (design sections 3.1, 5 and 6.2 step 4) -----------------------------------------------------

def _js_num(a: float) -> float:
    """A JavaScript comparator's NaN, which Array.prototype.sort takes as 0."""
    return 0 if math.isnan(a) else a


def group(items: list, first_seen_ms: Optional[dict] = None, assign: Optional[dict] = None,
          keep: Optional[Callable[[dict], bool]] = None) -> list:
    """Folds classified messages into groups, worst outcome first, then the most messages. A never-received message
    on a FIFO queue whose group has an earlier unfinished message becomes fifo_blocked; more than MAX_FINGERPRINTS
    error texts for one (queue, consumer, exception class) merge into one group. ``assign``, when given, gets each
    message ID's group id; ``keep`` narrows the messages after the FIFO check."""
    first_seen_ms = first_seen_ms or {}

    def at_of(x):
        return first_seen_ms.get(x["mid"], x["evidenceMs"])

    fifo_first: dict = {}
    for x in items:
        if not x.get("group") or not x["queue"].endswith(".fifo"):
            continue
        k = (x["queueKey"], x["group"])
        fifo_first[k] = min(fifo_first.get(k, math.inf), at_of(x))
    all_ = []
    for x in items:
        if x["kind"] == "never_received" and x.get("group") and x["queue"].endswith(".fifo"):
            first = fifo_first.get((x["queueKey"], x["group"]))
            if first is not None and first < at_of(x):
                x = {**x, "kind": "fifo_blocked", "signature": f"fifo_blocked:{x['group']}"}
        all_.append(x)
    if keep is not None:
        all_ = [x for x in all_ if keep(x)]

    prints: dict = {}
    for x in all_:
        if x["kind"] == "exc":
            prints.setdefault((x["queueKey"], x["consumer"], x["errorType"]), set()).add(x["signature"])

    def merged(x) -> bool:
        return x["kind"] == "exc" and len(prints.get((x["queueKey"], x["consumer"], x["errorType"]), ())) > MAX_FINGERPRINTS

    out: dict = {}
    for x in all_:
        sig = f"exc:{x['errorType']}:*" if merged(x) else x["signature"]
        key = f"{KEY_VERSION}|{x['queueKey']}|{x['consumer']}|{sig}"
        g = out.get(key)
        if g is None:
            g = {"id": cause_id(key), "key": key, "kind": x["kind"], "signature": sig, "label": "",
                 "basis": BASIS[x["kind"]], "inferred": BASIS[x["kind"]] == "inferred", "why": WHY.get(x["kind"]),
                 "queue": x["queue"], "queueKey": x["queueKey"], "consumer": x["consumer"], "outcome": x["outcome"],
                 "count": 0, "counts": {"dead_lettered": 0, "stuck": 0, "filtered": 0}, "dlqInferred": 0,
                 "firstMs": math.inf, "lastMs": -math.inf, "times": [], "tenants": [], "sampleError": None,
                 "sampleMid": x["mid"], "examples": [], "earlier": [], "retrying": 0, "versions": [],
                 "_first": (math.inf, None), "_x": x, "_earlier": {}, "_versions": {}}
            out[key] = g
        g["count"] += 1
        g["counts"][x["outcome"]] += 1
        if x["dlqInferred"] and x["outcome"] == "dead_lettered":
            g["dlqInferred"] += 1
        if x["outcome"] == "stuck" and x["kind"] in FAILED and x["deliveries"] >= 2:
            g["retrying"] += 1
        g["outcome"] = worse(g["outcome"], x["outcome"])
        ms = x["evidenceMs"] if _finite(x["evidenceMs"]) else NAN
        if _finite(ms):
            g["times"].append(ms)
            if ms < g["firstMs"]:
                g["firstMs"] = ms
            if ms >= g["lastMs"]:
                g["lastMs"] = ms
                g["sampleMid"] = x["mid"]
                if x["error"]:
                    g["sampleError"] = x["error"]
            if x["error"] and ms < g["_first"][0]:
                g["_first"] = (ms, x["error"])
        if x["error"] and not g["sampleError"]:
            g["sampleError"] = x["error"]
        if x["tenant"] and x["tenant"] not in g["tenants"]:
            g["tenants"].append(x["tenant"])
        # in flight or not deleted only by the assumed default timer: an inference, and the reason says so
        if x.get("assumedVisibility") and ASSUMED_VISIBILITY not in (g["why"] or ""):
            g["basis"], g["inferred"] = "inferred", True
            g["why"] = f"{g['why']} {ASSUMED_VISIBILITY}" if g["why"] else ASSUMED_VISIBILITY
        if x["version"] is not None:
            v = g["_versions"].setdefault(x["version"], {"version": x["version"], "count": 0, "firstMs": NAN, "lastMs": NAN})
            v["count"] += 1
            if _finite(ms):
                if not ms >= v["firstMs"]:
                    v["firstMs"] = ms
                if not ms <= v["lastMs"]:
                    v["lastMs"] = ms
        g["examples"].append(x["mid"])
        if assign is not None:
            assign[x["mid"]] = g["id"]
        for k in x["earlier"]:
            g["_earlier"][k] = g["_earlier"].get(k, 0) + 1

    at = {x["mid"]: x["evidenceMs"] for x in all_}
    groups = []
    for g in out.values():
        first, x, earlier, versions = g.pop("_first"), g.pop("_x"), g.pop("_earlier"), g.pop("_versions")
        n = len(prints.get((x["queueKey"], x["consumer"], x["errorType"]), ())) if g["signature"].endswith(":*") else 0
        g["label"] = cause_label(g["kind"], g["consumer"], x["fn"], x["errorType"], None if n else first[1],
                                 x["errorCode"], x.get("group"), n)
        if not _finite(g["firstMs"]):
            g["firstMs"] = g["lastMs"] = NAN
        # newest evidence first, for the examples
        g["examples"] = sorted(g["examples"], key=functools.cmp_to_key(
            lambda a, b: _js_num(at.get(b, 0) - at.get(a, 0))))[:EXAMPLES]
        g["earlier"] = sorted(({"what": EARLIER.get(k, k), "n": c} for k, c in earlier.items()), key=lambda e: -e["n"])
        # first seen first; a version with no evidence time last, then by name
        g["versions"] = sorted(versions.values(), key=lambda v: (v["firstMs"] if _finite(v["firstMs"]) else math.inf,
                                                                 v["version"]))
        groups.append(g)
    return sorted(groups, key=functools.cmp_to_key(_by_rank))


def _by_rank(a: dict, b: dict) -> int:
    d = RANK.index(a["outcome"]) - RANK.index(b["outcome"]) or b["count"] - a["count"]
    if d:
        return d
    la = a["lastMs"] if _finite(a["lastMs"]) and a["lastMs"] else 0
    lb = b["lastMs"] if _finite(b["lastMs"]) and b["lastMs"] else 0
    if lb != la:
        return 1 if lb > la else -1  # newest last seen first
    return (a["id"] > b["id"]) - (a["id"] < b["id"])


def trend_of(times: Iterable[float], from_ms: float, to_ms: float, n: int = 9) -> list:
    """A group's evidence times in n equal buckets over [from_ms, to_ms]: its trend. Messages, not deliveries."""
    out = [0] * n
    span = max(1, to_ms - from_ms)
    for t in times:
        if t < from_ms or t > to_ms:
            continue
        out[min(n - 1, math.floor((t - from_ms) / span * n))] += 1
    return out


def seen_at(x: dict, first_seen_ms: dict) -> float:
    """When a message's cause was seen: its evidence, else when the message was first seen."""
    return x["evidenceMs"] if _finite(x["evidenceMs"]) else first_seen_ms.get(x["mid"], NAN)


def in_range(classified: list, first_seen_ms: dict, from_ms: Optional[int], to_ms: Optional[int],
             assign: Optional[dict] = None) -> list:
    """The groups of the messages whose cause was seen in [from_ms, to_ms] (either end open when None)."""
    def keep(x):
        t = seen_at(x, first_seen_ms)
        return (from_ms is None or t >= from_ms) and (to_ms is None or t <= to_ms)
    return group(classified, first_seen_ms, assign, keep)


def dlq_groups(items: list, urls: dict) -> list:
    """Messages in a dead-letter queue, grouped by that queue, as the app's ``dlqGroups`` (lib/app/redrive.ts):
    the account and region are the source queue's, from its URL (a dead-letter queue shares them)."""
    by: dict = {}
    for m in items:
        if not m.get("in_dead_letter_queue"):
            continue
        place = parse_queue_url(urls.get(m["message_id"])) or {}
        account, region = place.get("account"), place.get("region")
        key = f"{account or ''}/{region or ''}/{m['current_queue']}"
        g = by.setdefault(key, {"key": key, "name": m["current_queue"], "account": account, "region": region,
                                "messageIds": [], "sources": [], "newest": None})
        g["messageIds"].append(m["message_id"])
        seen = _iso_ms(m.get("last_seen"))
        if _finite(seen) and (g["newest"] is None or seen > g["newest"]):
            g["newest"] = int(seen)
        if m["source_queue"] not in g["sources"]:
            g["sources"].append(m["source_queue"])
    return sorted(by.values(), key=lambda g: (-len(g["messageIds"]), g["name"]))


# ---- reading (design section 6.2) -------------------------------------------------------------------------

def read_unfinished(db, now_ns: int, lambda_timeout_s: int, mrc: Optional[dict], older_than_s: int,
                    queue: Optional[str] = None, limit: int = UNFINISHED_LIMIT) -> dict:
    """The stuck messages (as ``/api/stuck`` lists them, first seen over ``older_than_s`` ago with no final delete),
    up to ``limit`` of them oldest first, each built into its timeline and classified.

    Returns ``classified`` (cause_of's, one per message), ``items`` (each message's ``/api/stuck`` item, oldest
    first), ``first_seen`` (message ID -> ms), ``urls`` (message ID -> its source queue's URL) and ``capped``
    (more messages are stuck than were read)."""
    found = db.stuck_messages(now_ns - older_than_s * 1_000_000_000, limit + 1, queue)
    capped = len(found) > limit
    found = found[:limit]
    sums = {s["mid"]: s for s in db.summaries([r.message_id for r in found])}
    now_ms = now_ns // 1_000_000
    classified, items, first_seen, urls = [], [], {}, {}
    rows_of = db.messages_rows([r.message_id for r in found])
    # the PurgeQueue calls on these messages' queues (contract section 18): one read for them all
    purges = db.purges({x["queue"] for rows in rows_of.values() for x in rows})
    for r in found:
        rows = rows_of.get(r.message_id)
        if not rows or r.message_id not in sums:
            continue
        t = timeline.build(rows, now_ns, lambda_timeout_s, mrc, purges)
        if t["state"] in ("purged", "deleted", "deleted_inferred"):
            continue  # the stuck query leaves these out already; a span stored since can add one
        url = next((x["queue_url"] for x in rows if x["queue"] == t["source_queue"] and x["queue_url"]), None)
        x = cause_of(t, now_ms, queue_url=url, tenant=summary.message_tenant(rows))
        classified.append(x)
        first_seen[r.message_id] = r.first_ns // 1_000_000
        urls[r.message_id] = url
        item = summary.item(sums[r.message_id], now_ns, lambda_timeout_s, mrc, purges)
        item["age_s"] = round((now_ns - r.first_ns) / 1e9, 3)
        item["age_across_clocks"] = r.on_sqs_clock
        items.append(item)
    return {"classified": classified, "items": items, "first_seen": first_seen, "urls": urls, "capped": capped}


def public(g: dict) -> dict:
    """A group as JSON: NaN (no evidence time) as null, as JSON.stringify writes it in the app."""
    def fix(v):
        if isinstance(v, float):
            return None if math.isnan(v) else (int(v) if v.is_integer() else v)
        if isinstance(v, list):
            return [fix(e) for e in v]
        if isinstance(v, dict):
            return {k: fix(e) for k, e in v.items()}
        return v
    return fix(g)
