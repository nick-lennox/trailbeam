"""Per-queue events, the queue key and the queue page's series (QV-363; the page is QV-360's design), the same for
every store.

**The queue key.** A queue is told apart by its account, region and name, as its ARN is, so two queues called
``orders`` in different accounts or regions are two queues: ``<account>/<region>/<name>`` when the span's
``aws.sqs.queue.url`` names an account, else the URL, else the name. This is the hosted app's ``queueKey``
(trailbeam-app ``lib/app/queue-url.ts``), so both products key a queue the same way.

**Queue events.** ``events(rows)`` turns one message's spans into what it adds to each queue's page, as
``(at_ms, kind, queue_key, service, lambda, value, peer, version)`` (``version``: the span's ``service.version``,
QV-362, or None); ``summary.summarize`` keeps them as ``queue_events``
and a store indexes them by (queue key, time), so a queue's range is one index range scan, and adds them up per
hour in ``queue_rollup`` for the flows (below). The kinds:

- ``send``: a send span, except those SQS deduplicated (timeline.deduplicated_sends), at its time; ``value`` its
  ``messaging.message.body.size``, when it has one.
- ``receive``: a receive span, ``value`` its receive count (2 or more: a re-receive).
- ``wait``: the message's first receive (receive count 1) when a send of it was recorded, ``value`` the time in
  queue from the earliest send to that receive, at the receive's time. ``no_send``: a first receive with no
  recorded send. The page's p95 time in queue reads ``wait`` only and reports ``no_send`` as a count. (The hosted
  explorer falls back to SQS's SentTimestamp there; the page does not, because that clock is SQS's, not the
  sender's.)
- ``delete`` / ``delete_failed``: an accepted / failed delete, ``value`` the time in handler (below), NULL when
  the receive whose handle it used was not recorded.
- ``lambda_ok`` / ``failed``: a Lambda outcome span, ``success`` / any failed value, ``value`` the time in handler.
- ``visibility``: a change_visibility span (for the flow counts).
- ``dlq_seen``: SQS moved the message to a dead-letter queue, as a receive from it reported
  (``DeadLetterQueueSourceArn``, contract 9b), once per (source, DLQ) at the earliest receive reporting it. It
  belongs to the *source* queue, keyed from the ARN.
- ``at_max``: the message is unfinished (no accepted delete of its latest receive, no Lambda success for it) and
  no move was seen: its latest receive on its source queue, ``value`` that receive count. With the queue's
  configured maxReceiveCount, ``value >= maxReceiveCount`` is a move SQS will have made that no DLQ receive has
  shown yet (timeline's ``not_seen_on_dlq_yet``), so it is ``dlq_inferred``.
- ``dlq_receive``: a receive that reported a ``DeadLetterQueueSourceArn`` naming another queue, every one, keyed by
  the queue it was received from (the dead-letter queue), ``peer`` the source queue's key (``place_key`` of the
  ARN). The flows link a queue to its dead-letter queue by these, as the hosted app's do.

Only ``dlq_receive`` has a ``peer``; it is None on the others.

**Time in handler** is from the end of the receive to the start of the delete (the time between the two calls,
an approximation), and for a Lambda from the end of the receive to the outcome (a zero-length span, at the moment
the wrapper decided). It is the hosted app's processing time (``live-events.ts``, ``overview.ts queueP95``): every
delete with a recorded receive, accepted or not, and every Lambda outcome, success or failure, at the end time.

**Flows** (``flows``; the overview, ``GET /api/flows``) are the hosted app's ``listFlows`` (trailbeam-app
``lib/app/flows.ts``), in its shape, from the hourly ``queue_rollup``: per queue, the services that sent to it and
received from it, the counts of sends, receives, deletes (accepted or not) and visibility changes, the average body
size of the sends that reported one, and its dead-letter queue, when a receive from another queue ever named it as
the source. One difference: SQS-deduplicated FIFO sends are not counted (they were not enqueued); the app counts
every send span.

**Steps.** The page uses one step for every chart: at least 60 s and one of 1, 5, 15 or 30 min, 1, 3 or 6 h
(``STEPS_S``), so buckets line up with the CloudWatch period it asks for. ``default_step`` is the page's rule,
``max(60 s, nice(range / 120))``. Buckets start at multiples of the step since the epoch, as CloudWatch's do.
"""
from __future__ import annotations

import json
import math
import re
from typing import Iterable, NamedTuple, Optional
from urllib.parse import unquote, urlsplit

SEND, RECEIVE, WAIT, NO_SEND = "send", "receive", "wait", "no_send"
DELETE, DELETE_FAILED, LAMBDA_OK, FAILED, VISIBILITY = "delete", "delete_failed", "lambda_ok", "failed", "visibility"
DLQ_SEEN, AT_MAX, DLQ_RECEIVE = "dlq_seen", "at_max", "dlq_receive"
KINDS = (SEND, RECEIVE, WAIT, NO_SEND, DELETE, DELETE_FAILED, LAMBDA_OK, FAILED, VISIBILITY, DLQ_SEEN, AT_MAX,
         DLQ_RECEIVE)
HANDLER_KINDS = (DELETE, DELETE_FAILED, LAMBDA_OK, FAILED)  # p95 time in handler

STEPS_S = (60, 300, 900, 1800, 3600, 10800, 21600)
MAX_BUCKETS = 2000  # 400 days at 6 h is 1,600

_NS_PER_MS = 1_000_000
_REGION = re.compile(r"(?:^|\.)((?:[a-z]{2}(?:-gov|-iso[a-z]?)?)-[a-z]+-\d+)(?=\.|$)")
_ACCOUNT = re.compile(r"^\d{12}$")
_ARN = re.compile(r"^arn:[^:]+:sqs:([^:]*):([^:]*):(.+)$")


class Place(NamedTuple):
    name: str
    account: Optional[str]
    region: Optional[str]


def parse_url(url: Optional[str]) -> Optional[Place]:
    """https://sqs.<region>.amazonaws.com/<account>/<name> and its older, VPC endpoint, China and emulator forms."""
    if not url:
        return None
    try:
        u = urlsplit(url)
    except ValueError:
        return None
    if not u.scheme or not u.netloc:
        return None
    parts = [p for p in u.path.split("/") if p]
    if not parts:
        return None
    before = parts[-2] if len(parts) > 1 else None
    host = (u.hostname or "").lower()
    m = _REGION.search(host)
    region = m.group(1) if m else ("us-east-1" if host == "queue.amazonaws.com" else None)
    return Place(unquote(parts[-1]), before if before and _ACCOUNT.match(before) else None, region)


def parse_arn(arn: Optional[str]) -> Optional[Place]:
    """arn:aws:sqs:<region>:<account>:<name>."""
    m = _ARN.match(arn or "")
    return Place(m.group(3), m.group(2) or None, m.group(1) or None) if m else None


def key(name: str, url: Optional[str]) -> str:
    """The key a queue is stored and grouped under (queue-url.ts ``queueKey``)."""
    p = parse_url(url)
    if p is not None and p.account:
        return f"{p.account}/{p.region or ''}/{name}"
    return url or name


def place_key(p: Place) -> str:
    return f"{p.account}/{p.region or ''}/{p.name}" if p.account else p.name


def place_of_key(k: str) -> Place:
    """The name, account and region a key says: an account key, a URL or a bare name."""
    parts = k.split("/")
    if len(parts) >= 3 and _ACCOUNT.match(parts[0]) and "://" not in k:
        return Place("/".join(parts[2:]), parts[0], parts[1] or None)
    p = parse_url(k)
    return p if p is not None else Place(k, None, None)


def _fits(a: Place, b: Place) -> bool:
    """Two places can be one queue: the same name, and account and region the same where both say one."""
    return (a.name == b.name and (a.account is None or b.account is None or a.account == b.account)
            and (a.region is None or b.region is None or a.region == b.region))


def resolve(asked: str, keys: Iterable[str]) -> tuple:
    """The stored keys that are the queue ``asked`` (a key, or a name), its own key first, and its place.

    A queue's spans don't all say as much about where it is: spans without a URL are keyed by the name, a local
    emulator's URL names no region, and a dead-letter move names its source queue by ARN, which always has both.
    A key that says less (``orders``, ``000000000000//orders``) belongs to the one key that says more and fits it
    (``_top``), when there is one: as the hosted app's flows give spans without a URL to the one queue of that
    name. A queue with no stored key is itself, with no events. ValueError: a name more than one queue has."""
    places = {k: place_of_key(k) for k in keys}
    if asked not in places:
        named = [k for k, p in places.items() if p.name == asked]
        tops = sorted({_top(k, places) or k for k in named})
        if not tops:
            return [asked], place_of_key(asked)
        if len(tops) > 1:
            raise ValueError(f"more than one queue is called {asked}: ask by key, one of " + ", ".join(tops))
        asked = tops[0]
    own = _top(asked, places) or asked
    return [own] + sorted(k for k in places if k != own and _top(k, places) == own), places[own]


def _more(a: Place, b: Place) -> int:
    """How much more ``a`` says about where the queue is than ``b`` (account, region)."""
    return (a.account is not None) + (a.region is not None) - (b.account is not None) - (b.region is not None)


def _top(k: str, places: dict) -> Optional[str]:
    """The one key that fits ``k``, says more than it, and has no key above it in turn; None if none or several."""
    up = [o for o, p in places.items() if o != k and _fits(places[k], p) and _more(p, places[k]) > 0]
    top = [o for o in up if not any(_fits(places[o], places[x]) and _more(places[x], places[o]) > 0 for x in up)]
    return top[0] if len(top) == 1 else None


def _ms(ns: int) -> int:
    return int(ns) // _NS_PER_MS


def _handler_ms(receive, end_row) -> Optional[int]:
    """Receive end to the settle's start (or end, without a start), in ms; None without the receive."""
    if receive is None:
        return None
    at = end_row["start_ns"] if end_row["start_ns"] is not None else end_row["time_ns"]
    return max(0, _ms(at) - _ms(receive["time_ns"]))


def _lambda(r) -> int:
    if r["kind"] == "send":
        return int(bool(r["faas_name"]))
    return int(r["source"] == "lambda" or r["operation"] == "LambdaEvent")


def _version(r) -> Optional[str]:
    """The span's service.version; None on a row from before it was kept (an older schema's, mid-migration)."""
    try:
        return r["service_version"]
    except (IndexError, KeyError):
        return None


def _body_size(r) -> Optional[int]:
    try:
        a = r["attributes"]
        v = (json.loads(a) if isinstance(a, (str, bytes)) else a or {}).get("messaging.message.body.size")
    except (ValueError, AttributeError, KeyError, IndexError, TypeError):
        return None
    return int(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0 else None


def events(rows, deduplicated: Iterable[int] = ()) -> list:
    """A message's queue events (module docstring), from every row of it in (time_ns, row) order. ``deduplicated``:
    the rows of sends SQS deduplicated (timeline.deduplicated_sends)."""
    deduplicated = set(deduplicated)
    out: list = []
    by_row = {r["row"]: r for r in rows}
    receives = [r for r in rows if r["kind"] == "receive"]
    sends = [r for r in rows if r["kind"] == "send"]
    latest = receives[-1] if receives else None

    def add(r, kind, value=None, k=None, peer=None):
        out.append([_ms(r["time_ns"]), kind, k if k is not None else key(r["queue"], r["queue_url"]),
                    r["service_name"], _lambda(r), value, peer, _version(r)])

    for r in rows:
        kind = r["kind"]
        if kind == "send":
            if r["row"] not in deduplicated:
                add(r, SEND, _body_size(r))
        elif kind == "receive":
            add(r, RECEIVE, r["receive_count"])
            src = parse_arn(r["dlq_source_arn"]) if r["dlq_source_arn"] else None
            if src is not None and src.name != r["queue"]:
                add(r, DLQ_RECEIVE, None, None, place_key(src))
        elif kind == "delete":
            add(r, DELETE if r["result"] == "ok" else DELETE_FAILED, _handler_ms(by_row.get(r["receive_row"]), r))
        elif kind == "change_visibility":
            add(r, VISIBILITY)

    # Lambda outcomes: one per invocation, the latest span of it (as timeline.build), against the receive of it
    outcomes: dict = {}
    for r in rows:
        if r["kind"] == "outcome":
            outcomes[r["faas_invocation_id"]] = r
    for inv, o in outcomes.items():
        rec = next((x for x in reversed(receives) if x["faas_invocation_id"] == inv and inv is not None), None)
        add(o, LAMBDA_OK if o["outcome"] == "success" else FAILED, _handler_ms(rec, o))

    if receives and receives[0]["receive_count"] == 1:
        first = receives[0]
        if sends:
            add(first, WAIT, max(0, _ms(first["time_ns"]) - _ms(min(s["time_ns"] for s in sends))))
        else:
            add(first, NO_SEND)

    # observed moves (summary.summarize's reports): once per (source, DLQ), at the earliest receive reporting it
    reports: dict = {}
    for r in receives:
        src = parse_arn(r["dlq_source_arn"]) if r["dlq_source_arn"] else None
        if src is not None and src.name != r["queue"]:
            pair = (src, r["queue"])
            k = (r["receive_count"], r["time_ns"], r["row"])
            if pair not in reports or k < reports[pair][0]:
                reports[pair] = (k, r)
    for (src, _), (_, r) in sorted(reports.items(), key=lambda kv: kv[1][0][1:]):
        add(r, DLQ_SEEN, None, place_key(src))

    if latest is not None and not reports:
        final_delete = any(r["kind"] == "delete" and r["result"] == "ok" and r["receive_row"] == latest["row"]
                           for r in rows)
        inv = latest["faas_invocation_id"]
        lambda_ok = inv is not None and inv in outcomes and outcomes[inv]["outcome"] == "success"
        if not final_delete and not lambda_ok:
            source = sends[0]["queue"] if sends else receives[0]["queue"]
            on_source = [r for r in receives if r["queue"] == source and r["receive_count"] is not None]
            if on_source:
                top = max(on_source, key=lambda r: (r["receive_count"], r["time_ns"], r["row"]))
                add(top, AT_MAX, top["receive_count"])
    out.sort(key=lambda e: (e[0], e[1], e[2]))
    return out


# ---- the series ----------------------------------------------------------------------------------------------


def nice_step(raw_s: float) -> int:
    """The smallest of STEPS_S at least ``raw_s``; the largest for anything longer."""
    for s in STEPS_S:
        if s >= raw_s:
            return s
    return STEPS_S[-1]


def default_step(from_ms: int, to_ms: int) -> int:
    """The page's rule (QV-360 design 4.1): max(60 s, nice(range / 120)). 1 h gives 60 s, 1 d 15 min, 7 d 1 h."""
    return max(60, nice_step((to_ms - from_ms + 1) / 1000 / 120))


def check_step(step_s: int, from_ms: int, to_ms: int) -> None:
    """ValueError unless ``step_s`` is one of STEPS_S and the range is at most MAX_BUCKETS buckets of it."""
    if step_s not in STEPS_S:
        raise ValueError("step_s must be one of " + ", ".join(map(str, STEPS_S)) + " (1, 5, 15 or 30 min, 1, 3 or 6 h)")
    n = to_ms // (step_s * 1000) - from_ms // (step_s * 1000) + 1
    if n > MAX_BUCKETS:
        raise ValueError(f"{n} buckets of {step_s} s: at most {MAX_BUCKETS}; use a larger step_s or a shorter range")


def p95(values: list) -> Optional[int]:
    """Nearest rank, as queries.p95."""
    if not values:
        return None
    s = sorted(values)
    return s[max(0, math.ceil(0.95 * len(s)) - 1)]


def series(rows: Iterable, step_s: int, max_receive_count: Optional[int], by_version: bool = False) -> list:
    """The buckets of one queue's events, ``rows`` ``(at_ms, kind, value, service, version)`` in the range (the
    last two may be left off), as the API returns them: a bucket for every step with at least one event, in time
    order. A step with none is left out: the page draws a gap there, not a zero. ``max_receive_count``: the
    queue's configured one, else no ``dlq_inferred``.

    ``by_version`` (the page's "By version", QV-360 design 4.6): each bucket also has ``by_version``, one entry per
    ``service.version`` with a handled message or a p95 sample in it (None: spans without one), in version order,
    None last: ``handled`` and the two p95s, each by the version of the span that ends it (the receive for time in
    queue; the delete or Lambda outcome for time in handler and handled)."""
    step_ms = step_s * 1000
    by: dict = {}
    for r in rows:
        at, kind, value = r[0], r[1], r[2]
        t = (int(at) // step_ms) * step_ms
        b = by.get(t)
        if b is None:
            b = by[t] = {"sends": 0, "receives": 0, "re_receives": 0, "handled": 0, "failed": 0, "dlq_seen": 0,
                         "dlq_inferred": 0, "no_send_receives": 0, "_wait": [], "_handler": [], "_any": False,
                         "_v": {}}
        v = None
        if by_version and kind in (WAIT,) + HANDLER_KINDS:
            v = b["_v"].setdefault(r[4] if len(r) > 4 else None, {"handled": 0, "_wait": [], "_handler": []})
        if kind == SEND:
            b["sends"] += 1
        elif kind == RECEIVE:
            b["receives"] += 1
            if value is not None and value >= 2:
                b["re_receives"] += 1
        elif kind == WAIT:
            b["_wait"].append(value)
            if v is not None:
                v["_wait"].append(value)
        elif kind == NO_SEND:
            b["no_send_receives"] += 1
        elif kind in HANDLER_KINDS:
            if kind in (DELETE, LAMBDA_OK):
                b["handled"] += 1
                if v is not None:
                    v["handled"] += 1
            elif kind == FAILED:
                b["failed"] += 1
            if value is not None:
                b["_handler"].append(value)
                if v is not None:
                    v["_handler"].append(value)
        elif kind == DLQ_SEEN:
            b["dlq_seen"] += 1
        elif kind == AT_MAX:
            if max_receive_count and value is not None and value >= max_receive_count:
                b["dlq_inferred"] += 1
            else:
                continue
        else:
            continue  # visibility changes, and kinds a newer server wrote
        b["_any"] = True
    out = []
    for t in sorted(by):
        b = by[t]
        if not b.pop("_any"):
            continue
        wait, handler, versions = b.pop("_wait"), b.pop("_handler"), b.pop("_v")
        row = {"t_ms": t, **b, "p95_queue_ms": p95(wait), "p95_handler_ms": p95(handler),
               "queue_samples": len(wait), "handler_samples": len(handler)}
        if by_version:
            row["by_version"] = [
                {"version": k, "handled": x["handled"], "p95_queue_ms": p95(x["_wait"]),
                 "p95_handler_ms": p95(x["_handler"]), "queue_samples": len(x["_wait"]),
                 "handler_samples": len(x["_handler"])}
                for k, x in sorted(versions.items(), key=lambda kv: (kv[0] is None, kv[0] or ""))
                if x["handled"] or x["_wait"] or x["_handler"]]
        out.append(row)
    return out


def versions(rows: Iterable) -> list:
    """The deploy markers (QV-360 design 4.6): for each service, every ``service.version`` that first shows on the
    queue's events in the range after another version of it did, at that first time; ``rows`` as ``series`` takes
    them. A service's earliest version in the range was running when the range began, so it is no deploy, and a
    service with one version has none. By time, then service and version. Events without a service or a version
    make none."""
    first: dict = {}
    for r in rows:
        if len(r) < 5 or r[3] is None or r[4] is None:
            continue
        k = (r[3], r[4])
        if k not in first or r[0] < first[k]:
            first[k] = int(r[0])
    earliest: dict = {}
    for (svc, v), t in first.items():
        if svc not in earliest or (t, v) < earliest[svc]:
            earliest[svc] = (t, v)
    return [{"service": svc, "version": v, "first_ms": t}
            for (svc, v), t in sorted(first.items(), key=lambda kv: (kv[1], kv[0])) if earliest[svc] != (t, v)]


# ---- the flows -----------------------------------------------------------------------------------------------

NO_SERVICE = "unknown_service"  # a span without service.name, as the hosted app shows it


def rollup_key(e) -> tuple:
    """The queue_rollup row (hour, queue_key, kind, service, lambda, peer) an event adds to; '' for no service or
    peer (they are in the primary key)."""
    at, kind, k, svc, lam, _v, peer = e[:7]
    return (int(at) // 3_600_000, k, kind, svc or "", int(lam), peer or "")


def rollup_sums(added: Iterable = (), removed: Iterable = ()) -> dict:
    """{rollup_key: [n, vn, vsum]}: what adding the events ``added`` and taking out ``removed`` changes in
    queue_rollup, the keys that come to nothing left out. Exact: counts and sums subtract."""
    d: dict = {}
    for events, sign in ((added, 1), (removed, -1)):
        for e in events:
            v = e[5]
            c = d.setdefault(rollup_key(e), [0, 0, 0])
            c[0] += sign
            if v is not None:
                c[1] += sign
                c[2] += sign * int(v)
    return {k: c for k, c in d.items() if c != [0, 0, 0]}


def labels(places: dict) -> dict:
    """{key: the name as shown, unique}: the name, or with what tells it apart from the others of that name (the
    region within one account, else the account, else the key). queue-url.ts ``queueLabels``."""
    by: dict = {}
    for k, p in places.items():
        by.setdefault(p.name, []).append(k)
    out = {}
    for name, same in by.items():
        for k in same:
            if len(same) == 1:
                out[k] = name
                continue
            p = places[k]
            same_account = sum(places[x].account == p.account for x in same) > 1
            out[k] = f"{name} ({(p.region if same_account else p.account) or k})"
    return out


def flows(keys: Iterable[str], last_seen: dict, counts: Iterable, links: Iterable) -> list:
    """The flows (module docstring), as the hosted app's ``Flow`` objects. ``keys``: every queue key with events;
    ``last_seen``: {key: its latest event's at_ms}; ``counts``: (key, kind, service, lambda, peer, n, vn, vsum) in
    the range, added up (vn, vsum: how many had a value, and their sum); ``links``: (dead-letter queue key, source
    key) pairs ever seen, with ``counts``' dlq_receive rows giving how many in the range.

    A key that says less about where its queue is goes to the one queue it fits (``resolve``), so a queue's spans
    without a URL join it. Ordered by sends and receives, most first, then by the name shown."""
    keys = sorted(set(keys))
    places = {k: place_of_key(k) for k in keys}
    top = {k: _top(k, places) or k for k in keys}
    own = sorted(set(top.values()))
    shown = labels({k: places[k] for k in own})
    out = {}
    for k in own:
        p = places[k]
        out[k] = {"key": k, "queue": shown[k], "name": p.name, "url": k if "://" in k else None,
                  "account": p.account, "region": p.region, "lastSeen": 0,
                  "sent": 0, "received": 0, "deleted": 0, "visibilityChanges": 0,
                  "producers": [], "consumers": [], "avgBytes": None, "dlq": None, "_bytes": [0, 0]}
    for k in keys:
        f = out[top[k]]
        f["lastSeen"] = max(f["lastSeen"], int(last_seen.get(k) or 0))

    def flow_of(k):
        if k in top:
            return out[top[k]]
        p = place_of_key(k)  # a key no event has (a source ARN's): the one queue it fits
        fits = sorted({top[o] for o in keys if _fits(p, places[o])})
        return out[fits[0]] if len(fits) == 1 else None

    def add(lst, name, n, lam):
        for s in lst:
            if s["name"] == name:
                s["n"] += n
                s["lambda"] = s["lambda"] or lam
                return
        lst.append({"name": name, "n": n, "lambda": lam})

    moved: dict = {}
    for k, kind, svc, lam, peer, n, vn, vsum in counts:
        f = flow_of(k)
        if f is None or not n:
            continue
        n = int(n)
        if kind == SEND:
            f["sent"] += n
            add(f["producers"], svc or NO_SERVICE, n, bool(lam))
            f["_bytes"][0] += int(vn or 0)
            f["_bytes"][1] += int(vsum or 0)
        elif kind == RECEIVE:
            f["received"] += n
            add(f["consumers"], svc or NO_SERVICE, n, bool(lam))
        elif kind in (DELETE, DELETE_FAILED):
            f["deleted"] += n
        elif kind == VISIBILITY:
            f["visibilityChanges"] += n
        elif kind == DLQ_RECEIVE:
            moved[(k, peer)] = moved.get((k, peer), 0) + n
    for dlq_key, src_key in sorted(set(links)):
        src, target = flow_of(src_key), flow_of(dlq_key)
        if src is None or target is None or src is target:
            continue
        if src["dlq"] is None:
            src["dlq"] = {"name": target["queue"], "received": 0}
        if src["dlq"]["name"] == target["queue"]:
            src["dlq"]["received"] += moved.get((dlq_key, src_key), 0)
    for f in out.values():
        n, total = f.pop("_bytes")
        f["avgBytes"] = total / n if n else None
        f["producers"].sort(key=lambda s: -s["n"])
        f["consumers"].sort(key=lambda s: -s["n"])
    return sorted(out.values(), key=lambda f: (-(f["sent"] + f["received"]), f["queue"]))
