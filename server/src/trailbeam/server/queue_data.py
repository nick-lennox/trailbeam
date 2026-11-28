"""Queues as AWS and their spans describe them together (QV-589): the hosted app's ``buildQueueData``
(trailbeam-app lib/app/queue-data.ts), for ``/api/queues/{queue}/now``, ``/api/queues/{queue}/series`` and the
dashboards' ``queueLive``.

``build`` reads AWS for the queues (aws_reader.AwsReader.read, which makes no AWS call while reads are off), works
out "In ‹queue› now" for the ones asked, with the settings and SQS counts the reads returned (queue_now.view), and
the queue's figures from both (aws_figures.live_of). Only queues the store has seen are read (``known``): a page's URL
or a dashboard's measure can name any queue, and the server's credentials do the reading (QV-740). Each queue's
answer is ``{"aws", "live", "inQueue"}``: ``aws`` in @trailbeam/ui/model's ``QueueAws`` shape, ``live`` its
``QueueLive``, ``inQueue`` its ``InQueueView`` or None.

The span-derived history the hosted dashboards draw (Stuck in flight and One receive from the dead-letter queue over
the day, lib/app/span-history.ts) is not worked out yet: those measures show their figure now.
"""
from __future__ import annotations

from typing import Callable, Optional

from . import aws_figures as af, aws_reader

#: the queue page's one CloudWatch window (queue-page.ts PAGE_WINDOW)
PAGE_WINDOW = "page"
#: the Overview's (queue-data.ts overviewWindows)
OVERVIEW_WINDOW = "recent"
#: the CloudWatch metrics the Overview's health uses (@trailbeam/ui's queue-live.ts health): the age of the oldest
#: message, and sends and deletes for whether the backlog drains. Its depth and messages in flight are SQS's own
#: counts (GetQueueAttributes, not billed per metric), so CloudWatch's are not asked for (QV-743)
OVERVIEW_METRICS = ("oldestS", "sent", "deleted")
#: the most queues one /api/queues/now asks for; the Overview asks in groups of fewer (flows-data.ts NOW_BATCH)
NOW_BATCH_MAX = 100
_DAY_MS = 86_400_000


def overview_windows(now_ms: int, minutes: int) -> list:
    """The past range the Overview shows, at one minute, or five past six hours (queue-data.ts overviewWindows)."""
    return [{"id": OVERVIEW_WINDOW, "fromMs": now_ms - max(60, minutes) * 60_000, "toMs": now_ms,
             "periodS": 300 if minutes > 6 * 60 else 60}]


def dashboard_windows(now_ms: int) -> list:
    """A day at one minute and 30 days at one hour (queue-data.ts dashboardWindows)."""
    return [{"id": "day", "fromMs": now_ms - _DAY_MS, "toMs": now_ms, "periodS": 60},
            {"id": "month", "fromMs": now_ms - 30 * _DAY_MS, "toMs": now_ms, "periodS": 3600}]


def page_window(from_ms: int, to_ms: int, step_s: int, now_ms: int) -> list:
    """The queue page's range at its step, or the shortest period CloudWatch keeps that far back."""
    return [{"id": PAGE_WINDOW, "fromMs": from_ms, "toMs": to_ms,
             "periodS": max(step_s, af.period_for(from_ms, to_ms, now_ms))}]


def ref_of(key: str, place, url: Optional[str] = None) -> dict:
    """A queue as the reader takes it, from its stored key and place (queues.resolve)."""
    return {"key": key, "name": place.name, "account": place.account, "region": place.region, "url": url}


def totals_of(aws: dict, window_id: str) -> Optional[dict]:
    """SQS's counts for "In ‹queue› now", when the queue was read."""
    if aws.get("status") != "ok":
        return None
    f = af.now_figures(aws, window_id)
    return {"visible": f["visible"], "inFlight": f["inFlight"], "delayed": f["delayed"]}


def read(reader: Optional[aws_reader.AwsReader], refs: list, windows: list, now_ms: int, *, known, **kw) -> dict:
    """The reads by queue key; every queue ``not_configured`` when the server has no reader. ``known``: the store's
    queue keys. A queue whose key is not one is not read (``no_account``, NOT_SEEN), whatever the settings say."""
    if reader is None:
        return {q["key"]: af.empty_aws("not_configured", aws_reader.OFF) for q in refs}
    known = set(known)
    seen = [q for q in refs if q["key"] in known]
    out = reader.read(seen, windows, now_ms, **kw) if seen else {}
    for q in refs:
        if q["key"] not in known:
            out[q["key"]] = af.empty_aws("not_configured", aws_reader.OFF) if not reader.on() else \
                af.empty_aws("no_account", aws_reader.NOT_SEEN)
    return out


def build(reader: Optional[aws_reader.AwsReader], refs: list, windows: list, now_ms: int,
          in_queue: Callable[[dict, Optional[dict], Optional[dict]], Optional[dict]], *, known,
          want: Optional[Callable[[dict, dict], bool]] = None, triggers: bool = True,
          metrics: Optional[list] = None, dlq_depth: str = "cloudwatch",
          attrs_ttl_ms: Optional[float] = None, caller: Optional[str] = None, admin: bool = True) -> dict:
    """``{key: {"aws", "live", "inQueue"}}`` for the queues ``refs`` (ref_of) over ``windows``. ``in_queue(ref,
    settings, totals)``: the queue's InQueueView (queue_now.view), worked out for the queues ``want(ref, aws)`` says
    (all when None). ``known``: the store's queue keys (read). The other options are the reader's."""
    aws = read(reader, refs, windows, now_ms, known=known, triggers=triggers, metrics=metrics, dlq_depth=dlq_depth,
               attrs_ttl_ms=attrs_ttl_ms, caller=caller, admin=admin)
    first = windows[0]["id"] if windows else ""
    out: dict = {}
    for q in refs:
        a = aws[q["key"]]
        view = in_queue(q, a.get("settings"), totals_of(a, first)) if want is None or want(q, a) else None
        out[q["key"]] = {"aws": a, "live": af.live_of(a, view, windows, now_ms), "inQueue": view}
    return out


def live_by_name(reader: Optional[aws_reader.AwsReader], db, names: list, now_ms: int, lambda_timeout_s: int,
                 max_receive_counts: Optional[dict] = None, *, windows: Optional[list] = None,
                 metrics: Optional[list] = None, dlq_depth: str = "cloudwatch", in_queue: bool = True,
                 admin: bool = True) -> dict:
    """``{queue name: QueueLive}``, the ``queueLive`` of the measures engine's EventData (dashboards_api.event_data)
    and the alert check, over the dashboards' windows by default. A name is the stored queue's of that name that was
    active last when two share it, as the measures go by name. "In ‹queue› now" (for Stuck in flight and One receive
    from the DLQ) is worked out only for the queues AWS read, and only when ``in_queue``: with reads off this reads
    nothing but the store's queue list."""
    from . import queue_now, queues as queues_mod  # queue_now imports the timeline, which is heavy

    try:
        keys = db.queue_keys()
    except NotImplementedError:
        keys = []
    first: dict = {}
    for k in keys:
        first.setdefault(queues_mod.place_of_key(k).name, k)
    refs, places = [], {}
    for n in dict.fromkeys(names):
        k = first.get(n, n)
        try:
            want, place = queues_mod.resolve(k, keys) if n in first else ([k], queues_mod.place_of_key(k))
        except ValueError:
            want, place = [k], queues_mod.place_of_key(k)
        places[k] = (want, place)
        refs.append({**ref_of(k, place, k if "://" in k else None), "name": n})
    now_ns = now_ms * 1_000_000

    def view(ref: dict, settings, totals):
        want, place = places[ref["key"]]
        return queue_now.view(db, want, place, now_ns, lambda_timeout_s, max_receive_counts, settings=settings,
                              totals=totals)

    got = build(reader, refs, dashboard_windows(now_ms) if windows is None else windows, now_ms, view, known=keys,
                want=lambda q, a: in_queue and a.get("status") == "ok", triggers=False, metrics=metrics,
                dlq_depth=dlq_depth, admin=admin)
    return {q["name"]: got[q["key"]]["live"] for q in refs}
