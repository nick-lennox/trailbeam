"""What AWS says about each queue (QV-589), when AWS reads are on (aws_settings.py): its settings, its CloudWatch series
over the windows asked for, and its Lambda triggers, each queue in the shape of @trailbeam/ui/model's ``QueueAws``.

This is the hosted app's ``readQueuesAws`` (trailbeam-app lib/app/queue-aws.ts), with one difference: the hosted app
assumes a role per connected account, and self-hosted Trailbeam has one set of credentials, boto3's own (the
environment, a profile, an instance or task role).

**Who picks what is read (QV-740).** The credentials are the server's, but a queue's key, name, account, region and
URL come from spans, which anyone who can send them chooses, and from a page's URL, which any member chooses. So only
the admin's settings decide where the server reads: a queue is read only when

- its name is one SQS allows (aws_settings.is_queue_name) and the settings name it (an exact name or a pattern);
- it is in the settings' account: ``accountId``, else the credentials' own (sts:GetCallerIdentity). A queue whose
  spans name another account is not read;
- it is in the settings' home region (``region``, else boto3's default) or one of its other ``regions``.

Its URL is then built from that account, region and name.

**CloudWatch is the credentials' own account's (QV-741).** A queue policy can let the credentials read another
account's queue with SQS, but CloudWatch's SQS metrics are named by QueueName alone, in the account that calls. So
when the settings' ``accountId`` is not the credentials' account, a GetMetricData would return the figures of a
same-named queue in the credentials' account. Such a queue keeps its SQS settings and counts, and gets no CloudWatch
series, with a sentence in ``errors`` (``OTHER_ACCOUNT_METRICS``). When the credentials' account can't be found, the
series are not read either (``METRICS_UNCONFIRMED``). A URL from spans is never passed to AWS, so a URL key
without an account in it can't skip the account check. The dead-letter queue a redrive policy names is read only in
the same account and region. The callers (queue_data.py) also read only queues the store has seen.

Its statuses mean:

- ``not_configured``: reads are off, or on without boto3 installed. With reads off nothing is read and no client is
  made, so the server makes no AWS call.
- ``no_account``: reads are on, but the settings don't name this queue, or it is in another AWS account or region
  than they allow, or its name is not an SQS queue's.
- ``failed``: a read failed; the sentence says what AWS answered.
- ``ok``: read. A read that failed while others worked (a denied lambda:ListEventSourceMappings) is in ``errors``.

Results are kept in memory for a short while, with the hosted app's lifetimes: CloudWatch's SQS metrics change once a
minute, and every GetMetricData call is billed to the AWS account (about $0.01 per 1,000 metrics requested,
https://aws.amazon.com/cloudwatch/pricing/), so a page that refreshes every few seconds must not read them every time.
Two requests that miss the cache together make one read, and a failed read is not kept. ``read`` never raises.

**Reads give up in seconds (QV-742).** They run inside a page's request. Every AWS call gives up after a few seconds
(aws_reads.clients), and a request waiting on a read another one started waits WAIT_S at most, so a slow or
unreachable AWS makes the page say so instead of holding it, and every other viewer's, for minutes. Clients are made
only for the regions the settings allow (QV-740), so a page's URL can't make the server build more.
"""
from __future__ import annotations

import concurrent.futures as cf
import threading
from typing import Any, Callable, Optional

from . import aws_figures as af, aws_reads, aws_settings

#: GetQueueAttributes carries SQS's own current counts, and SQS requests cost next to nothing, so it is read often;
#: triggers change rarely (queue-aws.ts ATTRS_TTL_MS, SETTINGS_TTL_MS, METRICS_TTL_MS)
ATTRS_TTL_MS = 15_000
SETTINGS_TTL_MS = 5 * 60_000
METRICS_TTL_MS = 60_000
CLIENTS_TTL_MS = 10 * 60_000
MAX_ENTRIES = 5000
#: reads made at once, per page
WORKERS = 8
#: how long a page waits on a read another page started before giving up on it: longer than one AWS call can take
#: (aws_reads: 2 attempts of 3 s to connect and 5 s to read, and the retry's back-off), so a waiter normally gets that
#: read's answer, but a read that hangs anyway can't hold every viewer's request with it (QV-742)
WAIT_S = 2 * (aws_reads.CONNECT_TIMEOUT_S + aws_reads.READ_TIMEOUT_S) + 4

OFF = "AWS reads are off on this server, so the queue's settings and depth are not read. An admin turns them on in " \
      "Settings, under AWS."
NOT_NAMED = "This queue is not in the AWS settings' list of queues, so its settings and depth are not read. An admin " \
            "adds it in Settings, under AWS."
NO_BOTO3 = 'AWS reads are on, but this server does not have boto3, so nothing is read. Install it with pip install ' \
           '"trailbeam[aws]".'
NO_CREDENTIALS = "No AWS credentials were found. Give the server credentials the usual way: environment variables, " \
                 "a profile, or an instance or task role."
NO_REGION = "No AWS region is known for this queue. Set one in Settings, under AWS, or AWS_REGION in the server's " \
            "environment."
NOT_A_NAME = "This is not an SQS queue name, so nothing is read for it."
def other_account_metrics(queue_account: str, caller: str) -> str:
    return (f"CloudWatch is not read for this queue: it is in AWS account {queue_account}, and the server's credentials "
            f"are in account {caller}, where CloudWatch would give the figures of a queue with the same name. The "
            "depth history, the age of the oldest message and the dead-letter queue's depth need credentials in "
            f"account {queue_account}.")


def metrics_unconfirmed(e: BaseException) -> str:
    return (f"CloudWatch is not read for this queue: the credentials' AWS account could not be found "
            f"(sts:GetCallerIdentity failed with {aws_reads.error_name(e)}), so it is not known whether CloudWatch "
            "would give this queue's figures or another account's.")


NOT_SEEN = "Trailbeam has no spans for this queue, so AWS is not read for it."


def other_account(queue_account: str, settings_account: str) -> str:
    return (f"This queue is in AWS account {queue_account}, and the AWS settings name account {settings_account}, so "
            "it is not read.")


def other_region(queue_region: str, allowed: list) -> str:
    return (f"This queue is in {queue_region}, and the AWS settings allow {', '.join(allowed) or 'no region'}, so "
            "it is not read. An admin adds the region in Settings, under AWS.")


def no_caller_account(e: BaseException) -> str:
    n = aws_reads.error_name(e)
    if n in ("NoCredentialsError", "PartialCredentialsError", "CredentialRetrievalError"):
        return NO_CREDENTIALS
    return (f"The credentials' AWS account could not be found (sts:GetCallerIdentity failed with {n}), so nothing is "
            "read. Set the AWS account ID in Settings, under AWS.")


def failed_text(call: str, name: Optional[str], e: BaseException) -> str:
    if isinstance(e, WaitTimeout):
        on = f" on {name}" if name else ""
        return f"{call}{on} did not answer within {e.wait_s:g} s, so nothing is shown from it this time."
    n = aws_reads.error_name(e)
    if n in ("NoCredentialsError", "PartialCredentialsError", "CredentialRetrievalError"):
        return NO_CREDENTIALS
    return f"{call} failed on {name} with {n}." if name else f"{call} failed with {n}."


# ── the short-lived cache ────────────────────────────────────────────────────────────────────────────────────────

class WaitTimeout(TimeoutError):
    """A read another request started did not finish within the wait (Cache.get's ``wait_s``)."""

    def __init__(self, key: str, wait_s: float):
        super().__init__(f"{key} was still being read after {wait_s:g} s")
        self.wait_s = wait_s


class _Entry:
    def __init__(self, until: float):
        self.until = until
        self.done = threading.Event()
        self.value: Any = None
        self.error: Optional[BaseException] = None


class Cache:
    """Values by key until a time (ms, the caller's clock). A miss is read once: callers asking for the same key
    meanwhile wait for that read, ``wait_s`` at most, then raise WaitTimeout. A failed read is not kept, and every
    caller waiting on it gets its error."""

    def __init__(self, max_entries: int = MAX_ENTRIES, wait_s: float = WAIT_S):
        self.max_entries = max_entries
        self.wait_s = wait_s
        self._d: dict[str, _Entry] = {}
        self._lock = threading.Lock()

    def get(self, key: str, ttl_ms: float, now: float, make: Callable[[], Any]) -> Any:
        with self._lock:
            e = self._d.get(key)
            mine = e is None or e.until <= now
            if mine:
                e = self._d[key] = _Entry(now + ttl_ms)
                if len(self._d) > self.max_entries:
                    for k in [k for k, x in self._d.items() if x.until <= now]:
                        del self._d[k]
        if not mine:
            if not e.done.wait(self.wait_s):
                raise WaitTimeout(key, self.wait_s)
            if e.error is not None:
                raise e.error
            return e.value
        try:
            e.value = make()
        except BaseException as err:
            e.error = err
            with self._lock:
                if self._d.get(key) is e:
                    del self._d[key]
            raise
        finally:
            e.done.set()
        return e.value

    def clear(self) -> None:
        with self._lock:
            self._d.clear()

    def __len__(self) -> int:
        return len(self._d)


# ── reading ──────────────────────────────────────────────────────────────────────────────────────────────────────

def queue_url(name: str, region: str, account: str) -> str:
    """The queue's URL, built from what the settings allow: never a URL from spans (QV-740). An SQS emulator is
    reached through AWS_ENDPOINT_URL_SQS (aws_reads.sqs_endpoint), whatever host the URL names."""
    domain = "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"
    return f"https://sqs.{region}.{domain}/{account}/{name}"


class AwsReader:
    """Reads what AWS says about queues, with the settings ``settings()`` gives (aws_settings.AwsSettings.current)
    and clients from ``clients_for(region)`` (aws_reads.clients by default: boto3's credentials)."""

    def __init__(self, settings: Callable[[], dict], clients_for: Optional[Callable[[Optional[str]], dict]] = None,
                 installed: Callable[[], bool] = aws_settings.boto3_installed, cache: Optional[Cache] = None):
        self.settings = settings
        self.clients_for = clients_for or (lambda region: aws_reads.clients(region))
        self.installed = installed
        self.cache = cache if cache is not None else Cache()  # an empty Cache is falsy (__len__)

    def clear(self, *_: Any) -> None:
        """Drops every kept read and client (the settings changed)."""
        self.cache.clear()

    def on(self) -> bool:
        """Reads are on; False, never raising, when the settings can't be read."""
        try:
            return aws_settings.is_on(self.settings())
        except Exception:  # noqa: BLE001
            return False

    def read(self, queues: list, windows: list, now: float, *, triggers: bool = True,
             metrics: Optional[list] = None, dlq_depth: str = "cloudwatch",
             attrs_ttl_ms: Optional[float] = None) -> dict:
        """What AWS says about each queue, by its key. ``queues``: ``{key, name, account, region, url}`` (account,
        region and url from its spans; None when not known; the url is never sent to AWS). ``windows``: ``{id,
        fromMs, toMs, periodS}``. ``metrics``: the CloudWatch metrics to read for the queues (af.METRIC_NAMES), all
        when None. ``dlq_depth``: "cloudwatch" (with history), or "sqs" (the dead-letter queue's own
        GetQueueAttributes, falling back to CloudWatch). ``attrs_ttl_ms``: how long a queue's GetQueueAttributes is
        kept, ATTRS_TTL_MS by default. Never raises."""
        out: dict = {}
        try:
            s = self.settings()
        except Exception as e:  # noqa: BLE001 - the store is unreachable: no figures, said so
            for q in queues:
                out[q["key"]] = af.empty_aws("failed", f"The AWS settings could not be read ({type(e).__name__}).")
            return out
        if not aws_settings.is_on(s):
            for q in queues:
                out[q["key"]] = af.empty_aws("not_configured", OFF)
            return out
        by_region: dict = {}
        for q in queues:
            account = q.get("account") or s.get("accountId")
            place = {"accountId": account, "region": q.get("region") or s.get("region")}
            if not aws_settings.is_queue_name(q.get("name")):
                out[q["key"]] = af.empty_aws("no_account", NOT_A_NAME, place)
            elif not any(af.name_matches(p, q["name"]) for p in s.get("queues") or []):
                out[q["key"]] = af.empty_aws("no_account", NOT_NAMED, place)
            elif q.get("account") and s.get("accountId") and q["account"] != s["accountId"]:
                out[q["key"]] = af.empty_aws("no_account", other_account(q["account"], s["accountId"]), place)
            elif not self.installed():
                out[q["key"]] = af.empty_aws("not_configured", NO_BOTO3, place)
            else:
                by_region.setdefault(place["region"], []).append(q)
        if not by_region:
            return out
        others = list(s.get("regions") or [])
        if s.get("region"):
            allowed = [s["region"], *others]
        else:  # boto3's default region is the home region: its clients say which
            try:
                home = self._clients(None, now).get("region")
            except Exception:  # noqa: BLE001 - no default region: queues without a region say so below
                home = None
            allowed = [home, *others] if home else others
        for region in [r for r in by_region if r is not None and r not in allowed]:
            for q in by_region.pop(region):
                out[q["key"]] = af.empty_aws("no_account", other_region(region, allowed),
                                             {"accountId": q.get("account") or s.get("accountId"), "region": region})
        if not by_region:
            return out
        with cf.ThreadPoolExecutor(WORKERS, thread_name_prefix="trailbeam-aws") as pool:
            for region, qs in by_region.items():
                self._region(pool, out, s, region, qs, windows, now, triggers, metrics, dlq_depth, attrs_ttl_ms)
        return out

    def _clients(self, region: Optional[str], now: float) -> dict:
        return self.cache.get(f"clients|{region or ''}", CLIENTS_TTL_MS, now, lambda: self.clients_for(region))

    def _region(self, pool, out, s, region, qs, windows, now, triggers, metrics, dlq_depth, attrs_ttl_ms) -> None:
        def place(q: dict, r: Optional[str]) -> dict:
            return {"accountId": q.get("account") or s.get("accountId"), "region": r}

        try:
            clients = self._clients(region, now)
        except Exception as e:  # noqa: BLE001
            text = NO_REGION if aws_reads.error_name(e) == "NoRegionError" else \
                failed_text("Making the AWS clients", None, e)
            for q in qs:
                out[q["key"]] = af.empty_aws("failed", text, place(q, region))
            return
        region = clients.get("region") or region
        if not region:
            for q in qs:
                out[q["key"]] = af.empty_aws("failed", NO_REGION, place(q, None))
            return
        base = region

        # the one account queues are read in: the settings', else the credentials' own (kept as the clients are)
        account = s.get("accountId")
        if not account:
            try:
                account = self.cache.get("account", CLIENTS_TTL_MS, now,
                                         lambda: aws_reads.caller_account(clients["sts"]))
            except Exception as e:  # noqa: BLE001
                for q in qs:
                    out[q["key"]] = af.empty_aws("failed", no_caller_account(e), place(q, region))
                return

        def place(q: dict, r: Optional[str]) -> dict:  # noqa: F811 - from here on the account is known
            return {"accountId": account, "region": r}

        # CloudWatch answers for the credentials' account only (QV-741). A failed lookup is kept a minute, so a server
        # that can't reach STS doesn't wait on it at every page load.
        cw_off: Optional[str] = None
        if s.get("accountId"):
            def caller() -> dict:
                try:
                    return {"account": aws_reads.caller_account(clients["sts"])}
                except Exception as e:  # noqa: BLE001
                    return {"error": e}
            c = self.cache.get("caller", METRICS_TTL_MS, now, caller)
            if c.get("error") is not None:
                cw_off = metrics_unconfirmed(c["error"])
            elif c["account"] != account:
                cw_off = other_account_metrics(account, c["account"])

        # settings first: the dead-letter queue's name, for its depth, is in the redrive policy
        urls: dict = {}
        for q in qs:
            if q.get("account") and q["account"] != account:
                out[q["key"]] = af.empty_aws("no_account", other_account(q["account"], account),
                                             {"accountId": q["account"], "region": region})
            else:
                urls[q["key"]] = queue_url(q["name"], region, account)
        qs = [q for q in qs if q["key"] in urls]

        def attrs(url: str, ttl: Optional[float]) -> dict:
            # kept apart from the short-lived reads, so a page never gets counts minutes old from it
            key = f"attrs|{base}|{url}" if ttl is None else f"attrs{int(ttl)}|{base}|{url}"
            return self.cache.get(key, ATTRS_TTL_MS if ttl is None else ttl, now,
                                  lambda: aws_reads.read_queue_settings(clients["sqs"], url))

        got = {k: pool.submit(attrs, u, attrs_ttl_ms) for k, u in urls.items()}
        settings: dict = {}
        for q in qs:
            f = got.get(q["key"])
            if f is None:
                continue
            try:
                settings[q["key"]] = f.result()
            except Exception as e:  # noqa: BLE001
                out[q["key"]] = af.empty_aws("failed", failed_text("sqs:GetQueueAttributes", q["name"], e),
                                             place(q, region))
        ok = [q for q in qs if q["key"] in settings]
        for q in ok:
            out[q["key"]] = {**af.empty_aws("ok", None, place(q, region)), "readAt": now,
                             "settings": settings[q["key"]]}

        # the dead-letter queues' depth from SQS itself: current, and an SQS request instead of a billed metric
        dlq_now: dict = {}
        if dlq_depth == "sqs":
            arns = sorted({settings[q["key"]]["dlqArn"] for q in ok if settings[q["key"]].get("dlqArn")})

            def dlq(arn: str) -> None:
                p = af.parse_queue_arn(arn)
                # SQS keeps a dead-letter queue in its source's account and region; any other is not read
                if not p or p.get("account") != account or (p.get("region") or region) != region \
                        or not aws_settings.is_queue_name(p["name"]):
                    return
                url = queue_url(p["name"], region, account)
                try:
                    d = attrs(url, None)
                except Exception:  # noqa: BLE001 - the credentials may not reach the DLQ: CloudWatch has its depth
                    return
                if d.get("visible") is not None:
                    dlq_now[arn] = {"visible": d["visible"], "at": now}

            list(pool.map(dlq, arns))
            for q in ok:
                d = settings[q["key"]].get("dlqArn")
                out[q["key"]]["dlqNow"] = dlq_now.get(d) if d else None

        if cw_off is not None:
            for q in ok:
                out[q["key"]]["errors"].append(cw_off)

        # one GetMetricData per window for every queue of this region and their dead-letter queues
        names = sorted({q["name"] for q in ok}) if cw_off is None else []
        dlqs = sorted({settings[q["key"]]["dlqName"] for q in ok
                       if settings[q["key"]].get("dlqName") and settings[q["key"]].get("dlqArn") not in dlq_now})
        ms = sorted(set(metrics)) if metrics is not None else None

        def window(w: dict) -> None:
            if not names or (ms is not None and not ms and not dlqs):
                return
            step = w["periodS"] * 1000
            # aligned to the period, so the same request is made, and kept, across page loads in the same minute
            from_ms = (w["fromMs"] // step) * step
            to_ms = -((-w["toMs"]) // step) * step
            key = (f"metrics|{account}|{base}|{w['periodS']}|{from_ms}|{to_ms}|{','.join(names)}|{','.join(dlqs)}|"
                   f"{','.join(ms) if ms is not None else 'all'}")
            try:
                r = self.cache.get(key, METRICS_TTL_MS, now, lambda: aws_reads.read_queue_metrics(
                    clients["cloudwatch"], names, dlqs, from_ms, to_ms, w["periodS"], ms))
            except Exception as e:  # noqa: BLE001
                for q in ok:
                    out[q["key"]]["errors"].append(failed_text("cloudwatch:GetMetricData", None, e))
                return
            for q in ok:
                d = settings[q["key"]].get("dlqName")
                out[q["key"]]["series"][w["id"]] = {
                    "periodS": w["periodS"], "fromMs": from_ms, "toMs": to_ms,
                    "queue": r["queues"].get(q["name"]) or af.empty_series(),
                    "dlq": (r["dlqs"].get(d) or []) if d else []}

        list(pool.map(window, windows))

        if triggers:
            def esm(q: dict) -> None:
                arn = settings[q["key"]].get("arn")
                if not arn:
                    return
                try:
                    out[q["key"]]["triggers"] = self.cache.get(
                        f"esm|{base}|{arn}", SETTINGS_TTL_MS, now,
                        lambda: aws_reads.read_triggers(clients["lambda"], arn))
                except Exception as e:  # noqa: BLE001
                    out[q["key"]]["errors"].append(failed_text("lambda:ListEventSourceMappings", None, e))

            list(pool.map(esm, ok))


# ── the server's AWS status (the settings screen, /api/health, trailbeam doctor) ─────────────────────────────────

def credentials_source(region: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
    """(where boto3 finds credentials, the region it uses): ("env", "eu-west-1"), ("iam-role", None), ...; None for
    the source when it finds none. No AWS API call is made, though an instance role is looked up on the instance's
    metadata address. Call only with reads on."""
    import boto3  # the caller checked it is installed

    session = boto3.session.Session(region_name=region or None)
    creds = session.get_credentials()
    return (getattr(creds, "method", None) or "found") if creds is not None else None, session.region_name


class Status:
    """What the settings screen, /api/health and trailbeam doctor say about AWS reads, kept ``keep_ms``: finding the
    credentials can mean a request to the instance's metadata address."""

    KEEP_MS = 60_000

    def __init__(self, settings: Callable[[], dict], installed: Callable[[], bool] = aws_settings.boto3_installed,
                 source: Callable[[Optional[str]], tuple] = credentials_source, keep_ms: float = KEEP_MS):
        self.settings, self.installed, self.source, self.keep_ms = settings, installed, source, keep_ms
        self._lock = threading.Lock()
        self._kept: Optional[tuple[float, tuple, dict]] = None

    def clear(self, *_: Any) -> None:
        with self._lock:
            self._kept = None

    def get(self, now_ms: float) -> dict:
        """``{on, queues, boto3, credentials, region, problem}``. ``on``: reads are on. ``credentials``: where boto3
        found them, None when it found none or was not asked (reads off). ``problem``: a sentence when reads are on
        but can't work, else None."""
        try:
            s = self.settings()
        except Exception as e:  # noqa: BLE001
            return {"on": False, "queues": 0, "boto3": None, "credentials": None, "region": None,
                    "problem": f"The AWS settings could not be read ({type(e).__name__})."}
        installed = self.installed()
        base = {"on": aws_settings.is_on(s), "queues": len(s.get("queues") or []), "boto3": installed,
                "credentials": None, "region": s.get("region"), "problem": None}
        if not base["on"]:
            return base
        if not installed:
            return {**base, "problem": NO_BOTO3}
        key = (s.get("region"),)
        with self._lock:
            if self._kept is not None and self._kept[1] == key and now_ms - self._kept[0] < self.keep_ms:
                return {**base, **self._kept[2]}
        try:
            source, region = self.source(s.get("region"))
            # no region is not a problem: a queue's URL names its own, and only a queue without one needs it
            found = {"credentials": source, "region": region, "problem": NO_CREDENTIALS if source is None else None}
        except Exception as e:  # noqa: BLE001
            found = {"credentials": None, "problem": f"Looking for AWS credentials failed with "
                                                     f"{aws_reads.error_name(e)}."}
        with self._lock:
            self._kept = (now_ms, key, found)
        return {**base, **found}
