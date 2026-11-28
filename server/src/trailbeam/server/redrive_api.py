"""``/api/redrive`` (QV-590): redrive's API, for the shared Redrive views (@trailbeam/ui ui/src/redrive) on the message
page and on Stuck. The hosted app's redrive server actions (trailbeam-app lib/app/actions.ts) over HTTP.

- ``GET /api/redrive``: ``{"on", "reason", "canRedrive", "log"}``. ``on``: redrive is available on this server (AWS
  on, boto3 installed, at least one dead-letter queue named in the settings' ``redrive``), with ``reason`` the
  sentence why not. ``canRedrive``: the person may start and cancel one (an admin; anyone, before accounts exist).
  ``log``: the latest 50 entries (redrive_log.py), newest first. The moves still open are read from SQS first, at
  most every REFRESH_MS (redrive.next_check).
- ``POST /api/redrive/gates`` ``{"dlqs": [{name, account, region, messageId}]}``: ``{"gates": [...]}``, each
  ``{"ok": true, "dlq"}`` or ``{"ok": false, "code", "reason", "dlq"}`` (redrive.gate). ``messageId``: a message in
  the queue, whose spans' queue URL gives the account and region when they aren't sent (a message's timeline does
  not carry them); ``dlq`` is the place the gate used, to send with preview and start. Makes no AWS call.
- ``POST /api/redrive/preview`` ``{"dlq"}``: what a redrive would move, ``{"preview": {visible, inFlight,
  running}}``. Reads only.
- ``POST /api/redrive/start`` ``{"dlq", "messageIds"}``: starts the move, after the person confirmed it, and writes
  the log entry, whatever SQS answered. ``{"entry"}``.
- ``POST /api/redrive/cancel`` ``{"id"}``: asks SQS to cancel a running move, and records who asked on the entry.
  ``{"entry"}``.

Every write goes through the gate again on the server: the button being on in a browser proves nothing. A gate that
is shut is a 403 (not an admin) or a 409, and what AWS refused is a 409, each with its sentence in ``{"error"}``. The
routes are always mounted; with AWS off they make no AWS call and import no boto3.
"""
from __future__ import annotations

import re
import threading
from typing import Any, Callable, Optional

from . import aws_reader, aws_settings, redrive, redrive_log
from .causes import parse_queue_url
from .alerts_api import ApiProblem, check_text

PREFIX = "/api/redrive"
GATES = "/api/redrive/gates"
PREVIEW = "/api/redrive/preview"
START = "/api/redrive/start"
CANCEL = "/api/redrive/cancel"
#: the moves still open are read from SQS at most this often, however often the pages ask
REFRESH_MS = 5_000
LOG_LIMIT = 50
MAX_DLQS = 100
#: the messages picked, kept on the entry (a request body is at most 16 KiB)
MAX_IDS = 200
_ID = re.compile(r"^[0-9a-f]{32}$")


def is_redrive_path(path: str) -> bool:
    return path in (PREFIX, GATES, PREVIEW, START, CANCEL)


def answers(path: str, method: str) -> bool:
    """One of the routes, with its method: anything else is a 404 before the body is read (server.App.mounted)."""
    return method == "GET" if path == PREFIX else method == "POST" and is_redrive_path(path)


def _by(who) -> str:
    return getattr(who, "name", None) or getattr(who, "email", None) or getattr(who, "via", None) or "none"


def _place(v: Any, field: str = "dlq") -> dict:
    """A dead-letter queue as sent: ``{name, account, region}``, account and region optional."""
    if not isinstance(v, dict) or not isinstance(v.get("name"), str) or not v["name"].strip():
        raise ApiProblem(400, f"{field} is an object with the dead-letter queue's name, and its account and region "
                              "when known.")
    out = {"name": v["name"].strip()[:80]}
    for k in ("account", "region"):
        x = v.get(k)
        if x is not None and not isinstance(x, str):
            raise ApiProblem(400, f"{field}.{k} is a string or null.")
        out[k] = (x or "").strip()[:40] or None
    return out


class ServerRedrive:
    """Redrive on one server: the AWS parts (aws_api.ServerAws, for the settings and the cached clients), the log,
    and who may redrive."""

    def __init__(self, aws, log, is_admin: Callable[[object], bool], now_ms: Callable[[], int],
                 installed: Callable[[], bool] = aws_settings.boto3_installed,
                 place_of: Callable[[str, str], Optional[dict]] = lambda message_id, queue: None):
        self.aws, self.log, self.is_admin, self.now_ms, self.installed = aws, log, is_admin, now_ms, installed
        #: (message ID, its dead-letter queue) -> {account, region} from its spans' queue URLs, or None
        self.place_of = place_of
        self._refreshed_at: Optional[float] = None
        self._refresh_lock = threading.Lock()

    # ── what the pages read ──────────────────────────────────────────────────────────────────────────────────────

    def _settings(self) -> dict:
        return self.aws.settings.current()  # a store that can't be read is a 500, as on /api/settings/aws

    def available(self, s: dict) -> tuple[bool, Optional[str]]:
        if not aws_settings.is_on(s):
            return False, redrive.OFF
        if not self.installed():
            return False, redrive.NO_BOTO3
        if not s.get("redrive"):
            return False, ("Redrive is not turned on for any dead-letter queue. An admin names the dead-letter queues "
                           "to redrive in Settings, under AWS.")
        return True, None

    def gate(self, s: dict, dlq: dict, who) -> dict:
        return redrive.gate(s, dlq, installed=self.installed(), can_manage=self.is_admin(who))

    def _sqs(self, region: Optional[str]) -> tuple[Any, Optional[str]]:
        """boto3's SQS client for a region, from the AWS reader's cache, and the region it is in."""
        r = self.aws.reader
        clients = r.cache.get(f"clients|{region or ''}", aws_reader.CLIENTS_TTL_MS, self.now_ms(),
                              lambda: r.clients_for(region))
        return clients["sqs"], clients.get("region") or region

    def _where(self, g: dict, name: str) -> tuple[Any, str, str, str]:
        """(client, region, ARN, URL) for an open gate. Raises ApiProblem when no region is known."""
        try:
            sqs, region = self._sqs(g.get("region"))
        except Exception as e:  # noqa: BLE001
            raise ApiProblem(409, aws_reader.failed_text("Making the AWS client", None, e)) from None
        if not region:
            raise ApiProblem(409, aws_reader.NO_REGION)
        return sqs, region, redrive.dlq_arn(g["account"], region, name), redrive.dlq_url(g["account"], region, name)

    def refresh(self, force: bool = False) -> None:
        """Reads the status of the moves still open from SQS and keeps it in the log (the hosted refreshRedrives).
        A move SQS calls finished stays open until the dead-letter queue's depth is read again a minute later. A
        read that fails leaves the status as it was; the next page load tries again."""
        now = self.now_ms()
        with self._refresh_lock:
            if not force and self._refreshed_at is not None and now - self._refreshed_at < REFRESH_MS:
                return
            self._refreshed_at = now
        try:
            s = self.aws.settings.current()
        except Exception:  # noqa: BLE001
            return
        if not aws_settings.is_on(s) or not self.installed():
            return
        open_ = [e for e in self.log.list(LOG_LIMIT)
                 if e["outcome"] == "started" and redrive.next_check(e, now) in ("task", "depth")]
        for e in open_:
            try:
                sqs, _ = self._sqs(e["region"])
                t = redrive.move_task(sqs, e["dlqArn"], e["taskHandle"], e["requestedAt"])
                if t is None:
                    continue
                change = {"status": t["status"], "moved": t["moved"], "toMove": t["toMove"],
                          "failure": t["failure"] or e["failure"]}
                if t["status"] != e["status"]:
                    change["statusAt"] = now
                # the count SQS gives a finished move can still change, so it is read again with the depth
                elif redrive.next_check(e, now) == "depth":
                    change["left"] = redrive.queue_depth(sqs, redrive.dlq_url(e["accountId"], e["region"],
                                                                              e["dlqName"]))
                    change["leftAt"] = now
                self.log.set_status(e["id"], change)
            except Exception:  # noqa: BLE001
                continue

    def view(self, who) -> dict:
        s = self._settings()
        on, reason = self.available(s)
        if on:
            self.refresh()
        return {"on": on, "reason": reason, "canRedrive": bool(self.is_admin(who)), "log": self.log.list(LOG_LIMIT)}

    # ── the calls ────────────────────────────────────────────────────────────────────────────────────────────────

    def call(self, method: str, path: str, body: Optional[dict], who, base: Optional[str] = None) -> tuple[int, dict]:
        if not is_redrive_path(path):
            raise ApiProblem(404, f"no such endpoint: {method} {path}")
        if path == PREFIX:
            if method != "GET":
                raise ApiProblem(405, "GET /api/redrive")
            return 200, self.view(who)
        if method != "POST":
            raise ApiProblem(404, f"no such endpoint: {method} {path}")
        check_text(body)
        body = body if isinstance(body, dict) else {}
        if path == GATES:
            dlqs = body.get("dlqs")
            if not isinstance(dlqs, list) or len(dlqs) > MAX_DLQS:
                raise ApiProblem(400, f"dlqs is a list of at most {MAX_DLQS} dead-letter queues.")
            s = self._settings()
            gates = []
            for i, d in enumerate(dlqs):
                place = _place(d, f"dlqs[{i}]")
                mid = d.get("messageId")
                if isinstance(mid, str) and mid and not (place["account"] and place["region"]):
                    found = self.place_of(mid[:256], place["name"]) or {}
                    place = {**place, "account": place["account"] or found.get("account"),
                             "region": place["region"] or found.get("region")}
                g = self.gate(s, place, who)
                gates.append({"ok": True, "dlq": place} if g["ok"]
                             else {"ok": False, "code": g["code"], "reason": g["reason"], "dlq": place})
            return 200, {"gates": gates}
        if path == PREVIEW:
            return self._preview(_place(body.get("dlq")), who)
        if path == START:
            ids = body.get("messageIds", [])
            if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids) or len(ids) > MAX_IDS:
                raise ApiProblem(400, f"messageIds is a list of at most {MAX_IDS} message IDs.")
            return self._start(_place(body.get("dlq")), ids, who)
        if path == CANCEL:
            id_ = body.get("id")
            if not isinstance(id_, str) or not _ID.match(id_):
                raise ApiProblem(400, "id is the log entry's ID.")
            return self._cancel(id_, who)
        raise ApiProblem(404, f"no such endpoint: {method} {path}")

    def _open_gate(self, dlq: dict, who) -> dict:
        g = self.gate(self._settings(), dlq, who)
        if not g["ok"]:
            raise ApiProblem(403 if g["code"] == "not_manager" else 409, g["reason"])
        return g

    def _preview(self, dlq: dict, who) -> tuple[int, dict]:
        g = self._open_gate(dlq, who)
        sqs, _, arn, url = self._where(g, dlq["name"])
        r = redrive.preview(sqs, arn, url)
        if not r["ok"]:
            raise ApiProblem(409, r["reason"])
        return 200, {"preview": r["preview"], "dlqArn": arn}

    def _start(self, dlq: dict, ids: list, who) -> tuple[int, dict]:
        g = self._open_gate(dlq, who)
        sqs, region, arn, _ = self._where(g, dlq["name"])
        now = self.now_ms()
        base = {"dlqName": dlq["name"], "dlqArn": arn, "accountId": g["account"], "region": region,
                "messageIds": ids, "requestedBy": _by(who), "requestedByEmail": getattr(who, "email", None),
                "requestedAt": now}
        r = redrive.start(sqs, arn)
        if r["ok"]:
            id_ = self.log.add({**base, "outcome": "started", "taskHandle": r["handle"], "status": "RUNNING",
                                "statusAt": now})
            return 200, {"entry": self.log.get(id_)}
        self.log.add({**base, "outcome": "refused" if r["refused"] else "failed", "detail": r["reason"]})
        raise ApiProblem(409, r["reason"])

    def _cancel(self, id_: str, who) -> tuple[int, dict]:
        if not self.is_admin(who):
            raise ApiProblem(403, redrive.NOT_MANAGER)
        s = self._settings()
        e = self.log.get(id_)
        if e is None or not e.get("taskHandle") or e["outcome"] != "started":
            raise ApiProblem(404, "That redrive can't be cancelled from here.")
        if e["status"] not in (None, "RUNNING"):
            raise ApiProblem(409, "That redrive is not running any more.")
        g = self.gate(s, {"name": e["dlqName"], "account": e["accountId"], "region": e["region"]}, who)
        if not g["ok"]:
            raise ApiProblem(409, g["reason"])
        sqs, _, _, _ = self._where({**g, "region": e["region"]}, e["dlqName"])
        r = redrive.cancel(sqs, e["dlqArn"], e["taskHandle"])
        if not r["ok"]:
            raise ApiProblem(409, r["reason"])
        # SQS keeps moving after a cancel, so the count it answers with is not kept: refresh reads the final one
        self.log.set_status(id_, {"status": "CANCELLING", "failure": f"Cancel requested by {_by(who)}.",
                                  "statusAt": self.now_ms()})
        return 200, {"entry": self.log.get(id_)}


def for_app(app) -> ServerRedrive:
    """Redrive on server.App: its AWS parts and its store's redrive log (a test double's in memory)."""

    def is_admin(who) -> bool:
        return app.access.mode() != "accounts" or getattr(who, "role", None) == "admin"

    def place_of(message_id: str, queue: str) -> Optional[dict]:
        rows = [dict(x) for x in (app.db.messages_rows([message_id]) or {}).get(message_id) or []]
        # the dead-letter queue's own URL first; a source queue's shares its account and region
        for x in sorted(rows, key=lambda x: x.get("queue") != queue):
            p = parse_queue_url(x.get("queue_url"))
            if p and p.get("account"):
                return {"account": p["account"], "region": p.get("region")}
        return None

    return ServerRedrive(app.aws, redrive_log.tables_for(app.db), is_admin, now_ms=lambda: app.now() // 1_000_000,
                         place_of=place_of)
