"""Triage on a cause (QV-593, open source; the hosted app's QV-393 design): the rules, as pure functions.

Who has a cause, and whether it is open, resolved or ignored. A cause is worked out at read time and never stored
(causes.py), so its triage is kept apart from it, keyed by the cause's key, and joined when a page is read. This
module is the rules only, the same as @trailbeam/ui/model's triage.ts and arrivals.ts, which the hosted app and the
shell use: the state a stored row is in now (``triage_of``), which row a cause shows across the merge and split of
error texts (``row_for``), the list's filters, and what a note may hold. The store is triage_store.py.

Rows and results are dicts with the TypeScript names (``causeKey``, ``sinceSeq``, ...), so they go to the browser as
they are. Times are milliseconds on the server's clock. ``seq`` is a span's arrival on the server (ms * 1000 plus a
counter); a span with no seq (``None``, stored before the server kept it) never counts as arriving after anything.

contract/fixtures/triage-parity.json holds the cases both engines must agree on (make-triage-parity.py writes it from
this module; python/tests/server/test_triage.py and @trailbeam/ui's test/triage.test.ts run it, and the hosted app's
test reads it from @trailbeam/ui/fixtures).
"""
from __future__ import annotations

import math
import re
import unicodedata
from typing import Any, Optional

STATUSES = ("open", "resolved", "ignored")
#: the ignore lengths the menu offers, in seconds
IGNORE_FOR_S = (3600, 86_400, 7 * 86_400)
#: "ignore until N more messages"
IGNORE_MORE = (10, 100)
NOTE_MAX = 2000
NOTE_COUNT_FROM = 1800
MAX_NOTES = 500
MAX_TRIAGE_ROWS = 5000
#: how long after an action its Undo still works on the server
UNDO_MS = 60_000
#: how long before a resolve or ignore a span's own time may be and still count: room for hosts' clock drift
LATE_EXPORT_MS = 10 * 60_000

FILTERS = ("open", "mine", "unassigned", "resolved", "ignored", "all")


# ---- arrivals -------------------------------------------------------------------------------------------------

def _num(v) -> float:
    """A JSON number, with None as NaN (JSON has no NaN)."""
    return math.nan if v is None else float(v)


def seq_ms(seq: float) -> int:
    """Seq units (ms * 1000) as ms on the server's clock."""
    return math.floor(seq / 1000)


def new_since(arrivals: list, since_seq: float, at_ms: float, except_versions: Optional[set] = None) -> list:
    """The arrivals (``{mid, seq, ms, version}``) whose deciding span arrived after the watermark ``since_seq``, oldest
    arrival first. A span whose own time is more than LATE_EXPORT_MS before ``at_ms`` (the resolve or ignore) is a
    late export of an earlier failure and does not count; nor does one on a version in ``except_versions``."""
    not_before = at_ms - LATE_EXPORT_MS

    def counts(a: dict) -> bool:
        seq, ms = _num(a.get("seq")), _num(a.get("ms"))
        if not seq > since_seq:  # NaN: never
            return False
        if ms < not_before:
            return False
        v = a.get("version")
        return not (except_versions and v is not None and v in except_versions)

    return sorted((a for a in arrivals if counts(a)), key=lambda a: _num(a.get("seq")))


def failures_after(row: dict, arrivals: Optional[list]) -> list:
    """The failures after the row's resolve or ignore: seq past sinceSeq, not a late export, and not on a version the
    resolve excused ("resolved in a new version")."""
    if row.get("sinceSeq") is None or not arrivals:
        return []
    versions = row.get("resolveVersions") or []
    except_versions = set(versions) if row.get("resolveMode") == "next_version" and versions else None
    at = row["statusAt"] if row.get("statusAt") is not None else seq_ms(row["sinceSeq"])
    return new_since(arrivals, row["sinceSeq"], at, None if row.get("status") == "ignored" else except_versions)


# ---- the state a row is in now --------------------------------------------------------------------------------

OPEN: dict = {"status": "open", "mode": None, "versions": [], "until": None, "more": None, "seen": None, "ended": None,
              "regressed": False, "regressedAt": None, "regressedVersion": None, "regressedCount": None,
              "regression": None, "assignee": None, "statusBy": None, "statusAt": None, "notes": 0, "gone": False}


def triage_of(row: Optional[dict], group: Optional[dict], now_ms: int) -> dict:
    """The state a cause is in now: the stored row, with an ignore whose time is up or whose count is reached read as
    open, and a resolved cause with a failure after the resolve read as open and regressed. ``ended`` and
    ``regression`` are what the read found and has not stored yet: the store writes each once (triage_store.settle).
    With no row the cause is open and nobody has it. ``group`` is the cause as read now (its ``arrivals``), or None
    when it has no unfinished messages."""
    if not row:
        return {**OPEN, "versions": [], "gone": not group}
    status = row["status"]
    base = {
        "status": status,
        "mode": (row.get("resolveMode") or "now") if status == "resolved" else None,
        "versions": list(row.get("resolveVersions") or []) if status == "resolved" else [],
        "until": row.get("ignoreUntil") if status == "ignored" else None,
        "more": row.get("ignoreMore") if status == "ignored" else None,
        "seen": (row.get("ignoreSeen") or 0) if status == "ignored" and row.get("ignoreMore") is not None else None,
        "ended": None,
        "regressed": row.get("regressedAt") is not None,
        "regressedAt": row.get("regressedAt"), "regressedVersion": row.get("regressedVersion"),
        "regressedCount": row.get("regressedCount"), "regression": None,
        "assignee": row.get("assignee"), "statusBy": row.get("statusBy"), "statusAt": row.get("statusAt"),
        "notes": row.get("noteCount") or 0, "gone": not group,
    }
    fresh = failures_after(row, (group or {}).get("arrivals"))
    if status == "ignored":
        until = row.get("ignoreUntil")
        if until is not None and until <= now_ms:
            return {**base, "status": "open", "until": None, "more": None, "seen": None,
                    "ended": {"why": "time", "at": until, "by": row.get("statusBy")}, "statusBy": None, "statusAt": until}
        more = row.get("ignoreMore")
        if more is not None:
            # messages, not deliveries: one arrival per message. Never fewer than a read already counted
            seen = max(row.get("ignoreSeen") or 0, len(fresh))
            if seen >= more:
                at = seq_ms(_num(fresh[more - 1]["seq"])) if len(fresh) >= more else now_ms
                return {**base, "status": "open", "until": None, "more": None, "seen": None,
                        "ended": {"why": "count", "at": at, "more": more, "by": row.get("statusBy")},
                        "statusBy": None, "statusAt": at}
            return {**base, "seen": seen}
        return base
    if status == "resolved" and row.get("regressedAt") is None and fresh:
        first = fresh[0]
        regression = {"at": max(seq_ms(_num(first["seq"])), row.get("statusAt") or 0),
                      "version": first.get("version"), "count": len(fresh)}
        return {**base, "status": "open", "mode": None, "versions": [], "regressed": True,
                "regressedAt": regression["at"], "regressedVersion": regression["version"],
                "regressedCount": regression["count"], "regression": regression}
    # regressed: the count of new failures keeps growing until the next status change
    if row.get("regressedAt") is not None and len(fresh) > (row.get("regressedCount") or 0):
        return {**base, "regressedCount": len(fresh)}
    return base


# ---- which row a cause shows ----------------------------------------------------------------------------------

def same_cause(r: str, g: str) -> bool:
    """Whether a group with key g is the cause with key r, across the merge of error texts ("...|exc:<class>:*")."""
    if r == g:
        return True
    if g.endswith(":*") and "|exc:" in g and r.startswith(g[:-1]):
        return True
    if r.endswith(":*") and "|exc:" in r and g.startswith(r[:-1]):
        return True
    return False


def is_merged(key: str) -> bool:
    """The key of a merged cause: all of one exception class's error texts, "...|exc:<class>:*"."""
    return key.endswith(":*") and "|exc:" in key


def _newest(rows: list) -> Optional[dict]:
    """The latest change first, then by key (code units, as JavaScript compares): the same pick on every read."""
    if not rows:
        return None
    return sorted(rows, key=lambda r: (-r["updatedAt"], _js_key(r["causeKey"])))[0]


def _js_key(s: str) -> bytes:
    # JavaScript's < on strings compares UTF-16 code units
    return s.encode("utf-16-be")


def row_for(rows: list, key: str) -> Optional[dict]:
    """The row the cause with this key shows (the rules in @trailbeam/ui's triage.ts rowFor): its own row, or for a
    split cause its class's merged row when that one changed later or it has none, or for a merged cause with no row
    of its own the most recently changed row of its texts."""
    own = next((r for r in rows if r["causeKey"] == key), None)
    if is_merged(key):
        return own or _newest([r for r in rows if not is_merged(r["causeKey"]) and same_cause(r["causeKey"], key)])
    merged = _newest([r for r in rows if is_merged(r["causeKey"]) and same_cause(r["causeKey"], key)])
    if not merged or not own:
        return own or merged
    return merged if merged["updatedAt"] > own["updatedAt"] else own


# ---- the list's filters ---------------------------------------------------------------------------------------

def filter_of(raw: Optional[str]) -> str:
    """?status= as a filter; anything else is the default, open."""
    return raw if raw in FILTERS else "open"


def in_filter(t: dict, f: str, me: str, members: set) -> bool:
    """Whether a cause is in a filter. "mine" and "unassigned" are of the open causes. An assignee who is no longer
    a member counts as nobody."""
    who = t.get("assignee") if t.get("assignee") and t["assignee"] in members else None
    status = t.get("status")
    if f == "all":
        return True
    if f in ("resolved", "ignored", "open"):
        return status == f
    if f == "mine":
        return status == "open" and who == me
    if f == "unassigned":
        return status == "open" and who is None
    return False


# ---- notes ----------------------------------------------------------------------------------------------------

#: control characters other than tab and newline, and the bidi embeddings, overrides and isolates (QV-557)
_NOTE_BAD = re.compile("[\u0000-\u0008\u000b-\u001f\u007f-\u009f‪-‮⁦-⁩]")
# JavaScript's \s: WhiteSpace and LineTerminator (Zs is checked by category)
_JS_SPACE = set("\t\n\u000b\u000c\r  ﻿  ")
_INVISIBLE = set("͏ᅟᅠ឴឵᠎⠀ㅤﾠ") | {chr(c) for c in range(0xFE00, 0xFE10)}


def _invisible(ch: str) -> bool:
    return ch in _JS_SPACE or ch in _INVISIBLE or unicodedata.category(ch) in ("Cf", "Zs")


def note_blank(body: str) -> bool:
    """True when a note would show nothing: empty, or only spaces and invisible characters."""
    return all(_invisible(ch) for ch in body)


def js_length(s: str) -> int:
    """A string's length as JavaScript counts it: UTF-16 code units."""
    return len(s.encode("utf-16-le")) // 2


def note_problem(body: str) -> Optional[str]:
    """What is wrong with a note's text, or None."""
    if note_blank(body):
        return "Write the note first."
    if _NOTE_BAD.search(body):
        return ("The note has hidden characters that can't be saved (control characters or text-direction marks), "
                "often from pasted text. Retype the pasted part and try again.")
    if js_length(body) > NOTE_MAX:
        return "Notes can be up to 2,000 characters."
    return None


def public(t: Any) -> Any:
    """A result as JSON: NaN as null, and whole floats as integers, as JSON.stringify writes them."""
    if isinstance(t, float):
        return None if math.isnan(t) else (int(t) if t.is_integer() else t)
    if isinstance(t, list):
        return [public(e) for e in t]
    if isinstance(t, dict):
        return {k: public(v) for k, v in t.items()}
    return t
