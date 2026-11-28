"""Writes triage-parity.json: the triage rule cases both engines must agree on (QV-593).

Self-hosted Trailbeam (server/src/trailbeam/server/triage.py, python/tests/server/test_triage.py) and the shared
TypeScript rules (@trailbeam/ui/model triage.ts and arrivals.ts, run by ui/test/triage.test.ts and by the hosted app's
test/triage-parity.test.ts, reading @trailbeam/ui/fixtures/triage-parity.json) run every case and must give the
expected result: the state a stored row is in now (ignore until a time, ignore until N more messages, regression,
resolved in a new version, late exports), which row a cause shows across the merge and split of error texts, the
list's filters, what a note may hold, and the exception signature a cause key is built from (``causeSignature``: the
server's causes.exc_signature and the hosted app's causes.ts excSignature; @trailbeam/ui builds no keys). The expected results are the Python engine's. Run from the repository root
after changing a case or the rules:

    PYTHONPATH=python/src:server/src python contract/fixtures/make-triage-parity.py

JSON has no NaN: an arrival's ``seq`` or ``ms`` of null stands for NaN (no span seq, no evidence time).
"""
import json
from pathlib import Path

from trailbeam.server import causes, triage

OUT = Path(__file__).resolve().parent / "triage-parity.json"
MIN = 60_000
H = 3_600_000
T0 = 1_764_000_000_000  # 2025-11-24T16:00:00Z, ms
S0 = T0 * 1000           # T0 in seq units


def row(**kw):
    r = {"causeKey": "v1|orders|billing|exc:TimeoutError:ab12", "causeId": "c1", "label": "billing timed out",
         "queue": "orders", "status": "open", "resolveMode": None, "resolveVersions": [], "sinceSeq": None,
         "ignoreUntil": None, "ignoreMore": None, "ignoreSeen": None, "regressedAt": None, "regressedVersion": None,
         "regressedCount": None, "assignee": None, "assignedBy": None, "assignedAt": None, "statusBy": None,
         "statusAt": None, "noteCount": 0, "heldRule": 0, "createdAt": T0 - H, "updatedAt": T0 - H}
    r.update(kw)
    return r


def resolved(at=T0, **kw):
    return row(**{"status": "resolved", "resolveMode": "now", "sinceSeq": at * 1000, "statusBy": "ana", "statusAt": at,
                  "updatedAt": at, **kw})


def ignored(at=T0, **kw):
    return row(**{"status": "ignored", "sinceSeq": at * 1000, "statusBy": "ana", "statusAt": at, "updatedAt": at, **kw})


def arr(mid, after_ms, ms=None, version=None, seq=True):
    """An arrival after_ms after T0 on the server's clock; its own span time ms (default: the same instant)."""
    return {"mid": mid, "seq": (S0 + after_ms * 1000 + 1) if seq else None,
            "ms": (T0 + after_ms) if ms is None else ms, "version": version}


def group(*arrivals):
    return {"id": "c1", "arrivals": list(arrivals)}


TRIAGE_OF = [
    ("no row, a cause now: open, nobody", None, group(), T0),
    ("no row, no cause now: open and gone", None, None, T0),
    ("open with an assignee and notes", row(assignee="sam", assignedBy="ana", assignedAt=T0, noteCount=3), group(), T0),
    ("resolved, nothing since", resolved(), group(), T0 + H),
    ("resolved, no cause now", resolved(), None, T0 + H),
    ("resolved with no mode stored reads as now", resolved(resolveMode=None), group(), T0 + H),
    ("resolved, a failure from before the resolve", resolved(), group(arr("m1", -5 * MIN)), T0 + H),
    ("resolved, one failure after: regressed", resolved(), group(arr("m1", 5 * MIN, version="1.4.4")), T0 + H),
    ("resolved, three failures after: count 3, first's version", resolved(),
     group(arr("m3", 9 * MIN, version="1.4.5"), arr("m1", 2 * MIN, version="1.4.4"), arr("m2", 4 * MIN)), T0 + H),
    ("resolved, a failure with no seq never counts", resolved(), group(arr("m1", 5 * MIN, seq=False)), T0 + H),
    ("resolved, a late export (own time 11 min before): not counted", resolved(),
     group(arr("m1", 5 * MIN, ms=T0 - 11 * MIN)), T0 + H),
    ("resolved, own time 9 min before: counted", resolved(), group(arr("m1", 5 * MIN, ms=T0 - 9 * MIN)), T0 + H),
    ("resolved, own time exactly 10 min before: counted", resolved(), group(arr("m1", 5 * MIN, ms=T0 - 10 * MIN)), T0 + H),
    ("resolved, no evidence time: counted", resolved(), group({"mid": "m1", "seq": S0 + 1000, "ms": None, "version": None}), T0 + H),
    ("resolved, arrival at the watermark exactly: not after", resolved(),
     group({"mid": "m1", "seq": S0, "ms": T0, "version": None}), T0 + H),
    ("resolved in a new version: failures on the old versions don't count",
     resolved(resolveMode="next_version", resolveVersions=["1.4.3", "1.4.4"]),
     group(arr("m1", 5 * MIN, version="1.4.3"), arr("m2", 6 * MIN, version="1.4.4")), T0 + H),
    ("resolved in a new version: a failure on a new version regresses it",
     resolved(resolveMode="next_version", resolveVersions=["1.4.3"]),
     group(arr("m1", 5 * MIN, version="1.4.3"), arr("m2", 6 * MIN, version="1.4.5")), T0 + H),
    ("resolved in a new version: a failure with no version counts",
     resolved(resolveMode="next_version", resolveVersions=["1.4.3"]), group(arr("m1", 5 * MIN)), T0 + H),
    ("regression at: never before the resolve", resolved(at=T0 + 2000), group({"mid": "m1", "seq": S0 + 2_000_001 + 1000, "ms": T0, "version": None}), T0 + H),
    ("already regressed: the count grows", resolved(regressedAt=T0 + 5 * MIN, regressedVersion="1.4.4", regressedCount=1, status="open"),
     group(arr("m1", 5 * MIN), arr("m2", 7 * MIN)), T0 + H),
    ("already regressed: the count never goes down", resolved(regressedAt=T0 + 5 * MIN, regressedCount=4, status="open"),
     group(arr("m1", 5 * MIN)), T0 + H),
    ("already regressed: no cause now", resolved(regressedAt=T0 + 5 * MIN, regressedCount=2, status="open"), None, T0 + H),
    ("ignored with no end", ignored(), group(arr("m1", 5 * MIN)), T0 + 30 * 86_400_000),
    ("ignored for 1 h, before it ends", ignored(ignoreUntil=T0 + H), group(), T0 + H - 1),
    ("ignored for 1 h, at the end: open again", ignored(ignoreUntil=T0 + H), group(), T0 + H),
    ("ignored for 1 h, after the end, no cause now", ignored(ignoreUntil=T0 + H), None, T0 + 2 * H),
    ("ignored until 10 more, none yet", ignored(ignoreMore=10, ignoreSeen=0), group(), T0 + H),
    ("ignored until 10 more, 3 so far", ignored(ignoreMore=10, ignoreSeen=0),
     group(arr("m1", MIN), arr("m2", 2 * MIN), arr("m3", 3 * MIN)), T0 + H),
    ("ignored until 10 more, a read counted 6 before: never fewer", ignored(ignoreMore=10, ignoreSeen=6),
     group(arr("m1", MIN)), T0 + H),
    ("ignored until 10 more, no seen stored", ignored(ignoreMore=10), group(), T0 + H),
    ("ignored until 10 more, the 10th arrives: open, ended at its arrival", ignored(ignoreMore=10, ignoreSeen=4),
     group(*[arr(f"m{i}", i * MIN) for i in range(1, 12)]), T0 + H),
    ("ignored until 10 more, counted 10 before, the messages have finished since: ended now",
     ignored(ignoreMore=10, ignoreSeen=10), group(arr("m1", MIN)), T0 + H),
    ("ignored until 10 more: excused versions still count while ignored",
     ignored(ignoreMore=10, ignoreSeen=0, resolveVersions=["1.4.3"], resolveMode="next_version"),
     group(arr("m1", MIN, version="1.4.3")), T0 + H),
    ("ignored until 10 more: late exports don't count", ignored(ignoreMore=10, ignoreSeen=0),
     group(arr("m1", MIN, ms=T0 - 20 * MIN), arr("m2", 2 * MIN)), T0 + H),
    ("ignored for a time and its time is up before the count", ignored(ignoreUntil=T0 + H, ignoreMore=None), group(*[arr(f"m{i}", i * MIN) for i in range(1, 3)]), T0 + 2 * H),
    ("ignored, set by Trailbeam (no one)", ignored(statusBy=None, ignoreUntil=T0 + H), group(), T0 + 2 * H),
    ("open with no since: arrivals don't matter", row(), group(arr("m1", MIN)), T0 + H),
]

NEW_SINCE = [
    ("nothing", [], S0, T0, None),
    ("sorted by arrival", [arr("b", 3 * MIN), arr("a", MIN), arr("c", 2 * MIN)], S0, T0, None),
    ("excepted versions", [arr("a", MIN, version="1"), arr("b", 2 * MIN, version="2"), arr("c", 3 * MIN)], S0, T0, ["1"]),
    ("before the watermark", [arr("a", -MIN), arr("b", MIN)], S0, T0, None),
    ("late exports", [arr("a", MIN, ms=T0 - 10 * MIN - 1), arr("b", MIN, ms=T0 - 10 * MIN)], S0, T0, None),
    ("no seq", [arr("a", MIN, seq=False)], S0, T0, None),
]

M = "v1|orders|billing|exc:TimeoutError:*"
A = "v1|orders|billing|exc:TimeoutError:aa"
B = "v1|orders|billing|exc:TimeoutError:bb"
OTHER = "v1|orders|billing|exc:KeyError:cc"
SIG = [("TimeoutError", "Stripe timed out after 10000 ms"), ("Err", "db down"), ("Err:Timeout", "db down"),
       ("Other", "*"), ("Other", " * "), ("Other", "**"), ("Other", "boom"), ("Other", None), ("pkg.mod:Err|x%y", "a: b"),
       ("Err:", ""), ("*", "*")]


def key(typ, text=None, merged=False, q="orders", c="billing"):
    """A cause key as causes.group writes it."""
    sig = causes.merged_signature(typ) if merged else causes.exc_signature(typ, text)
    return f"{causes.KEY_VERSION}|{q}|{c}|{sig}"


ERR_M = key("Err", merged=True)
COLON = key("Err:Timeout", "db down")
COLON_M = key("Err:Timeout", merged=True)
STAR = key("Other", "*")
OTHER_M = key("Other", merged=True)
OTHER_BOOM = key("Other", "boom")
assert not COLON.startswith(ERR_M[:-1]) and STAR != OTHER_M, (COLON, STAR)
ROW_FOR = [
    ("no rows", [], A),
    ("its own row", [row(causeKey=A), row(causeKey=OTHER, updatedAt=T0)], A),
    ("a split cause with no row: its class's merged row", [row(causeKey=M, status="resolved")], A),
    ("a split cause: the merged row changed later wins", [row(causeKey=A, updatedAt=T0), row(causeKey=M, updatedAt=T0 + 1)], A),
    ("a split cause: its own row changed later wins", [row(causeKey=A, updatedAt=T0 + 1), row(causeKey=M, updatedAt=T0)], A),
    ("a split cause: same time, its own row", [row(causeKey=A, updatedAt=T0), row(causeKey=M, updatedAt=T0)], A),
    ("a merged cause: its own row", [row(causeKey=M, updatedAt=T0), row(causeKey=A, updatedAt=T0 + 5)], M),
    ("a merged cause with no row: the newest of its texts", [row(causeKey=A, updatedAt=T0), row(causeKey=B, updatedAt=T0 + 5)], M),
    ("a merged cause with no row: a tie goes to the first key", [row(causeKey=B, updatedAt=T0), row(causeKey=A, updatedAt=T0)], M),
    ("a merged cause: another class's rows don't count", [row(causeKey=OTHER, updatedAt=T0)], M),
    ("another consumer's merged row doesn't count", [row(causeKey="v1|orders|mailer|exc:TimeoutError:*")], A),
    ("a key that only shares a prefix", [row(causeKey="v1|orders|billing|exc:Timeout:*")], A),
    # QV-702: a class with a colon, and an error text of "*", match only their own cause
    ("a class with a colon: not the merged row of the class before the colon", [row(causeKey=ERR_M)], COLON),
    ("a class with a colon: its own class's merged row", [row(causeKey=ERR_M), row(causeKey=COLON_M, status="resolved")], COLON),
    ("a merged class: not a class that starts with it and a colon", [row(causeKey=COLON)], ERR_M),
    ("a text of \"*\" is one text: its class's merged row changed later wins", [row(causeKey=STAR, updatedAt=T0), row(causeKey=OTHER_M, updatedAt=T0 + 1)], STAR),
    ("a text of \"*\" is not merged: no other text's row", [row(causeKey=OTHER_BOOM)], STAR),
    ("the merged cause: a text of \"*\" is one of its texts", [row(causeKey=STAR)], OTHER_M),
]

MEMBERS = ["me", "sam"]
T_OPEN = triage.OPEN
FILTER_TRIAGES = [
    ("open, nobody", {**T_OPEN}),
    ("open, mine", {**T_OPEN, "assignee": "me"}),
    ("open, Sam", {**T_OPEN, "assignee": "sam"}),
    ("open, someone who left", {**T_OPEN, "assignee": "gone"}),
    ("resolved, mine", {**T_OPEN, "status": "resolved", "assignee": "me"}),
    ("ignored, nobody", {**T_OPEN, "status": "ignored"}),
]

NOTES = [
    ("plain", "Retried after the Stripe outage."),
    ("empty", ""),
    ("spaces and newlines", "  \n\t "),
    ("zero-width spaces and a BOM", "​‌‍﻿"),
    ("blank fillers", "ㅤ⠀ᅟ"),
    ("ideographic space", "　"),
    ("a variation selector only", "️"),
    ("tab and newline are fine", "line one\n\tline two"),
    ("NUL", "a\u0000b"),
    ("ESC", "\u001b[31mred"),
    ("a C1 control", "a\u0085b"),
    ("a bidi override", "file‮gnp.exe"),
    ("a bidi isolate", "a⁦b"),
    ("2,000 characters", "x" * 2000),
    ("2,001 characters", "x" * 2001),
    ("1,000 emoji: 2,000 UTF-16 units", "\U0001F600" * 1000),
    ("1,001 emoji: over", "\U0001F600" * 1001),
    ("an emoji with a variation selector", "❤️"),
    ("a lone high surrogate (a truncated emoji)", "see \ud83d attached"),
    ("a lone low surrogate", "see \ude00 attached"),
    ("a lone surrogate at the end", "see \ud83d"),
    ("a lone surrogate only", "\ud83d"),
    ("a surrogate pair is one emoji: 2 units", "x" * 1998 + "\U0001F600"),
    ("a surrogate pair at 2,001 units", "x" * 1999 + "\U0001F600"),
]

RAW_FILTERS = ["open", "mine", "unassigned", "resolved", "ignored", "all", "", None, "OPEN", "closed"]


def main():
    cases = {
        "triageOf": [{"name": n, "row": r, "group": g, "now": now, "expected": triage.public(triage.triage_of(r, g, now))}
                     for n, r, g, now in TRIAGE_OF],
        "newSince": [{"name": n, "arrivals": a, "sinceSeq": s, "atMs": at, "except": e,
                      "expected": [x["mid"] for x in triage.new_since(a, s, at, set(e) if e else None)]}
                     for n, a, s, at, e in NEW_SINCE],
        "rowFor": [{"name": n, "rows": rows, "key": k, "expected": (lambda r: r and r["causeKey"])(triage.row_for(rows, k))}
                   for n, rows, k in ROW_FOR],
        "sameCause": [{"r": r, "g": g, "expected": triage.same_cause(r, g)}
                      for r in (A, M, OTHER) for g in (A, B, M, OTHER, "v1|orders|billing|exc:TimeoutError")]
                     + [{"r": r, "g": g, "expected": triage.same_cause(r, g)}
                        for r in (ERR_M, COLON, COLON_M, STAR, OTHER_M) for g in (ERR_M, COLON, COLON_M, STAR, OTHER_M, OTHER_BOOM)
                        if r != g],
        "causeSignature": [{"errorType": t, "text": x, "expected": causes.exc_signature(t, x),
                            "merged": causes.merged_signature(t)} for t, x in SIG],
        "inFilter": [{"name": n, "triage": t, "filter": f, "me": "me", "members": MEMBERS,
                      "expected": triage.in_filter(t, f, "me", set(MEMBERS))}
                     for n, t in FILTER_TRIAGES for f in triage.FILTERS],
        "filterOf": [{"raw": raw, "expected": triage.filter_of(raw)} for raw in RAW_FILTERS],
        "noteProblem": [{"name": n, "body": b, "expected": triage.note_problem(b)} for n, b in NOTES],
    }
    OUT.write_text(json.dumps(cases, indent=1, ensure_ascii=True) + "\n")
    return cases


if __name__ == "__main__":
    c = main()
    print(f"wrote {OUT.name}: " + ", ".join(f"{len(v)} {k}" for k, v in c.items()))
