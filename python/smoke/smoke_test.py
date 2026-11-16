"""Smoke test for a running Trailbeam (for example the container): send recorded spans, check the screen.

    python smoke_test.py http://127.0.0.1:4318

Standard library only. Waits for ``/api/health``, posts ``recorded-spans.pb`` (real Trailbeam spans recorded
by ``record_spans.py``) to ``/v1/traces``, then checks that the web screen shows each message's timeline
in order, with the inferred step marked, and lists the duplicate and the stuck message. Exits non-zero
with a message on the first failure.
"""
from __future__ import annotations

import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from html import escape
from pathlib import Path

HERE = Path(__file__).resolve().parent


class SmokeFailure(AssertionError):
    pass


def check(cond: bool, what: str) -> None:
    if not cond:
        raise SmokeFailure(what)
    print(f"ok: {what}")


def get(base: str, path: str) -> tuple[int, str, str]:
    try:
        with urllib.request.urlopen(base + path, timeout=10) as r:
            return r.status, r.headers.get("Content-Type", ""), r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("Content-Type", ""), e.read().decode()


def wait_healthy(base: str, timeout_s: float = 60) -> dict:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            status, _, body = get(base, "/api/health")
            if status == 200:
                return json.loads(body)
            last = f"status {status}"
        except (OSError, ValueError) as e:
            last = e
        time.sleep(0.5)
    raise SmokeFailure(f"Trailbeam at {base} not healthy after {timeout_s:.0f} s: {last}")


def timeline_events(html: str) -> list[tuple[str, bool]]:
    return [(t, i == "true") for t, i in
            re.findall(r'<li class="[^"]*" data-type="([^"]+)" data-inferred="(\w+)"', html)]


def message_href(mid: str) -> str:
    return 'href="' + escape("/message?" + urllib.parse.urlencode({"id": mid})) + '"'


def run(base: str) -> None:
    base = base.rstrip("/")
    expected = json.loads((HERE / "recorded-spans.json").read_text())
    before = wait_healthy(base)
    check(before["status"] == "ok", f"Trailbeam {before.get('version')} is healthy")

    req = urllib.request.Request(base + "/v1/traces", data=(HERE / "recorded-spans.pb").read_bytes(),
                                 method="POST", headers={"Content-Type": "application/x-protobuf"})
    with urllib.request.urlopen(req, timeout=10) as r:
        check(r.status == 200, "POST /v1/traces accepted the recorded spans")
    health = wait_healthy(base)
    inserted = health["ingest"]["inserted"] - before["ingest"]["inserted"]
    # A second run against the same volume finds them stored already, which is fine too.
    dupes = health["ingest"]["duplicates"] - before["ingest"]["duplicates"]
    check(inserted + dupes == expected["trailbeam_span_count"] and health["ingest"]["rejected"] == 0,
          f"{expected['trailbeam_span_count']} Trailbeam spans stored, none rejected")

    for name in ("redelivered", "released", "never_received"):
        m = expected[name]
        status, ctype, html = get(base, "/message?" + urllib.parse.urlencode({"id": m["message_id"]}))
        check(status == 200 and ctype.startswith("text/html"), f"timeline page for {name} loads")
        got = timeline_events(html)
        check([t for t, _ in got] == m["events"], f"{name} timeline in order: {' > '.join(m['events'])}")
        for t, inferred in got:
            check(inferred == (t == "visibility_timeout_expired"),
                  f"{name}: {t} is {'marked inferred' if inferred else 'observed'}")
        inferred_items = re.findall(r'<li [^>]*data-inferred="true"[^>]*>(.*?)</li>', html)
        check(len(inferred_items) == sum(i for _, i in got)
              and all('<span class="badge">inferred</span>' in li for li in inferred_items),
              f"{name}: every inferred step carries the visible INFERRED label")

    _, _, dup = get(base, "/duplicates")
    check(message_href(expected["redelivered"]["message_id"]) in dup, "duplicates page lists the redelivered message")
    _, _, stuck = get(base, "/stuck?older_than_s=0")
    check(message_href(expected["never_received"]["message_id"]) in stuck, "stuck page lists the never-received message")
    check(message_href(expected["redelivered"]["message_id"]) not in stuck, "stuck page omits the deleted message")
    status, _, _ = get(base, "/message?id=no-such-message")
    check(status == 404, "unknown message gives 404")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        run(argv[1])
    except SmokeFailure as e:
        print(f"FAILED: {e}", file=sys.stderr)
        return 1
    print("smoke test passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
