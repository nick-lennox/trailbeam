"""Smoke test for a running Trailbeam (for example the container): send recorded spans, check the screen.

    python smoke_test.py http://127.0.0.1:4318

Standard library only. Waits for ``/api/health``, posts ``recorded-spans.pb`` (real Trailbeam spans recorded
by ``record_spans.py``) to ``/v1/traces``, then checks each message's timeline through the API (in order,
with the inferred step marked), that its timeline page loads (the shared UI's shell and its files when
built, otherwise the server-rendered page, which must show the same steps), and that the duplicates and
stuck pages list the right messages (from the page itself, or for the shell from the API it draws from). Exits non-zero with a message on the first failure.
"""
from __future__ import annotations

import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from html import unescape
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


def listed(base: str, page: str, query: str) -> set[str]:
    """The message ids the /duplicates or /stuck page shows for this query."""
    status, ctype, html = get(base, f"/{page}" + (f"?{query}" if query else ""))
    check(status == 200 and ctype.startswith("text/html"), f"{page} page loads")
    if 'id="root"' not in html:  # the server-rendered page lists them as links
        return set(linked_ids(html))
    # The shared UI's shell: it passes the page's query to /api/<page> and draws the list in the browser.
    assets = re.findall(r'(?:src|href)="(/ui/assets/[^"]+)"', html)
    check(any(a.endswith(".js") for a in assets) and all(get(base, a)[0] == 200 for a in assets),
          f"{page}: the page's script and styles load")
    status, _, body = get(base, f"/api/{page}" + (f"?{query}" if query else ""))
    check(status == 200, f"{page}: the list the page draws loads from the API")
    return {m["message_id"] for m in json.loads(body)["messages"]}


def linked_ids(html: str) -> list[str]:
    return [urllib.parse.parse_qs(urllib.parse.urlsplit(unescape(h)).query).get("id", [""])[0]
            for h in re.findall(r'href="(/message\?[^"]+)"', html)]


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
        mid = urllib.parse.quote(m["message_id"], safe="")
        status, _, body = get(base, f"/api/messages/{mid}")
        check(status == 200, f"timeline for {name} loads from the API")
        got = [(e["type"], e["inferred"]) for e in json.loads(body)["events"]]
        check([t for t, _ in got] == m["events"], f"{name} timeline in order: {' > '.join(m['events'])}")
        for t, inferred in got:
            check(inferred == (t == "visibility_timeout_expired"),
                  f"{name}: {t} is {'marked inferred' if inferred else 'observed'}")

        status, ctype, html = get(base, "/message?" + urllib.parse.urlencode({"id": m["message_id"]}))
        check(status == 200 and ctype.startswith("text/html"), f"timeline page for {name} loads")
        if 'id="root"' in html:  # the shared UI's shell: it draws the timeline from the API above in the browser
            assets = re.findall(r'(?:src|href)="(/ui/assets/[^"]+)"', html)
            check(any(a.endswith(".js") for a in assets) and all(get(base, a)[0] == 200 for a in assets),
                  f"{name}: the timeline page's script and styles load")
            continue
        html_got = timeline_events(html)  # the server-rendered page
        check(html_got == got, f"{name}: the page shows the API's steps in the API's order")
        inferred_items = re.findall(r'<li [^>]*data-inferred="true"[^>]*>(.*?)</li>', html)
        check(len(inferred_items) == sum(i for _, i in got)
              and all('<span class="badge">inferred</span>' in li for li in inferred_items),
              f"{name}: every inferred step carries the visible INFERRED label")

    dup = listed(base, "duplicates", "")
    check(expected["redelivered"]["message_id"] in dup, "duplicates page lists the redelivered message")
    stuck = listed(base, "stuck", "older_than_s=0")
    check(expected["never_received"]["message_id"] in stuck, "stuck page lists the never-received message")
    check(expected["redelivered"]["message_id"] not in stuck, "stuck page omits the deleted message")
    status, _, _ = get(base, "/api/messages/no-such-message")
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
