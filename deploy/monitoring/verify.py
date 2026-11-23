"""Checks for deploy/monitoring/check.sh (QV-304). No dependencies beyond Python.

  verify.py agent <agent check --json output> <datadog dir>
      The Datadog Agent's OpenMetrics check collected the metrics openmetrics.yaml names, under trailbeam.*, with
      the seeded values; every tag is from a fixed set; the monitors' queries name metrics it collected.
  verify.py prometheus <Prometheus URL> <dashboard JSON> <alert rules>
      Prometheus scraped Trailbeam; every dashboard and alert expression runs there without an error; the stuck and
      dead-letter series have the seeded values.
"""
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

# Every tag key a Trailbeam series may carry in Datadog, and where its values come from: a fixed set each.
# "endpoint" is added by the Agent (the openmetrics_endpoint).
TAG_KEYS = {"route", "code", "reason", "result", "outcome", "join", "file", "gauge", "version", "endpoint"}
# Counters that only appear once something happened (a rejected export, a failed gauge)
MAY_BE_ABSENT = {"ingest_rejected_requests", "metrics_gauge_errors"}
SEEDED = {"trailbeam.stuck_messages": 1, "trailbeam.dead_lettered_messages_recent": 1}
MAX_SERIES = 100  # with the seeded traffic: about 45. The count is fixed by the routes and codes, not by traffic.


def fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"ok: {msg}")


def conf_metrics(conf: str) -> list[str]:
    block = conf.split("\n    metrics:\n", 1)[1]
    names = []
    for line in block.splitlines():
        m = re.match(r"^      - ([a-z0-9_]+)\s*$", line)
        if m:
            names.append(m.group(1))
        elif line.strip() and not line.strip().startswith("#") and not line.startswith("      "):
            break
    return names


def agent(out_path: str, dd_dir: str) -> None:
    runs = json.loads(Path(out_path).read_text())
    run = runs[-1]
    errors = run["runner"]["TotalErrors"]
    if errors:
        fail(f"the check had {errors} error(s): {run['runner']['LastError']}")
    series = run["aggregator"]["metrics"]
    by_name: dict = {}
    for s in series:
        by_name.setdefault(s["metric"], []).append(s)
    names = set(by_name)
    if not names or not all(n.startswith("trailbeam.") for n in names):
        fail(f"metrics outside trailbeam.*: {sorted(n for n in names if not n.startswith('trailbeam.'))}")
    ok(f"the Agent collected {len(names)} metrics, {len(series)} series, all named trailbeam.*")
    if len(series) > MAX_SERIES:
        fail(f"{len(series)} series, more than {MAX_SERIES}")
    for s in series:
        for tag in s["tags"]:
            key = tag.split(":", 1)[0]
            if key not in TAG_KEYS:
                fail(f"{s['metric']} has the tag {tag!r}, not from a fixed set")
    ok(f"every tag is one of {sorted(TAG_KEYS)}")
    routes = {t for s in series for t in s["tags"] if t.startswith("route:")}
    if any(re.search(r"stuck-1|dead-1|done-1", r) for r in routes):
        fail(f"a message ID in a route tag: {sorted(routes)}")
    ok(f"route tags are templates: {sorted(routes)}")

    conf = (Path(dd_dir) / "openmetrics.yaml").read_text()
    for m in conf_metrics(conf):
        if m in MAY_BE_ABSENT:
            continue
        if not any(n == f"trailbeam.{m}" or n.startswith(f"trailbeam.{m}.") for n in names):
            fail(f"openmetrics.yaml names {m}, but the Agent collected no trailbeam.{m}")
    ok("every metric openmetrics.yaml names was collected")
    for name, want in SEEDED.items():
        got = {p[1] for s in by_name.get(name, []) for p in s["points"]}
        if got != {want}:
            fail(f"{name}: got {sorted(got)}, want {want}")
        ok(f"{name} = {want}")
    for f in sorted((Path(dd_dir) / "monitors").glob("*.json")):
        mon = json.loads(f.read_text())
        q = mon["query"]
        used = set(re.findall(r"trailbeam\.[a-z0-9_.]+", q))
        if not used or not used <= names:
            fail(f"{f.name}: the query names {sorted(used - names)}, which the Agent did not collect")
        m = re.fullmatch(r"(max|min|avg|sum)\(last_\d+[mh]\):(max|min|avg|sum):[a-z0-9_.]+\{[^}]*\} ([<>]=?) (\d+)", q)
        if not m or float(m.group(4)) != float(mon["options"]["thresholds"]["critical"]):
            fail(f"{f.name}: the query {q!r} doesn't end in the critical threshold")
        ok(f"{f.name}: {q}")


def prom_query(base: str, expr: str) -> list:
    url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=10) as r:
        body = json.load(r)
    if body.get("status") != "success":
        fail(f"{expr}: {body}")
    return body["data"]["result"]


def prometheus(base: str, dashboard: str, rules: str) -> None:
    up = prom_query(base, 'up{job="trailbeam"}')
    if not up or up[0]["value"][1] != "1":
        fail(f"Prometheus has not scraped Trailbeam: {up}")
    ok("Prometheus scraped Trailbeam")
    exprs = [t["expr"] for p in json.loads(Path(dashboard).read_text())["panels"] for t in p.get("targets", [])]
    exprs += [m.group(1).strip() for m in re.finditer(r"^\s+expr: (.+)$", Path(rules).read_text(), re.M)]
    for e in exprs:
        prom_query(base, e)
        for name in set(re.findall(r"\b(trailbeam_[a-z0-9_]+?)(?:_bucket|_count|_sum)?\b", e)):
            if not prom_query(base, f"count({{__name__=~\"{name}(_bucket|_total)?\"}})") and name not in (
                    "trailbeam_ingest_rejected_requests_total",):
                fail(f"{e}: Prometheus has no series {name}")
    ok(f"{len(exprs)} dashboard and alert expressions run, on series Prometheus has")
    for name, want in (("trailbeam_stuck_messages", 1), ("trailbeam_dead_lettered_messages_recent", 1)):
        got = prom_query(base, f"max({name})")
        if not got or float(got[0]["value"][1]) != want:
            fail(f"max({name}): got {got}, want {want}")
        ok(f"max({name}) = {want}")


if __name__ == "__main__":
    {"agent": agent, "prometheus": prometheus}[sys.argv[1]](*sys.argv[2:])
