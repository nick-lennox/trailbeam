#!/usr/bin/env python3
"""Trailbeam conformance kit: run a language package's driver on the bench and check its spans.

    python conformance/kit.py run conformance/drivers/python/driver.json conformance/drivers/node/driver.json
    python conformance/kit.py run conformance/drivers/node/driver.json --variant alone
    python conformance/kit.py check --truth T.jsonl --spans spans.jsonl --run RUN --key KEY

``run`` starts the bench (ElasticMQ and an OpenTelemetry Collector that writes every span to a file, each on
a random local port, in containers of their own), runs every variant of every driver, checks each run and
prints a report. It exits 1 if any run fails. See README.md. Standard library only.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

KIT = Path(__file__).resolve().parent
REPO = KIT.parent
sys.path.insert(0, str(KIT))
import checker  # noqa: E402

ELASTICMQ_IMAGE = "softwaremill/elasticmq-native:1.6.15"
COLLECTOR_IMAGE = "otel/opentelemetry-collector-contrib:0.165.0"
PARENT_MODES = ("any", "exported", "unexported")
DRIVER_TIMEOUT_S = 180


def log(msg: str) -> None:
    print(f"[kit] {msg}", file=sys.stderr, flush=True)


# -- bench -------------------------------------------------------------------------------------------


class Bench:
    """ElasticMQ and a Collector (OTLP/HTTP in, file out), on random ports of 127.0.0.1."""

    def __init__(self, workdir: Path):
        self.workdir = workdir
        self.tag = "tbkit-" + secrets.token_hex(4)
        self.out = workdir / "collector"
        self.containers: list[str] = []
        self.sqs = self.otlp = ""
        self.spans_file = self.out / "spans.jsonl"

    def _docker(self, *args: str) -> str:
        return subprocess.run(["docker", *args], check=True, capture_output=True, text=True).stdout.strip()

    def _port(self, name: str, port: int) -> int:
        out = self._docker("port", name, f"{port}/tcp")
        return int(out.splitlines()[0].rsplit(":", 1)[1])

    def up(self) -> None:
        self.out.mkdir(parents=True, exist_ok=True)
        shutil.copy(KIT / "bench" / "collector.yaml", self.out / "collector.yaml")
        sqs, col = f"{self.tag}-sqs", f"{self.tag}-collector"
        log(f"bench: starting {ELASTICMQ_IMAGE} and {COLLECTOR_IMAGE} ({self.tag})")
        self._docker("run", "-d", "--rm", "--name", sqs, "-p", "127.0.0.1::9324", ELASTICMQ_IMAGE)
        self.containers.append(sqs)
        user = f"{os.getuid()}:{os.getgid()}" if hasattr(os, "getuid") else "0:0"
        self._docker("run", "-d", "--rm", "--name", col, "--user", user, "-p", "127.0.0.1::4318",
                     "-v", f"{self.out}:/out", COLLECTOR_IMAGE, "--config=/out/collector.yaml")
        self.containers.append(col)
        self.sqs = f"http://127.0.0.1:{self._port(sqs, 9324)}"
        self.otlp = f"http://127.0.0.1:{self._port(col, 4318)}"
        self._wait(f"{self.sqs}/?Action=ListQueues", None, "ElasticMQ")
        self._wait(f"{self.otlp}/v1/traces", b"{}", "the Collector")
        log(f"bench: SQS {self.sqs}, OTLP {self.otlp}, spans in {self.spans_file}")

    def _wait(self, url: str, body, what: str) -> None:
        deadline = time.time() + 60
        while True:
            try:
                req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=2).read()
                return
            except (urllib.error.URLError, ConnectionError, OSError) as e:
                if time.time() > deadline:
                    raise SystemExit(f"[kit] {what} did not come up at {url}: {e}")
                time.sleep(0.5)

    def logs(self) -> str:
        out = []
        for c in self.containers:
            r = subprocess.run(["docker", "logs", "--tail", "40", c], capture_output=True, text=True)
            out.append(f"--- {c}\n{r.stdout}{r.stderr}")
        return "\n".join(out)

    def down(self) -> None:
        for c in self.containers:
            subprocess.run(["docker", "rm", "-f", c], capture_output=True)
        self.containers = []


# -- drivers -----------------------------------------------------------------------------------------


def expand(value: str, driver_dir: Path) -> str:
    def env(m):
        name = m.group(1)
        if name not in os.environ:
            raise SystemExit(f"[kit] driver configuration uses {{env:{name}}}, but ${name} is not set")
        return os.environ[name]

    value = re.sub(r"\{env:([A-Za-z_][A-Za-z0-9_]*)\}", env, value)
    return (value.replace("{kit}", str(KIT)).replace("{repo}", str(REPO)).replace("{driver_dir}", str(driver_dir))
            .replace("{python}", sys.executable))


def load_driver(path: Path) -> dict:
    d = json.loads(path.read_text())
    for k in ("name", "command", "variants"):
        if k not in d:
            raise SystemExit(f"[kit] {path}: missing {k!r} (README.md, 'Driver configuration')")
    for v in d["variants"]:
        if v.get("parent", "any") not in PARENT_MODES:
            raise SystemExit(f"[kit] {path}: variant {v.get('name')}: parent must be one of {PARENT_MODES}")
    d["_dir"] = path.resolve().parent
    return d


def run_variant(driver: dict, variant: dict, bench: Bench, workdir: Path) -> checker.Report:
    name = f"{driver['name']}-{variant['name']}"
    run = f"kit-{re.sub(r'[^A-Za-z0-9-]', '-', name)}-{secrets.token_hex(3)}"[:60]
    key = "kit-" + secrets.token_hex(16)
    truth = workdir / f"{run}.truth.jsonl"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TRAILBEAM_", "OTEL_", "KIT_", "AWS_"))}
    env.update({
        "KIT_RUN": run, "KIT_SQS_ENDPOINT": bench.sqs, "KIT_TRUTH_FILE": str(truth),
        "OTEL_SERVICE_NAME": run,
        "TRAILBEAM_OTLP_TRACES_ENDPOINT": f"{bench.otlp}/v1/traces",
        "OTEL_EXPORTER_OTLP_ENDPOINT": bench.otlp, "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        "TRAILBEAM_RECEIPT_HASH_KEY": key,
        "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x", "AWS_REGION": "elasticmq",
        "AWS_DEFAULT_REGION": "elasticmq",
    })
    for k, v in {**driver.get("env", {}), **variant.get("env", {})}.items():
        env[k] = expand(str(v), driver["_dir"])
    cmd = [expand(a, driver["_dir"]) for a in variant.get("prefix", []) + driver["command"]]
    cwd = expand(driver.get("cwd", "{repo}"), driver["_dir"])
    log(f"run {run}: {' '.join(cmd)}" + (f" (env {', '.join(sorted(variant.get('env', {})))})" if variant.get("env") else ""))
    out_log = workdir / f"{run}.driver.log"
    t0 = time.time()
    with open(out_log, "w") as fh:
        try:
            rc = subprocess.run(cmd, cwd=cwd, env=env, stdout=fh, stderr=subprocess.STDOUT,
                                timeout=DRIVER_TIMEOUT_S).returncode
        except subprocess.TimeoutExpired:
            rc = "timeout"
    log(f"run {run}: driver exited {rc} after {time.time() - t0:.1f} s (log {out_log})")
    if rc != 0 or not truth.exists():
        tail = "\n".join(out_log.read_text(errors="replace").splitlines()[-15:])
        problem = checker.Problem("run", f"the driver failed (exit {rc}"
                                  + ("" if truth.exists() else f", no ground truth at {truth}") + f"). Last lines:\n{tail}",
                                  rule="kit")
        return checker.Report(run, variant["name"], driver["name"], 0, 0, 0, [problem])

    header, calls = checker.read_truth(truth.read_text().splitlines())
    want = len(checker.build_expected(calls, key))
    spans = wait_for_spans(bench.spans_file, run, want)
    settings = {"run": run, "key": key, "variant": variant["name"], "parent": variant.get("parent", "any"),
                "capture": bool(variant.get("capture"))}
    # What `kit.py check --settings` needs to check this run again. The key is a throwaway of this bench.
    (workdir / f"{run}.run.json").write_text(json.dumps(settings, indent=2))
    return checker.check(header, calls, spans, run=run, key=key, variant=variant["name"],
                         parent=settings["parent"], capture=settings["capture"])


def wait_for_spans(path: Path, run: str, want: int, timeout: float = 20.0) -> list:
    """The Collector writes as exports arrive; wait until the run's spans are all there or stop coming."""
    deadline = time.time() + timeout
    last, stable_since = -1, time.time()
    while True:
        spans = checker.read_spans(path.read_text().splitlines()) if path.exists() else []
        n = sum(1 for s in spans if s.scope == checker.SCOPE and s.resource.get("service.name") == run)
        if n != last:
            last, stable_since = n, time.time()
        if (n >= want and time.time() - stable_since > 1.0) or time.time() > deadline or \
                (n > 0 and time.time() - stable_since > 5.0):
            return spans
        time.sleep(0.25)


# -- CLI ---------------------------------------------------------------------------------------------


def cmd_run(args) -> int:
    drivers = [load_driver(Path(p)) for p in args.drivers]
    workdir = Path(args.workdir or tempfile.mkdtemp(prefix="trailbeam-kit-")).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    bench = Bench(workdir)
    reports = []
    try:
        bench.up()
        for d in drivers:
            for v in d["variants"]:
                if args.variant and v["name"] not in args.variant:
                    continue
                reports.append(run_variant(d, v, bench, workdir))
                print(checker.render(reports[-1]), flush=True)
                print(flush=True)
    finally:
        if any(not r.ok for r in reports):
            (workdir / "bench.log").write_text(bench.logs())
        bench.down()
    if not reports:
        log("no variant matched")
        return 2
    (workdir / "report.json").write_text(json.dumps([r.as_json() for r in reports], indent=2))
    failed = [r for r in reports if not r.ok]
    print(f"{len(reports) - len(failed)} of {len(reports)} runs pass"
          + (f"; failing: {', '.join(f'{r.driver}/{r.variant}' for r in failed)}" if failed else "")
          + f". Ground truth, driver logs, spans and report.json are in {workdir}")
    print("(The ground-truth files hold raw receipt handles of this local bench. Don't publish them.)")
    return 1 if failed else 0


def cmd_check(args) -> int:
    if args.settings:
        saved = json.loads(Path(args.settings).read_text())
        for k in ("run", "key", "variant", "parent", "capture"):
            if getattr(args, k) in (None, "", False):
                setattr(args, k, saved.get(k))
    if not args.run or not args.key:
        raise SystemExit("[kit] check needs --run and --key, or --settings <run>.run.json")
    header, calls = checker.read_truth(Path(args.truth).read_text().splitlines())
    spans = checker.read_spans(Path(args.spans).read_text().splitlines())
    report = checker.check(header, calls, spans, run=args.run, key=args.key, variant=args.variant or "",
                           parent=args.parent or "any", capture=bool(args.capture))
    print(json.dumps(report.as_json(), indent=2) if args.json else checker.render(report))
    return 0 if report.ok else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="start the bench, run drivers, check their spans")
    r.add_argument("drivers", nargs="+", help="driver configuration files (driver.json)")
    r.add_argument("--variant", action="append", help="run only this variant (repeatable)")
    r.add_argument("--workdir", help="keep ground truth, logs and spans here (default: a new temporary directory)")
    r.set_defaults(fn=cmd_run)
    c = sub.add_parser("check", help="check an existing spans file against a ground-truth file")
    c.add_argument("--truth", required=True)
    c.add_argument("--spans", required=True, help="the Collector's file export (OTLP JSON lines)")
    c.add_argument("--settings", help="a <run>.run.json that `kit.py run` wrote: run, key, variant, parent, capture")
    c.add_argument("--run", help="the run's service.name")
    c.add_argument("--key", help="the TRAILBEAM_RECEIPT_HASH_KEY the driver ran with")
    c.add_argument("--variant", default="")
    c.add_argument("--parent", choices=PARENT_MODES)
    c.add_argument("--capture", action="store_true", help="the run had body capture on for every queue")
    c.add_argument("--json", action="store_true")
    c.set_defaults(fn=cmd_check)
    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
