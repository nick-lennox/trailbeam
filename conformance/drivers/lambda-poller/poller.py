#!/usr/bin/env python3
"""The Lambda drivers' poller: plays Lambda's SQS event source mapping against a function in the AWS Lambda
base image, whose Runtime Interface Emulator (RIE) takes invocations over HTTP, and writes the ground truth.

    poller.py build --name python-lambda --dockerfile D --context python --context conformance/drivers/python-lambda
    poller.py run   --name python-lambda

`build` (the driver's `setup`, once per kit run) builds the function image from a small context of the given
repository paths. `run` (once per variant) starts the function on the bench's Docker network, runs
SCENARIO-LAMBDA.md and writes $KIT_TRUTH_FILE. It started from QV-301's drivers/lambda-poller/poller.py.

Like Lambda's own poller it has no Trailbeam hooks and no tracing: Trailbeam sees only what the function
sees. Every value in the truth comes from SQS, from the RIE's response, or from the handler's KIT_HANDLER
log line (what the runtime gave the handler), never from the package under test.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
IGNORE = shutil.ignore_patterns("node_modules", "__pycache__", ".pytest_cache", "*.egg-info", ".venv", "dist",
                                "build", "test", "tests", "*.sqlite")
RIE_PATH = "/2015-03-31/functions/function/invocations"
VISIBILITY_TIMEOUT_S = 2
MAX_RECEIVE_COUNT = 2


def log(msg: str) -> None:
    print(f"[poller] {msg}", file=sys.stderr, flush=True)


def docker(*args: str, check: bool = True) -> str:
    r = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(f"[poller] docker {' '.join(args[:3])} ... failed ({r.returncode}):\n{r.stdout}{r.stderr}")
    return r.stdout.strip()


def image_tag(name: str) -> str:
    return f"{os.environ['KIT_BENCH_TAG']}-{name}"


# -- build -------------------------------------------------------------------------------------------


def cmd_build(args) -> int:
    with tempfile.TemporaryDirectory(prefix="tbkit-lambda-context-") as ctx:
        for rel in args.context:
            src, dst = REPO / rel, Path(ctx) / rel
            if src.is_dir():
                shutil.copytree(src, dst, ignore=IGNORE)
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(src, dst)
        tag = image_tag(args.name)
        log(f"building {tag} from {args.dockerfile} with {', '.join(args.context)}")
        t0 = time.time()
        r = subprocess.run(["docker", "build", "-q", "-t", tag, "-f", args.dockerfile, ctx], capture_output=True, text=True)
        if r.returncode != 0:
            print(r.stdout + r.stderr, file=sys.stderr)
            return 1
        log(f"built {tag} in {time.time() - t0:.0f} s")
    return 0


# -- the function container --------------------------------------------------------------------------


class Function:
    def __init__(self, name: str, run: str):
        self.image = image_tag(name)
        self.container = f"{run}-fn"
        self.function_name = f"{run}-fn"
        # One Lambda execution environment = one log stream (contract 5); the format of a real one.
        self.log_stream = time.strftime("%Y/%m/%d") + "/[$LATEST]" + secrets.token_hex(16)
        self.url = ""

    def env(self) -> list[str]:
        """The variables the kit set for the package, with the bench's addresses as seen from its network."""
        otlp = os.environ["KIT_OTLP_ENDPOINT_IN_NETWORK"]
        env = {k: v for k, v in os.environ.items() if k.startswith(("TRAILBEAM_", "OTEL_"))}
        env.update({
            "TRAILBEAM_OTLP_TRACES_ENDPOINT": f"{otlp}/v1/traces", "OTEL_EXPORTER_OTLP_ENDPOINT": otlp,
            "AWS_LAMBDA_FUNCTION_NAME": self.function_name, "AWS_LAMBDA_LOG_STREAM_NAME": self.log_stream,
            "AWS_REGION": os.environ.get("AWS_REGION", "elasticmq"), "AWS_DEFAULT_REGION": os.environ.get("AWS_REGION", "elasticmq"),
            "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x",
        })
        out = []
        for k, v in sorted(env.items()):
            out += ["-e", f"{k}={v}"]
        return out

    def start(self) -> None:
        docker("rm", "-f", self.container, check=False)
        docker("run", "-d", "--rm", "--name", self.container, "--network", os.environ["KIT_DOCKER_NETWORK"],
               "-p", "127.0.0.1::8080", *self.env(), self.image)
        port = int(docker("port", self.container, "8080/tcp").splitlines()[0].rsplit(":", 1)[1])
        self.url = f"http://127.0.0.1:{port}{RIE_PATH}"
        # Wait for an HTTP answer from the RIE itself: Docker's port proxy accepts connections on the host port
        # before anything listens in the container, and then resets them. Any HTTP status will do (GET on its
        # root is a 404); an invocation would run the handler, so it is not used to probe.
        deadline = time.time() + 30
        while True:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2).read()
                return
            except urllib.error.HTTPError:
                return
            except (urllib.error.URLError, ConnectionError, OSError) as e:
                if time.time() > deadline:
                    raise SystemExit(f"[poller] the RIE did not come up on port {port}: {e}")
                time.sleep(0.2)

    def versions(self) -> str:
        out = docker("run", "--rm", "--entrypoint", "cat", self.image, "/kit-versions.txt", check=False)
        return ", ".join(" ".join(line.split()) for line in out.splitlines() if line.strip())

    def logs(self) -> str:
        r = subprocess.run(["docker", "logs", self.container], capture_output=True, text=True)
        return r.stdout + r.stderr

    def stop(self) -> None:
        docker("rm", "-f", self.container, check=False)

    def invoke(self, event: dict, request_id: str):
        """POST the event the way the Lambda service hands it to the runtime. The RIE uses the request ID we give."""
        req = urllib.request.Request(self.url, data=json.dumps(event).encode(), method="POST",
                                     headers={"Content-Type": "application/json", "X-Amzn-RequestId": request_id})
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read()
        try:
            return json.loads(raw) if raw else None
        except ValueError:
            return raw.decode(errors="replace")


# -- the event source mapping ------------------------------------------------------------------------


def to_record(m: dict, arn: str, region: str) -> dict:
    """An SQS message as Lambda puts it in the event (the shape of AWS's sample events).

    `attributes` has every system attribute SQS returned. Real Lambda records are documented with
    ApproximateReceiveCount, SentTimestamp, SenderId, ApproximateFirstReceiveTimestamp (and AWSTraceHeader,
    MessageGroupId and the like when set); whether they carry DeadLetterQueueSourceArn is not verified
    (contract 4.2). The poller passes it on, so that the wrapper's copy of it is checked.
    """
    return {
        "messageId": m["MessageId"], "receiptHandle": m["ReceiptHandle"], "body": m["Body"],
        "attributes": dict(m.get("Attributes", {})),
        "messageAttributes": {k: {"stringValue": v.get("StringValue"), "stringListValues": [], "binaryListValues": [],
                                  "dataType": v["DataType"]} for k, v in m.get("MessageAttributes", {}).items()},
        "md5OfBody": m["MD5OfBody"], "eventSource": "aws:sqs", "eventSourceARN": arn, "awsRegion": region,
    }


class Poller:
    def __init__(self, sqs, fn: Function):
        self.sqs, self.fn = sqs, fn
        self.invocations: list[dict] = []
        self.arns: dict[str, str] = {}

    def arn(self, url: str) -> str:
        if url not in self.arns:
            self.arns[url] = self.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        return self.arns[url]

    def round(self, url: str, case: str) -> dict | None:
        """One poll of the event source mapping: receive a batch, invoke, delete what succeeded."""
        msgs = self.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                                        MessageSystemAttributeNames=["All"], MessageAttributeNames=["All"]).get("Messages", [])
        if not msgs:
            return None
        arn = self.arn(url)
        region = arn.split(":")[3]
        records = [to_record(m, arn, region) for m in msgs]
        request_id = str(uuid.uuid4())
        sent = time.time_ns()
        response = self.fn.invoke({"Records": records}, request_id)
        returned = time.time_ns()
        # The RIE answers a thrown error with 200 and {"errorType", "errorMessage", ...} (Lambda's
        # X-Amz-Function-Error header is not set by the RIE).
        error = None
        if isinstance(response, dict) and "errorType" in response and "batchItemFailures" not in response:
            error = {"type": response.get("errorType"), "message": response.get("errorMessage")}
        # Lambda's rules with ReportBatchItemFailures on: a function error deletes nothing; an invalid list
        # (an itemIdentifier that is not in the batch) fails the whole batch; else listed records stay.
        ids = [m["MessageId"] for m in msgs]
        if error:
            keep = set(ids)
        else:
            items = response.get("batchItemFailures") if isinstance(response, dict) else None
            listed = [i.get("itemIdentifier") if isinstance(i, dict) else None for i in (items or [])]
            keep = set(ids) if any(x not in ids for x in listed) else set(listed)
        done = [m for m in msgs if m["MessageId"] not in keep]
        if done:
            r = self.sqs.delete_message_batch(QueueUrl=url, Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(done)])
            if r.get("Failed"):
                raise SystemExit(f"[poller] the event source mapping's delete failed: {r['Failed']}")
        inv = {"op": "LambdaInvoke", "case": case, "queue_url": url, "event_source_arn": arn,
               "request_id": request_id, "invoke_sent_unix_nano": sent, "invoke_returned_unix_nano": returned,
               "records": [{"message_id": r["messageId"], "receipt_handle": r["receiptHandle"], "body": r["body"],
                            "attributes": r["attributes"]} for r in records],
               "response": response if not error else None, "function_error": error,
               "deleted": [m["MessageId"] for m in done]}
        self.invocations.append(inv)
        log(f"invoke {len(self.invocations)} ({case}) on {url.rsplit('/', 1)[-1]}: {len(msgs)} records, "
            f"{'error ' + error['type'] if error else 'response ' + json.dumps(response)[:120]}, deleted {len(done)}")
        return inv

    def until(self, url: str, case: str, want: set[str], tries: int = 10) -> None:
        seen: set[str] = set()
        for _ in range(tries):
            inv = self.round(url, case)
            if inv:
                seen |= {r["message_id"] for r in inv["records"]}
            if want <= seen:
                return
        raise SystemExit(f"[poller] case {case}: messages {sorted(want - seen)} never arrived on {url}")

    def cleanup(self, url: str, ids: set[str]) -> None:
        """Not Lambda behaviour: take a failed message out so that the next case starts with an empty queue."""
        deadline = time.time() + 15
        while ids and time.time() < deadline:
            for m in self.sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                                              VisibilityTimeout=30).get("Messages", []):
                self.sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
                ids.discard(m["MessageId"])
        if ids:
            raise SystemExit(f"[poller] cleanup: {sorted(ids)} did not come back")


def scenario(p: Poller, run: str) -> None:
    sqs = p.sqs
    dlq = sqs.create_queue(QueueName=f"{run}-dlq")["QueueUrl"]
    q = sqs.create_queue(QueueName=run, Attributes={
        "VisibilityTimeout": str(VISIBILITY_TIMEOUT_S),
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": p.arn(dlq), "maxReceiveCount": str(MAX_RECEIVE_COUNT)})})["QueueUrl"]

    def send(case: str, n: int) -> str:
        return sqs.send_message(QueueUrl=q, MessageBody=json.dumps({"case": case, "n": n}))["MessageId"]

    # 1. Success: one record, the handler returns nothing.
    p.until(q, "success", {send("success", 1)})
    # 2. Reported failure: a batch of two; the handler lists `report` in batchItemFailures.
    report = send("report", 2)
    p.until(q, "report", {report, send("success", 3)})
    # 3. Redelivery and the DLQ: `report` comes back after the visibility timeout (receive count 2) and
    #    fails again; at its third receive SQS moves it to the DLQ, where the function gets it and succeeds.
    time.sleep(VISIBILITY_TIMEOUT_S + 1)
    p.until(q, "report-retry", {report})
    time.sleep(VISIBILITY_TIMEOUT_S + 1)
    if p.round(q, "dlq-move") is not None:
        raise SystemExit("[poller] ElasticMQ delivered the message a third time instead of moving it to the DLQ")
    p.until(dlq, "dlq", {report})
    # 4. Invalid batchItemFailures: an itemIdentifier that is not in the batch fails the whole batch.
    invalid = send("invalid", 4)
    p.until(q, "invalid", {invalid})
    p.cleanup(q, {invalid})
    # 5. A thrown exception: every record fails, nothing is deleted.
    thrown = send("throw", 5)
    p.until(q, "throw", {thrown})
    p.cleanup(q, {thrown})


def handler_lines(text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        i = line.find("KIT_HANDLER ")
        if i >= 0:
            out.append(json.loads(line[i + len("KIT_HANDLER "):]))
    return out


def cmd_run(args) -> int:
    import boto3  # the kit's Python has it (README.md)

    run = os.environ["KIT_RUN"]
    truth = Path(os.environ["KIT_TRUTH_FILE"])
    sqs = boto3.client("sqs", endpoint_url=os.environ["KIT_SQS_ENDPOINT"], region_name=os.environ.get("AWS_REGION", "elasticmq"))
    fn = Function(args.name, run)
    fn.start()
    p = Poller(sqs, fn)
    try:
        scenario(p, run)
        time.sleep(0.5)  # the RIE writes the last log lines after it answers
    finally:
        logs = fn.logs()
        Path(os.environ.get("KIT_WORKDIR", ".")).joinpath(f"{run}.function.log").write_text(logs)
        fn.stop()
    lines = handler_lines(logs)
    if len(lines) != len(p.invocations):
        raise SystemExit(f"[poller] {len(p.invocations)} invocations but {len(lines)} KIT_HANDLER lines in the function log")
    for i, (inv, h) in enumerate(zip(p.invocations, lines), 1):
        if h["message_ids"] != [r["message_id"] for r in inv["records"]]:
            raise SystemExit(f"[poller] invocation {i}: the handler saw {h['message_ids']}")
        inv["handler"] = {k: h.get(k) for k in ("request_id", "function_name", "log_stream")}
        inv["handler"]["started_unix_nano"] = int(h["started_unix_nano"])
    header = {"truth": 1, "driver": args.name, "source": "lambda", "language": args.language,
              "runtime": args.runtime, "package": fn.versions(),
              "event_source_mapping": "conformance/drivers/lambda-poller/poller.py against the RIE",
              "function_name": fn.function_name, "log_stream": fn.log_stream}
    with open(truth, "w") as fh:
        fh.write(json.dumps(header) + "\n")
        for n, inv in enumerate(p.invocations, 1):
            fh.write(json.dumps({"call": n, **inv}) + "\n")
    log(f"{len(p.invocations)} invocations; ground truth in {truth}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--name", required=True)
    b.add_argument("--dockerfile", required=True)
    b.add_argument("--context", action="append", required=True, help="a repository path to put in the build context")
    b.set_defaults(fn=cmd_build)
    r = sub.add_parser("run")
    r.add_argument("--name", required=True)
    r.add_argument("--language", default="")
    r.add_argument("--runtime", default="")
    r.set_defaults(fn=cmd_run)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
