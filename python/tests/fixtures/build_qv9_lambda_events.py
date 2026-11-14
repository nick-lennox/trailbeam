"""Rebuild the SQS events of the QV-9 Lambda run from its CloudWatch logs.

Input: ``raw__lambda_logs.json`` from the QV-9 evidence bundle (qv9-evidence-bundle.zip on QV-9).
Output: ``qv9_lambda_events.json`` next to this file. Run with ``python -I build_qv9_lambda_events.py <logs>``.

The QV-9 prototype wrapper logged one ``QV2SPAN`` line per span it emitted: a ``qv2 receive`` per
record (message ID, receive count, sent timestamp, AWSTraceHeader, queue name, request ID) and a
``qv2 outcome`` per record. From those, each invocation's ``Records`` are rebuilt. What the logs do
not contain is filled in, and marked in ``provenance``:

- receipt handles: synthetic (the prototype logged only a hash), one per delivery;
- bodies: a fixed synthetic body (the bodies were synthetic and are not needed);
- eventSourceARN account: 123456789012 (the AWS documentation example account);
- ApproximateFirstReceiveTimestamp and SenderId: from the ``QV2EVENT`` line when one matches the
  record's SentTimestamp and AWSTraceHeader, otherwise omitted.

``expected`` is what the prototype recorded for each record: its outcome (with
``success_deleted_by_lambda`` renamed ``success``), or null when the invocation logged no outcome
(the function was killed). ``behaviour`` is what the test handler does to reproduce it.
"""
import hashlib
import json
import sys
from collections import OrderedDict
from pathlib import Path

ACCOUNT = "123456789012"
REGION = "us-east-1"
BODY = '{"synthetic": true}'


def main(path):
    logs = json.loads(Path(path).read_text("utf-8"))
    shapes = []
    spans = []
    streams = {}
    for e in logs:
        msg = e["message"]
        if msg.startswith("QV2EVENT "):
            shapes.append(json.loads(msg[len("QV2EVENT "):]))
        elif msg.startswith("QV2SPAN "):
            spans.append(json.loads(msg[len("QV2SPAN "):]))
        elif msg.startswith("START RequestId: "):
            streams[msg.split()[2]] = e["logStreamName"]

    invocations = OrderedDict()
    for s in spans:
        a = s["attributes"]
        req = a.get("faas.invocation_id")
        if s["name"] not in ("qv2 receive", "qv2 outcome") or not req:
            continue
        inv = invocations.setdefault(req, {"receives": [], "outcomes": {}, "errors": set()})
        if s["name"] == "qv2 receive":
            inv["receives"].append(a)
        else:
            oc = a["qv2.outcome"]
            inv["outcomes"][a["messaging.message.id"]] = "success" if oc == "success_deleted_by_lambda" else oc
            if a.get("qv2.error"):
                inv["errors"].add(a["qv2.error"].split("(", 1)[0])

    out = []
    for req, inv in invocations.items():
        records = []
        for a in inv["receives"]:
            attrs = {"ApproximateReceiveCount": str(a["qv2.receive_count"]), "SentTimestamp": a["qv2.sent_timestamp"]}
            if a.get("qv2.aws_trace_header"):
                attrs["AWSTraceHeader"] = a["qv2.aws_trace_header"]
            for sh in shapes:
                sa = sh["attributes"]
                if sa.get("SentTimestamp") == attrs["SentTimestamp"] and sa.get("AWSTraceHeader") == attrs.get("AWSTraceHeader"):
                    attrs["SenderId"] = sa["SenderId"]
                    attrs["ApproximateFirstReceiveTimestamp"] = sa["ApproximateFirstReceiveTimestamp"]
            mid = a["messaging.message.id"]
            rh = "synthetic-rh-" + hashlib.sha256(f"{req}/{mid}".encode()).hexdigest()
            records.append({
                "messageId": mid, "receiptHandle": rh, "body": BODY, "attributes": attrs,
                "messageAttributes": {}, "md5OfBody": hashlib.md5(BODY.encode()).hexdigest(),
                "eventSource": "aws:sqs",
                "eventSourceARN": f"arn:aws:sqs:{REGION}:{ACCOUNT}:{a['messaging.destination.name']}",
                "awsRegion": REGION,
            })
        ids = [r["messageId"] for r in records]
        expected = {i: inv["outcomes"].get(i) for i in ids}
        values = set(expected.values())
        if values == {None}:
            behaviour = {"kind": "killed"}
        elif values == {"failed_exception"}:
            behaviour = {"kind": "raise", "error_type": sorted(inv["errors"])[0]}
        elif values == {"failed_invalid_response"}:
            behaviour = {"kind": "return", "batchItemFailures": ["not-a-message-id"]}
        else:
            behaviour = {"kind": "return", "batchItemFailures": [i for i in ids if expected[i] == "failed_reported"]}
        out.append({"aws_request_id": req, "log_stream_name": streams.get(req), "function_name": "qv2-exp-fn",
                    "event": {"Records": records}, "behaviour": behaviour, "expected": expected})

    doc = {
        "provenance": __doc__.strip(),
        "source": "QV-9 evidence bundle, raw__lambda_logs.json",
        "invocations": out,
    }
    dest = Path(__file__).with_name("qv9_lambda_events.json")
    dest.write_text(json.dumps(doc, indent=1) + "\n", "utf-8")
    print(f"{len(out)} invocations, {sum(len(i['event']['Records']) for i in out)} records -> {dest}")


if __name__ == "__main__":
    main(sys.argv[1])
