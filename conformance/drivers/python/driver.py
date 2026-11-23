"""Conformance driver for the Python package (boto3 + trailbeam.boto). Runs SCENARIO.md and writes the
ground truth to $KIT_TRUTH_FILE. The kit sets every other variable (README.md, "What the kit sets").

KIT_APP_TRACING=otel: the application has its own tracing, as a real service would: a global tracer
provider (its sampler from OTEL_TRACES_SAMPLER) exporting over OTLP, and the stock botocore
instrumentation on it, set up before trailbeam.boto.install().
"""
import json
import os
import sys
import time

import boto3
import botocore
from botocore.exceptions import ClientError

RUN = os.environ["KIT_RUN"]
TRUTH = open(os.environ["KIT_TRUTH_FILE"], "w")


def write(row):
    TRUTH.write(json.dumps(row) + "\n")
    TRUTH.flush()


app_provider = None
if os.environ.get("KIT_APP_TRACING") == "otel":
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    app_provider = TracerProvider()  # sampler from OTEL_TRACES_SAMPLER / _ARG
    app_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(app_provider)
    BotocoreInstrumentor().instrument(tracer_provider=app_provider)

import trailbeam  # noqa: E402
import trailbeam.boto  # noqa: E402

provider = trailbeam.boto.install()
sqs = boto3.client("sqs", endpoint_url=os.environ["KIT_SQS_ENDPOINT"])

write({"truth": 1, "driver": "python", "language": "python", "sdk": f"boto3 {boto3.__version__}, botocore {botocore.__version__}",
       "package": f"trailbeam {getattr(trailbeam, '__version__', '?')}", "app_tracing": os.environ.get("KIT_APP_TRACING", "none"),
       "python": sys.version.split()[0]})


def error_code(e: ClientError) -> str:
    return e.response.get("Error", {}).get("Code") or type(e).__name__


def send(q, n):
    body = json.dumps({"n": n})
    r = sqs.send_message(QueueUrl=q, MessageBody=body)
    write({"op": "SendMessage", "queue_url": q, "ok": True, "message_id": r["MessageId"], "body": body})
    return r["MessageId"]


def receive(q, asked=None):
    params = {"QueueUrl": q, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 1}
    if asked:
        params["MessageSystemAttributeNames"] = list(asked)
    r = sqs.receive_message(**params)
    msgs = r.get("Messages", [])
    write({"op": "ReceiveMessage", "queue_url": q, "ok": True, "asked_system_attributes": list(asked or []),
           "messages": [{"message_id": m["MessageId"], "receipt_handle": m["ReceiptHandle"], "body": m["Body"],
                         "attributes": m.get("Attributes", {})} for m in msgs]})
    return msgs


def receive_until(q, want_ids, asked=None, tries=30):
    got = {}
    for _ in range(tries):
        for m in receive(q, asked):
            got[m["MessageId"]] = m
        if all(i in got for i in want_ids):
            return got
    raise SystemExit(f"timed out waiting for {want_ids} on {q}")


def delete(q, handle):
    row = {"op": "DeleteMessage", "queue_url": q, "receipt_handle": handle}
    try:
        sqs.delete_message(QueueUrl=q, ReceiptHandle=handle)
        row["ok"] = True
    except ClientError as e:
        row.update(ok=False, error_code=error_code(e))
    write(row)


def batch(op, q, entries, fn):
    r = fn(QueueUrl=q, Entries=entries)
    failed = {f["Id"]: f.get("Code") for f in r.get("Failed", [])}
    rows = []
    for e in entries:
        row = {"id": e["Id"], "receipt_handle": e["ReceiptHandle"], "ok": e["Id"] not in failed}
        if "VisibilityTimeout" in e:
            row["visibility_timeout"] = e["VisibilityTimeout"]
        if e["Id"] in failed:
            row["error_code"] = failed[e["Id"]]
        rows.append(row)
    write({"op": op, "queue_url": q, "ok": True, "entries": rows})


# 1. Queues
dlq = sqs.create_queue(QueueName=f"{RUN}-dlq")["QueueUrl"]
dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
q = sqs.create_queue(QueueName=RUN, Attributes={
    "VisibilityTimeout": "2",
    "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"})})["QueueUrl"]
pq = sqs.create_queue(QueueName=f"{RUN}-purge")["QueueUrl"]

# 2. Send and batch send
ids = {1: send(q, 1)}
bodies = {2: json.dumps({"n": 2}), 3: json.dumps({"n": 3})}
r = sqs.send_message_batch(QueueUrl=q, Entries=[{"Id": f"e{n}", "MessageBody": bodies[n]} for n in (2, 3)])
write({"op": "SendMessageBatch", "queue_url": q, "ok": True, "entries": [
    {"id": s["Id"], "ok": True, "message_id": s["MessageId"], "body": bodies[int(s["Id"][1:])]} for s in r["Successful"]]
    + [{"id": f["Id"], "ok": False, "error_code": f.get("Code")} for f in r.get("Failed", [])]})
for s in r["Successful"]:
    ids[int(s["Id"][1:])] = s["MessageId"]

# 3. Receive all three; the first receive asks for one system attribute of its own
got = receive_until(q, [ids[1], ids[2], ids[3]], asked=["SenderId"])

# 4. Delete n=1; batch delete n=2 and a bogus handle (one failed entry)
delete(q, got[ids[1]]["ReceiptHandle"])
batch("DeleteMessageBatch", q, [{"Id": "d2", "ReceiptHandle": got[ids[2]]["ReceiptHandle"]},
                                {"Id": "bogus", "ReceiptHandle": "not-a-receipt-handle"}], sqs.delete_message_batch)

# 5. Visibility-timeout retry: n=3 comes back with receive count 2
time.sleep(3)
receive_until(q, [ids[3]])

# 6. DLQ move: ElasticMQ moves n=3 on the next receive of the source queue
time.sleep(3)
receive(q)
d = receive_until(dlq, [ids[3]])
delete(dlq, d[ids[3]]["ReceiptHandle"])

# 7. ChangeMessageVisibility(0), receive again, ChangeMessageVisibilityBatch, delete
ids[4] = send(q, 4)
m = receive_until(q, [ids[4]])[ids[4]]
h = m["ReceiptHandle"]
row = {"op": "ChangeMessageVisibility", "queue_url": q, "receipt_handle": h, "visibility_timeout": 0}
sqs.change_message_visibility(QueueUrl=q, ReceiptHandle=h, VisibilityTimeout=0)
write({**row, "ok": True})
m = receive_until(q, [ids[4]])[ids[4]]
batch("ChangeMessageVisibilityBatch", q, [{"Id": "v4", "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 30}],
      sqs.change_message_visibility_batch)
delete(q, m["ReceiptHandle"])

# 8. A delete with an invalid handle: the call fails
delete(q, "definitely-not-a-valid-receipt-handle")

# 9. Purge
sqs.purge_queue(QueueUrl=pq)
write({"op": "PurgeQueue", "queue_url": pq, "ok": True})

# 10. Flush
trailbeam.boto.uninstall()  # flushes and shuts down the Trailbeam provider
if app_provider is not None:
    app_provider.shutdown()
TRUTH.close()
print(json.dumps({"driver": "python", "run": RUN, "ids": ids}))
