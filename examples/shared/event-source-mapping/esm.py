"""A stand-in for Lambda's SQS event source mapping, for running the Lambda examples on one machine.

In AWS, Lambda's own poller receives from the queue, invokes your function with a batch of records,
and deletes the records that succeeded. This script does the same against the SQS emulator and the
function container's Runtime Interface Emulator, with ReportBatchItemFailures on: a record listed in
the returned `batchItemFailures` is not deleted, and an invocation that throws deletes nothing. It has
no Trailbeam hooks on purpose: AWS's poller has none either, so Trailbeam sees what the function sees.

Settings (environment): SQS_ENDPOINT, QUEUES (comma-separated, each one a trigger of the function),
FUNCTION_URL (the emulator's invoke URL), BATCH_SIZE (default 10).
"""
import json
import os
import threading
import urllib.request

import boto3

sqs = boto3.client("sqs", endpoint_url=os.environ["SQS_ENDPOINT"])
function_url = os.environ["FUNCTION_URL"]
batch_size = int(os.environ.get("BATCH_SIZE", "10"))
invoke_lock = threading.Lock()  # the Runtime Interface Emulator runs one invocation at a time


def record(msg, arn, region):
    return {
        "messageId": msg["MessageId"],
        "receiptHandle": msg["ReceiptHandle"],
        "body": msg["Body"],
        "attributes": msg.get("Attributes", {}),
        "messageAttributes": {
            k: {"stringValue": v.get("StringValue"), "binaryValue": v.get("BinaryValue"), "stringListValues": [],
                "binaryListValues": [], "dataType": v["DataType"]}
            for k, v in msg.get("MessageAttributes", {}).items()},
        "md5OfBody": msg["MD5OfBody"],
        "eventSource": "aws:sqs",
        "eventSourceARN": arn,
        "awsRegion": region,
    }


def invoke(event):
    req = urllib.request.Request(function_url, data=json.dumps(event).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    with invoke_lock, urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read() or b"null")


def poll(queue):
    url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    region = arn.split(":")[3]
    while True:
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=batch_size, WaitTimeSeconds=10,
                                   MessageSystemAttributeNames=["All"], MessageAttributeNames=["All"]
                                   ).get("Messages", [])
        if not msgs:
            continue
        try:
            result = invoke({"Records": [record(m, arn, region) for m in msgs]})
        except OSError as e:
            print(f"{queue}: invoke failed, the batch comes back after the visibility timeout: {e}", flush=True)
            continue
        if isinstance(result, dict) and "errorType" in result:
            print(f"{queue}: the function threw {result['errorType']}, nothing deleted", flush=True)
            continue
        failed = {f["itemIdentifier"] for f in (result or {}).get("batchItemFailures") or []}
        done = [m for m in msgs if m["MessageId"] not in failed]
        if done:
            sqs.delete_message_batch(QueueUrl=url, Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(done)])
        print(f"{queue}: {len(msgs)} records, {len(done)} deleted, {len(failed)} left for a retry", flush=True)


threads = [threading.Thread(target=poll, args=(q.strip(),), daemon=True) for q in os.environ["QUEUES"].split(",")]
for t in threads:
    t.start()
for t in threads:
    t.join()
