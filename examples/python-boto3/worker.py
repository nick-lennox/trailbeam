"""Process orders from the `orders` queue. A failed order is not deleted, so SQS delivers it again after
the visibility timeout (5 s here), and moves it to `orders-dlq` after 3 receives."""
import json
import os

import boto3
import trailbeam.boto

trailbeam.boto.install()
sqs = boto3.client("sqs", endpoint_url=os.environ.get("SQS_ENDPOINT"))
queue_url = sqs.get_queue_url(QueueName="orders")["QueueUrl"]


def process(order, receive_count):
    if order["simulate"] == "always_fail" or (order["simulate"] == "fail_once" and receive_count == 1):
        raise RuntimeError(f"payment service unavailable for {order['order_id']}")
    print(f"processed {order['order_id']} on receive {receive_count}", flush=True)


while True:
    resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=10,
                               MessageSystemAttributeNames=["ApproximateReceiveCount"])
    for msg in resp.get("Messages", []):
        try:
            process(json.loads(msg["Body"]), int(msg["Attributes"]["ApproximateReceiveCount"]))
        except Exception as e:
            print(f"failed, leaving it for a retry: {e}", flush=True)
            continue
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
