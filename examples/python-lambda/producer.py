"""Send three orders to the `orders` queue, print a Trailbeam link for each, and exit."""
import json
import os
import urllib.parse

import boto3
import trailbeam.boto

trailbeam.boto.install()  # once per process, before the first SQS call
sqs = boto3.client("sqs", endpoint_url=os.environ.get("SQS_ENDPOINT"))
queue_url = sqs.get_queue_url(QueueName="orders")["QueueUrl"]
ui = os.environ.get("TRAILBEAM_UI", "http://127.0.0.1:4318")

# `simulate` tells the worker what to do with the order, so each timeline shows something different.
orders = [
    {"order_id": "A-1001", "simulate": "ok"},           # received and deleted
    {"order_id": "A-1002", "simulate": "fail_once"},    # fails once, comes back after the visibility timeout, then succeeds
    {"order_id": "A-1003", "simulate": "always_fail"},  # fails 3 times, then SQS moves it to orders-dlq
]
for order in orders:
    message_id = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(order))["MessageId"]
    print(f"sent {order['order_id']} ({order['simulate']}): {ui}/message?id={urllib.parse.quote(message_id)}", flush=True)
# trailbeam exports its spans when the process exits.
