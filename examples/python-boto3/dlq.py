"""Read the dead-letter queue `orders-dlq`: log each dead letter, then delete it.

Receiving here is what lets Trailbeam show the move to the DLQ: SQS reports it with the
DeadLetterQueueSourceArn attribute on this receive, and the trailbeam hooks record it."""
import os

import boto3
import trailbeam.boto

trailbeam.boto.install()
sqs = boto3.client("sqs", endpoint_url=os.environ.get("SQS_ENDPOINT"))
dlq_url = sqs.get_queue_url(QueueName="orders-dlq")["QueueUrl"]

while True:
    for msg in sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10, WaitTimeSeconds=10).get("Messages", []):
        print(f"dead letter {msg['MessageId']}: {msg['Body']}", flush=True)
        sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=msg["ReceiptHandle"])
