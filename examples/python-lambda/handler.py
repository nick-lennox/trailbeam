"""A Lambda function with two SQS triggers: `orders` and its dead-letter queue `orders-dlq`.

An order that fails is listed in `batchItemFailures` (ReportBatchItemFailures on the trigger), so only
that record comes back after the visibility timeout (5 s here); after 3 receives SQS moves it to
`orders-dlq`, and the DLQ trigger logs it."""
import json

import trailbeam.aws_lambda


def process(order, receive_count):
    if order["simulate"] == "always_fail" or (order["simulate"] == "fail_once" and receive_count == 1):
        raise RuntimeError(f"payment service unavailable for {order['order_id']}")
    print(f"processed {order['order_id']} on receive {receive_count}")


@trailbeam.aws_lambda.wrap
def handler(event, context):
    failures = []
    for record in event["Records"]:
        if record["eventSourceARN"].endswith(":orders-dlq"):
            # Receiving it is what lets Trailbeam show the move to the DLQ (SQS reports it on this receive).
            print(f"dead letter {record['messageId']}: {record['body']}")
            continue
        try:
            process(json.loads(record["body"]), int(record["attributes"]["ApproximateReceiveCount"]))
        except Exception as e:
            print(f"failed, leaving it for a retry: {e}")
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
