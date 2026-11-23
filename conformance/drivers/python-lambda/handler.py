"""The conformance kit's Lambda function (Python), wrapped the way the README tells users to.

What it does with a batch is set by each record's body, {"case": ...} (SCENARIO-LAMBDA.md): it throws if
any record is `throw`, returns an invalid batchItemFailures if any is `invalid`, and otherwise lists the
`report` and `dlq` records as failures. On the dead-letter queue it succeeds.

Its first act is one log line the poller reads (the "KIT_HANDLER" line): when the handler started, and
the request ID and function name the runtime gave it. The checker compares that start time with the
moment the Collector received each receive span, to see that the wrapper flushed them first.
"""
import json
import os
import time

import trailbeam.aws_lambda


@trailbeam.aws_lambda.wrap
def handler(event, context):
    started = time.time_ns()
    records = event.get("Records", [])
    print("KIT_HANDLER " + json.dumps({
        "started_unix_nano": started, "request_id": context.aws_request_id,
        "function_name": context.function_name, "log_stream": os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME"),
        "message_ids": [r["messageId"] for r in records]}), flush=True)
    if records and all(r["eventSourceARN"].endswith("-dlq") for r in records):
        return {"batchItemFailures": []}
    cases = [json.loads(r["body"]).get("case") for r in records]
    if "throw" in cases:
        raise RuntimeError("conformance kit: the handler threw (case throw)")
    if "invalid" in cases:
        return {"batchItemFailures": [{"itemIdentifier": "not-a-record-of-this-batch"}]}
    failures = [{"itemIdentifier": r["messageId"]} for r, c in zip(records, cases) if c in ("report", "dlq")]
    if not failures and cases == ["success"]:
        return None  # a plain return: no batchItemFailures at all
    return {"batchItemFailures": failures}
