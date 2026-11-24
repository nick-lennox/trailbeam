# The conformance scenario and the ground-truth file

Every driver runs exactly this scenario, with the language's usual SQS SDK and the Trailbeam package
installed the way its README tells users to. The calls are made one after the other from one thread or
async task: the checker relies on "call *n* returned before call *n + 1* was issued".

The kit sets `KIT_RUN` (a unique name, written *R* below), `KIT_SQS_ENDPOINT` and `KIT_TRUTH_FILE`, and
the variables the package reads (README.md, "What the kit sets"). The driver must not set any
`TRAILBEAM_*` variable itself.

## Steps

1. **Queues.** CreateQueue `R-dlq`; GetQueueAttributes `QueueArn`. CreateQueue `R` with
   `VisibilityTimeout = 2` and `RedrivePolicy = {"deadLetterTargetArn": <R-dlq's ARN>, "maxReceiveCount": "2"}`.
   CreateQueue `R-purge`. (These calls are not recorded and produce no Trailbeam span.)
2. **Send.** SendMessage to `R`, body `{"n": 1}`. SendMessageBatch to `R`: entries `e2` and `e3`,
   bodies `{"n": 2}` and `{"n": 3}`.
3. **Receive.** ReceiveMessage on `R` (`MaxNumberOfMessages = 10`, `WaitTimeSeconds = 1`) until n = 1, 2
   and 3 have all been seen. These receives ask for `MessageSystemAttributeNames = ["SenderId"]` and
   nothing else, so the kit can see that the package adds its own names without removing the caller's.
   **No other receive asks for any attribute**: the package must add what it needs (contract 4.2).
4. **Settle.** DeleteMessage n = 1. DeleteMessageBatch with entry `d2` (n = 2's handle) and entry
   `bogus` (handle `not-a-receipt-handle`, which SQS fails).
5. **Visibility-timeout retry.** Sleep 3 s. ReceiveMessage on `R` until n = 3 comes back (receive count 2).
   Do not delete it.
6. **Dead-letter queue.** Sleep 3 s. ReceiveMessage on `R` once: ElasticMQ moves n = 3 to `R-dlq` here.
   ReceiveMessage on `R-dlq` until n = 3 arrives; DeleteMessage it on `R-dlq`.
7. **Change visibility.** SendMessage n = 4 to `R`; receive it; ChangeMessageVisibility with
   `VisibilityTimeout = 0`; receive it again; ChangeMessageVisibilityBatch with entry `v4` and
   `VisibilityTimeout = 30`; DeleteMessage it.
8. **A failing call.** DeleteMessage on `R` with handle `definitely-not-a-valid-receipt-handle`. The SDK
   raises; the driver catches it and records the error code.
9. **Purge.** PurgeQueue `R-purge`.
10. **Flush.** Shut the Trailbeam package down (or flush it) so every span is exported, shut down the
    application's own tracer provider if the variant has one, then exit 0.

The Python driver ([`drivers/python/driver.py`](drivers/python/driver.py)) and the Node.js driver
([`drivers/node/driver.js`](drivers/node/driver.js)) are the reference: about 150 lines each.

### What the checker expects from it (20 spans)

| Step | Spans |
|---|---|
| 2 | 3 `trailbeam send` (one per message) |
| 3 | 3 `trailbeam receive` (more if a receive returned a message twice; the truth says) |
| 4 | 1 `trailbeam delete`; 2 `trailbeam delete` with `trailbeam.batch_entry_id`, `bogus` with `trailbeam.result = failed` |
| 5 | 1 `trailbeam receive` with `trailbeam.receive_count = 2` |
| 6 | 1 `trailbeam receive` on `R-dlq` with `trailbeam.dead_letter_queue_source_arn`; 1 `trailbeam delete` |
| 7 | 1 send, 2 receives, 2 `trailbeam change_visibility` (`trailbeam.visibility_timeout` 0 and 30), 1 delete |
| 8 | 1 `trailbeam delete`, `trailbeam.result = failed`, with `trailbeam.error_code` |
| 9 | 1 `trailbeam purge` |

The checker builds the list from the ground truth, not from this table, so a receive that returned
nothing (no span) or a message delivered twice is accounted for.

## Ground-truth file

JSON Lines at `$KIT_TRUTH_FILE`, written as the calls are made. Receipt handles are written **raw**: the
checker recomputes each HMAC from them with the run's key, so it needs no language-specific code. The file
never leaves the machine the kit runs on (README.md, "Receipt handles").

The first line is the header:

```json
{"truth": 1, "driver": "python", "language": "python", "sdk": "boto3 1.44.9", "package": "trailbeam 0.4.0"}
```

`truth` (the format version, `1`) and `driver` are required; the rest is free-form and shown in reports.
Then one line per SQS call of steps 2 to 9, in the order the calls were made. `call` (1, 2, 3, ...) may be
given; otherwise the line number is used.

| `op` | Fields |
|---|---|
| `SendMessage` | `queue_url`, `ok`, `message_id`, `body` |
| `SendMessageBatch` | `queue_url`, `ok`, `entries: [{id, ok, message_id, body}]`; a failed entry `{id, ok: false, error_code}` |
| `ReceiveMessage` | `queue_url`, `ok`, `asked_system_attributes` (what the driver asked for), `messages: [{message_id, receipt_handle, body, attributes}]`, where `attributes` is the message's system attributes exactly as SQS returned them (string values) |
| `DeleteMessage` | `queue_url`, `receipt_handle`, `ok`, `error_code` when not ok |
| `DeleteMessageBatch` | `queue_url`, `ok`, `entries: [{id, receipt_handle, ok, error_code}]` |
| `ChangeMessageVisibility` | `queue_url`, `receipt_handle`, `visibility_timeout`, `ok`, `error_code` when not ok |
| `ChangeMessageVisibilityBatch` | `queue_url`, `ok`, `entries: [{id, receipt_handle, visibility_timeout, ok, error_code}]` |
| `PurgeQueue` | `queue_url`, `ok`, `error_code` when not ok |

`queue_url` is the URL the driver passed to the SDK, verbatim. `error_code` is the error code as the SDK
reports it (the `Code` of a failed batch entry, or of the exception).

Everything in the truth comes from the SDK's own requests and responses, never from the Trailbeam
package: the truth is what SQS did, and the spans are what the package said it did.
