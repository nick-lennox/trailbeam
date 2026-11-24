"""Make test/fixtures/causes-parity.json: timelines, and what the cause classifier and grouping make of them.

Self-hosted Trailbeam (python/src/trailbeam/server/causes.py) and the hosted app (lib/app/causes.ts) must group
unfinished messages the same way, so the shared Needs attention view reads either (QV-367). Both test themselves
on this file: python/tests/server/test_causes.py, and the app through @trailbeam/ui/fixtures/causes-parity.json.
Each case is one message's timeline (the shape GET /api/messages/{id} returns, cut to what the classifier reads),
with its queue URL, tenant, FIFO group and the queues' visibility timeouts where known. ``expected`` is
causes.cause_of for each case, then causes.group over all of them (and over a time range, as in_range does).
Every rule has a case, including the ones only the app's AWS records can trigger (aws_step events).

    pip install -e "python[server]"   # from the repository root
    python ui/test/make-causes-fixture.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from trailbeam.server import causes

ROOT = Path(__file__).resolve().parents[2]
T0 = 1_763_080_000_000  # ms
NOW = T0 + 3_600_000
URL = "https://sqs.us-east-1.amazonaws.com/111111111111/"


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def end(ms: int) -> dict:
    return {"end_unix_nano": str(ms * 1_000_000)}


def recv(d: int, ms: int, service="orders-worker", queue="orders", version=None, **extra) -> dict:
    consumer = {"service_name": service, "faas_invocation_id": f"inv{d}", **({"service_version": version} if version else {})}
    return {"type": "received", "delivery": d, "queue": queue, "receive_count": d, "consumer": consumer, **end(ms), **extra}


def tl(mid: str, events: list, queue="orders", sent=True, **extra) -> dict:
    head = [{"type": "sent", "queue": queue, **end(T0)}] if sent else []
    return {"message_id": mid, "source_queue": queue, "current_queue": queue, "in_dead_letter_queue": False,
            "dead_letter_queue_move": None, "state": "not_deleted", "deliveries": sum(e["type"] == "received" for e in events),
            "first_seen": iso(T0), "events": head + events, **extra}


def exc(d: int, ms: int, typ: str, text=None) -> dict:
    return {"type": "lambda_outcome", "delivery": d, "outcome": "failed_exception", "error_type": typ,
            **({"error_message": text} if text else {}), **end(ms)}


def case(name: str, t: dict, queue_url=URL + "orders", tenant=None, group=None, visibility_s=None) -> dict:
    return {"name": name, "timeline": t, "queueUrl": queue_url, "tenant": tenant, "group": group,
            "visibilityS": visibility_s}


S = 1000
CASES = [
    case("rule 1: an event filter's drop (CloudTrail) is filtered, whatever else happened",
         tl("m-filter", [recv(1, T0 + S), {"type": "aws_step", "aws_kind": "filter_drop", "function_name": "thumbs",
                                           "time": iso(T0 + 5 * S)}])),
    case("rule 2: the function's log says it timed out",
         tl("m-timeout", [recv(1, T0 + S), {"type": "aws_step", "aws_kind": "timeout", "delivery": 1,
                                            "function_name": "billing-fn", "time": iso(T0 + 11 * S)}])),
    case("rule 3: out of memory",
         tl("m-oom", [recv(1, T0 + S), {"type": "aws_step", "aws_kind": "out_of_memory", "delivery": 1,
                                        "function_name": "billing-fn", "time": iso(T0 + 3 * S)}])),
    case("rule 4: an exception, with error text that varies by a number",
         tl("m-exc-1", [recv(1, T0 + S, version="1.4.2"), exc(1, T0 + 2 * S, "TimeoutError", "Stripe timed out after 10000 ms")]),
         tenant="acme"),
    case("rule 4: the same fingerprint, so the same group, on a later version and another tenant",
         tl("m-exc-2", [recv(1, T0 + 60 * S, version="1.4.3"), exc(1, T0 + 61 * S, "TimeoutError", "Stripe timed out after 9000 ms")]),
         tenant="globex"),
    case("rule 4: an exception with no error text: the class alone",
         tl("m-exc-bare", [recv(1, T0 + S), exc(1, T0 + 2 * S, "KeyError")])),
    case("rule 5: an invalid batch response",
         tl("m-invalid", [recv(1, T0 + S), {"type": "lambda_outcome", "delivery": 1, "outcome": "failed_invalid_response",
                                            **end(T0 + 2 * S)}])),
    case("rule 6: reported in batchItemFailures, retried twice: the latest failed delivery decides, earlier ones are listed",
         tl("m-reported", [recv(1, T0 + S), exc(1, T0 + 2 * S, "ValueError", "bad"),
                           recv(2, T0 + 40 * S), {"type": "lambda_invocation_ended_without_outcome", "delivery": 2},
                           recv(3, T0 + 80 * S), {"type": "lambda_outcome", "delivery": 3, "outcome": "failed_reported",
                                                  **end(T0 + 81 * S)}])),
    case("rule 7: no outcome within the Lambda timeout, evidenced at the receive",
         tl("m-no-outcome", [recv(1, T0 + S), {"type": "lambda_invocation_ended_without_outcome", "delivery": 1,
                                               "window": {"from": iso(T0 + S), "to": iso(T0 + 901 * S)}}])),
    case("rule 8: a failed DeleteMessage, by its error code",
         tl("m-delete", [recv(1, T0 + S), {"type": "deleted", "delivery": 1, "result": "failed",
                                           "error_code": "ReceiptHandleIsInvalid", **end(T0 + 1500)}])),
    case("rule 9: received, a Lambda invocation still running: in flight",
         tl("m-processing", [recv(1, NOW - 10 * S)], state="processing")),
    case("rule 9: received, its visibility timeout (from a ChangeMessageVisibility) not out yet: in flight",
         tl("m-in-flight", [recv(1, NOW - 600 * S), {"type": "visibility_changed", "delivery": 1, "result": "ok",
                                                     "visibility_timeout_s": 3600, **end(NOW - 599 * S)}])),
    case("rule 10: received and not deleted, the queue's visibility timeout known",
         tl("m-not-deleted", [recv(1, T0 + S)]), visibility_s={"orders": 60}),
    case("rule 10: received and not deleted on a queue whose timeout isn't known: SQS's default is assumed",
         tl("m-not-deleted-2", [recv(1, T0 + 2 * S, queue="payments")], queue="payments"), queue_url=URL + "payments"),
    case("rule 11: FIFO, the group's first message (never received)",
         tl("m-fifo-1", [], queue="jobs.fifo"), queue_url=URL + "jobs.fifo", group="g1"),
    case("rule 11: FIFO, blocked behind it",
         tl("m-fifo-2", [], queue="jobs.fifo", first_seen=iso(T0 + 5 * S)), queue_url=URL + "jobs.fifo", group="g1"),
    case("rule 12: sent, never received, no send seen either: first seen is the evidence",
         tl("m-never", [], queue="audit", sent=False), queue_url=None),
    case("dead-lettered (the move seen), and the group's worst outcome",
         tl("m-dlq", [recv(1, T0 + 3 * S, version="1.4.2"), exc(1, T0 + 4 * S, "TimeoutError", "Stripe timed out after 12000 ms")],
            current_queue="orders-dlq", in_dead_letter_queue=True, dead_letter_queue_move="observed"), tenant="acme"),
    case("dead-lettered, inferred from maxReceiveCount",
         tl("m-dlq-inferred", [recv(1, T0 + S), exc(1, T0 + 2 * S, "KeyError"), recv(2, T0 + 40 * S), exc(2, T0 + 41 * S, "KeyError")],
            dead_letter_queue_move="not_seen_on_dlq_yet")),
    case("the same cause on a queue of the same name in another account is another group",
         tl("m-other-account", [recv(1, T0 + S), exc(1, T0 + 2 * S, "KeyError")]),
         queue_url="https://sqs.eu-west-1.amazonaws.com/222222222222/orders"),
] + [
    # more than MAX_FINGERPRINTS error texts for one exception class: one group for the class
    case(f"merged: error text {i}", tl(f"m-many-{i:02}", [recv(1, T0 + i * S, service="search"),
                                                         exc(1, T0 + i * S + 500, "LookupError", f"no index '{chr(97 + i)}'")]),
         queue_url=URL + "orders")
    for i in range(causes.MAX_FINGERPRINTS + 1)
]


def main() -> None:
    classified = []
    for c in CASES:
        x = causes.cause_of(c["timeline"], NOW, visibility_s=c["visibilityS"], queue_url=c["queueUrl"],
                            tenant=c["tenant"], group_id=c["group"])
        classified.append(x)
    first_seen = {c["timeline"]["message_id"]: causes._iso_ms(c["timeline"]["first_seen"]) for c in CASES}
    assign: dict = {}
    groups = causes.group(classified, first_seen, assign)
    frm, to = T0 + 30 * S, NOW
    ranged = causes.in_range(classified, first_seen, frm, to)
    out = {
        "note": __doc__.split("\n\n")[0] + " Made by ui/test/make-causes-fixture.py; NaN is written as null.",
        "nowMs": NOW,
        "cases": CASES,
        "firstSeenMs": first_seen,
        "expected": {
            "classified": [causes.public(x) for x in classified],
            "groups": [causes.public(g) for g in groups],
            "assign": assign,
            "range": {"fromMs": frm, "toMs": to, "groups": [causes.public(g) for g in ranged]},
            "trends": {g["id"]: causes.trend_of(g["times"], T0, NOW) for g in groups},
        },
    }
    (ROOT / "ui" / "test" / "fixtures" / "causes-parity.json").write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    print(len(CASES), "cases,", len(groups), "groups")


if __name__ == "__main__":
    main()
