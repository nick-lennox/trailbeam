"""The checker's Lambda rules against recorded runs of the Python and Node.js wrappers (fixtures/*-lambda-*,
made with make_fixture.py), and against those runs broken one rule at a time: each broken run must fail
with a line that says what is wrong, on the invocation and record it is about."""
import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
KIT = HERE.parent
sys.path.insert(0, str(KIT))
import checker  # noqa: E402

FIXTURES = ["python-lambda-alone", "node-lambda-alone"]


def load(name):
    settings = json.loads((HERE / "fixtures" / f"{name}.run.json").read_text())
    header, calls = checker.read_truth((HERE / "fixtures" / f"{name}.truth.jsonl").read_text().splitlines())
    spans = checker.read_spans((HERE / "fixtures" / f"{name}.spans.jsonl").read_text().splitlines())
    return settings, header, calls, spans


def run_check(settings, header, calls, spans, **over):
    kw = dict(run=settings["run"], key=settings["key"], variant=settings["variant"], parent=settings["parent"],
              capture=settings["capture"])
    kw.update(over)
    return checker.check(header, calls, spans, **kw)


def ours(spans, name, case=None, calls=None, outcome=None):
    ids = None
    if case is not None:
        ids = {c["request_id"] for c in calls if c["case"] == case}
    return [s for s in spans if s.scope == "trailbeam" and s.name == f"trailbeam {name}"
            and (ids is None or s.get("faas.invocation_id") in ids)
            and (outcome is None or s.get("trailbeam.outcome") == outcome)]


def put(span, key, typ, value):
    span.attributes[key] = (typ, value)


@pytest.fixture(params=FIXTURES)
def run(request):
    return load(request.param)


def broken(run, mutate, **over):
    settings, header, calls, spans = copy.deepcopy(run)
    mutate(spans, calls)
    report = run_check(settings, header, calls, spans, **over)
    text = checker.render(report)
    assert not report.ok, text
    return text


def test_recorded_runs_pass(run):
    report = run_check(*run)
    assert report.ok, checker.render(report)
    # 6 invocations, 7 records: one receive and one outcome each.
    assert report.expected == report.matched == report.spans == 14


def test_outcomes_follow_lambdas_rules():
    rec = [{"message_id": "a"}, {"message_id": "b"}]
    assert checker.lambda_outcomes({"records": rec, "response": None}) == ({"a": "success", "b": "success"}, 0)
    assert checker.lambda_outcomes({"records": rec, "response": {"batchItemFailures": []}})[1] == 0
    assert checker.lambda_outcomes({"records": rec, "response": {"batchItemFailures": [{"itemIdentifier": "b"}]}}) == \
        ({"a": "success", "b": "failed_reported"}, 1)
    assert checker.lambda_outcomes({"records": rec, "response": {"batchItemFailures": [{"itemIdentifier": "z"}]}}) == \
        ({"a": "failed_invalid_response", "b": "failed_invalid_response"}, None)
    assert checker.lambda_outcomes({"records": rec, "response": {"batchItemFailures": [{"itemIdentifier": ""}]}})[1] is None
    assert checker.lambda_outcomes({"records": rec, "function_error": {"type": "E"}}) == \
        ({"a": "failed_exception", "b": "failed_exception"}, None)


def test_queue_url_from_arn():
    assert checker.queue_from_arn("arn:aws:sqs:eu-west-1:123456789012:orders") == \
        ("orders", "https://sqs.eu-west-1.amazonaws.com/123456789012/orders")


# -- trailbeam.source, operation, start_observed, zero-length receives, ticks -------------------------


def test_source_client(run):
    def m(spans, calls):
        put(ours(spans, "receive")[0], "trailbeam.source", "string", "client")
    assert "trailbeam.source: expected lambda, got client  [contract 3]" in broken(run, m)


def test_operation_receive_message(run):
    def m(spans, calls):
        put(ours(spans, "outcome")[0], "messaging.operation.name", "string", "ReceiveMessage")
    assert "messaging.operation.name: expected LambdaEvent, got ReceiveMessage" in broken(run, m)


def test_start_observed_missing(run):
    def m(spans, calls):
        del ours(spans, "receive")[0].attributes["trailbeam.start_observed"]
    assert "trailbeam.start_observed: missing (expected false)" in broken(run, m)


def test_start_observed_as_string(run):
    def m(spans, calls):
        put(ours(spans, "receive")[0], "trailbeam.start_observed", "string", "false")
    assert "trailbeam.start_observed: expected bool false, got string false" in broken(run, m)


def test_start_observed_on_outcome(run):
    def m(spans, calls):
        put(ours(spans, "outcome")[0], "trailbeam.start_observed", "bool", False)
    assert "trailbeam.start_observed: must not be set here, got false (the outcome's moment is observed)" in broken(run, m)


def test_receive_with_an_invented_start(run):
    def m(spans, calls):
        for s in ours(spans, "receive", "report", calls):
            s.start -= 50_000_000
    text = broken(run, m)
    assert "invocation (report)" in text
    assert "times: a zero-length span is required (its start is not observed, and the wrapper must not put any " \
           "other time in it); got start" in text and "50.000 ms apart" in text


def test_receive_with_an_issued_tick(run):
    def m(spans, calls):
        r = ours(spans, "receive")[0]
        put(r, "trailbeam.order.issued", "int", r.get("trailbeam.order.returned") - 1)
    assert "trailbeam.order.issued: must not be set here, got " in broken(run, m)
    assert "Lambda's poller made the ReceiveMessage call" in broken(run, m)


def test_receive_without_returned_tick(run):
    def m(spans, calls):
        del ours(spans, "receive")[0].attributes["trailbeam.order.returned"]
    assert "trailbeam.order.returned: missing  [contract 2a]" in broken(run, m)


def test_outcome_with_two_ticks(run):
    def m(spans, calls):
        o = ours(spans, "outcome")[0]
        put(o, "trailbeam.order.returned", "int", o.get("trailbeam.order.issued") + 1)
    assert "an outcome has one tick, issued = returned" in broken(run, m)


def test_outcome_ticked_before_its_receive(run):
    def m(spans, calls):
        for o in ours(spans, "outcome", "throw", calls):
            put(o, "trailbeam.order.issued", "int", 0)
            put(o, "trailbeam.order.returned", "int", 0)
    assert "call order: the receive's tick" in broken(run, m)


def test_receive_after_handler_started(run):
    def m(spans, calls):
        c = [c for c in calls if c["case"] == "success"][0]
        for r in ours(spans, "receive", "success", calls):
            r.start = r.end = c["handler"]["started_unix_nano"] + 20_000_000
    assert "after the handler started; it must be the moment the wrapper got the event" in broken(run, m)


# -- the outcome ----------------------------------------------------------------------------------------


def test_reported_failure_as_success(run):
    def m(spans, calls):
        put(ours(spans, "outcome", outcome="failed_reported")[0], "trailbeam.outcome", "string", "success")
    text = broken(run, m)
    assert "trailbeam.outcome: expected failed_reported, got success  [contract 4.4]" in text


def test_invalid_list_read_as_reported(run):
    def m(spans, calls):
        o = ours(spans, "outcome", "invalid", calls)[0]
        put(o, "trailbeam.outcome", "string", "failed_reported")
        put(o, "trailbeam.batch.reported_failure_count", "int", 1)
    text = broken(run, m)
    assert "invocation (invalid)" in text
    assert "trailbeam.outcome: expected failed_invalid_response, got failed_reported" in text
    assert "trailbeam.batch.reported_failure_count: must not be set here, got 1 (omitted when the list was invalid" in text


def test_outcome_value_not_in_the_contract(run):
    def m(spans, calls):
        put(ours(spans, "outcome", outcome="success")[0], "trailbeam.outcome", "string", "success_deleted_by_lambda")
    assert "trailbeam.outcome: expected success, got success_deleted_by_lambda" in broken(run, m)


def test_error_type_missing(run):
    def m(spans, calls):
        del ours(spans, "outcome", outcome="failed_exception")[0].attributes["trailbeam.error_type"]
    assert "trailbeam.error_type: missing (expected " in broken(run, m)


def test_error_type_wrong(run):
    def m(spans, calls):
        put(ours(spans, "outcome", outcome="failed_exception")[0], "trailbeam.error_type", "string", "Exception")
    assert "trailbeam.error_type: expected " in broken(run, m)


def test_error_message_without_error_text_on(run):
    def m(spans, calls):
        put(ours(spans, "outcome", outcome="failed_exception")[0], "trailbeam.error_message", "string", "boom")
    assert "trailbeam.error_message: must not be set here, got boom (error text is off for this queue)" in broken(run, m)


def test_missing_outcome(run):
    def m(spans, calls):
        spans.remove(ours(spans, "outcome", "report", calls)[0])
    text = broken(run, m)
    assert "missing span: no 'trailbeam outcome' was exported for this record (one per record after the handler" in text
    assert "13 of 14 expected spans found" in text


def test_missing_receive(run):
    def m(spans, calls):
        spans.remove(ours(spans, "receive", "dlq", calls)[0])
    text = broken(run, m)
    assert "invocation (dlq)" in text
    assert "(the wrapper must emit one per record before the handler runs)" in text


# -- faas, the queue, the batch -------------------------------------------------------------------------


def test_faas_name_missing(run):
    def m(spans, calls):
        del ours(spans, "receive")[0].attributes["faas.name"]
    assert "faas.name: missing (expected kit-fixture-" in broken(run, m)


def test_invocation_id_wrong(run):
    def m(spans, calls):
        put(ours(spans, "outcome", "report", calls)[0], "faas.invocation_id", "string", "not-the-request-id")
    text = broken(run, m)
    assert "faas.invocation_id: expected " in text and "got not-the-request-id" in text
    assert "missing span" not in text and "unexpected span" not in text


def test_queue_arn_missing(run):
    def m(spans, calls):
        del ours(spans, "receive")[0].attributes["trailbeam.queue.arn"]
    assert "trailbeam.queue.arn: missing (expected arn:aws:sqs:elasticmq:000000000000:kit-fixture-" in broken(run, m)


def test_queue_url_not_built_from_arn(run):
    def m(spans, calls):
        r = ours(spans, "receive")[0]
        put(r, "aws.sqs.queue.url", "string", "http://127.0.0.1:9324/000000000000/q")
    assert "aws.sqs.queue.url: expected https://sqs.elasticmq.amazonaws.com/000000000000/kit-fixture-" in broken(run, m)


def test_batch_index_wrong(run):
    def m(spans, calls):
        for r in ours(spans, "receive", "report", calls):
            put(r, "trailbeam.batch.index", "int", 0)
    assert "trailbeam.batch.index: expected 1, got 0  [contract 15.2]" in broken(run, m)


def test_batch_size_missing(run):
    def m(spans, calls):
        del ours(spans, "outcome")[0].attributes["trailbeam.batch.size"]
    assert "trailbeam.batch.size: missing (expected 1)" in broken(run, m)


def test_reported_failure_count_wrong(run):
    def m(spans, calls):
        for o in ours(spans, "outcome", "report", calls):
            put(o, "trailbeam.batch.reported_failure_count", "int", 2)
    assert "trailbeam.batch.reported_failure_count: expected 1, got 2" in broken(run, m)


def test_reported_failure_count_after_a_throw(run):
    def m(spans, calls):
        put(ours(spans, "outcome", "throw", calls)[0], "trailbeam.batch.reported_failure_count", "int", 0)
    assert "trailbeam.batch.reported_failure_count: must not be set here, got 0" in broken(run, m)


def test_dlq_source_arn_dropped(run):
    def m(spans, calls):
        del ours(spans, "receive", "dlq", calls)[0].attributes["trailbeam.dead_letter_queue_source_arn"]
    assert "trailbeam.dead_letter_queue_source_arn: missing (expected arn:aws:sqs:" in broken(run, m)


def test_receive_count_wrong(run):
    def m(spans, calls):
        put(ours(spans, "receive", "report-retry", calls)[0], "trailbeam.receive_count", "int", 1)
    assert "trailbeam.receive_count: expected 2, got 1" in broken(run, m)


def test_client_id_not_the_log_stream(run):
    def m(spans, calls):
        put(ours(spans, "receive")[0], "messaging.client.id", "string", "a-random-uuid")
    assert "messaging.client.id: expected 20" in broken(run, m)


# -- flushed before the handler ran ---------------------------------------------------------------------


def test_receive_not_flushed_before_the_handler(run):
    def m(spans, calls):
        # As a wrapper with a batch processor and no flush would look: the Collector got the receive
        # with the outcome, after the handler ran.
        c = [c for c in calls if c["case"] == "report"][0]
        for r in ours(spans, "receive", "report", calls):
            put(r, "kit.collector.received_unix_nano", "int", c["handler"]["started_unix_nano"] + 5_000_000_000)
    text = broken(run, m)
    assert "invocation (report)" in text
    assert "not flushed before the handler ran: the Collector received this receive span 5000.0 ms after the " \
           "handler started" in text


def test_flush_clock_tie_is_not_an_ordering_problem(run):
    # The Collector's and the handler's clocks agree to well under a millisecond, not exactly (QV-429: a
    # receive stamped 50 us after the handler's start failed the kit). Within CLOCK_SLACK_NS it passes; just
    # past it, it fails.
    def at(offset):
        def m(spans, calls):
            c = [c for c in calls if c["case"] == "throw"][0]
            for r in ours(spans, "receive", "throw", calls):
                put(r, "kit.collector.received_unix_nano", "int", c["handler"]["started_unix_nano"] + offset)
        return m

    for offset in (0, 50_000, checker.CLOCK_SLACK_NS):
        settings, header, calls, spans = copy.deepcopy(run)
        at(offset)(spans, calls)
        report = run_check(settings, header, calls, spans)
        assert report.ok, checker.render(report)
    text = broken(run, at(checker.CLOCK_SLACK_NS + 1_000))
    assert "invocation (throw)" in text
    assert "not flushed before the handler ran" in text


def test_bench_without_arrival_stamp(run):
    def m(spans, calls):
        for s in spans:
            s.attributes.pop("kit.collector.received_unix_nano", None)
    assert "the bench's Collector did not stamp it" in broken(run, m)


# -- the run as a whole -----------------------------------------------------------------------------------


def test_raw_handle_exported(run):
    def m(spans, calls):
        put(ours(spans, "receive")[0], "trailbeam.aws_trace_header", "string", calls[0]["records"][0]["receipt_handle"])
    assert "a raw receipt handle appears in an exported attribute" in broken(run, m)


def test_duplicate_outcome(run):
    def m(spans, calls):
        spans.append(copy.deepcopy(ours(spans, "outcome")[0]))
    assert "duplicate span: a second span for the same record in one invocation" in broken(run, m)


def test_driver_skipped_the_throw(run):
    def m(spans, calls):
        calls[:] = [c for c in calls if c["case"] != "throw"]
    assert "the ground truth has no failed_exception: the driver did not run the whole scenario" in broken(run, m)


def test_poller_deleted_a_failed_record(run):
    def m(spans, calls):
        c = [c for c in calls if c["case"] == "invalid"][0]
        c["deleted"] = [c["records"][0]["message_id"]]
    assert "but Lambda would delete []" in broken(run, m)


def test_capture_expected_but_off(run):
    text = broken(run, lambda spans, calls: None, capture=True)
    assert "trailbeam.body.capture_status: expected captured, got off" in text
    assert "trailbeam.error_message: missing (expected conformance kit: the handler threw (case throw))" in text


def test_cli_check(tmp_path):
    name = FIXTURES[0]
    f = HERE / "fixtures"
    base = [sys.executable, str(KIT / "kit.py"), "check", "--truth", str(f / f"{name}.truth.jsonl"),
            "--settings", str(f / f"{name}.run.json")]
    ok = subprocess.run(base + ["--spans", str(f / f"{name}.spans.jsonl")], capture_output=True, text=True)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "PASS, 14 of 14" in ok.stdout
