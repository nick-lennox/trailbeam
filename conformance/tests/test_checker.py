"""The checker against recorded runs of the Python and Node.js packages (fixtures/, made with
make_fixture.py), and against those runs broken one contract rule at a time: each broken run must fail with
a line that says what is wrong."""
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

FIXTURES = ["python-app-otel", "node-app-otel"]


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


def ours(spans, name=None, op=None):
    return [s for s in spans if s.scope == "trailbeam" and (name is None or s.name == name)
            and (op is None or s.get("messaging.operation.name") == op)]


def setattr_(span, key, typ, value):
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
    assert report.expected == report.matched == report.spans == 20


def test_contract_hmac_vectors():
    vectors = json.loads((KIT.parent / "contract" / "span-contract-vectors.json").read_text())
    cases = vectors.get("receipt_handle_hmac") or vectors.get("hmac") or vectors.get("vectors")
    assert cases, "no HMAC vectors found in span-contract-vectors.json"
    for v in cases:
        assert checker.receipt_hmac(v["key"], v["receipt_handle"]) == v["hmac"]
        if "key_id" in v:
            assert checker.key_id(v["key"]) == v["key_id"]


def test_wrong_hmac_on_a_delete(run):
    def m(spans, calls):
        d = ours(spans, op="DeleteMessage")[0]
        setattr_(d, "trailbeam.receipt_handle.hmac", "string", "0" * 32)
    text = broken(run, m)
    assert "call 4 DeleteMessage on kit-fixture-" in text
    assert "trailbeam.receipt_handle.hmac: expected " in text and "got 00000000000000000000000000000000" in text
    assert "missing span" not in text and "unexpected span" not in text


def test_hashed_with_another_key(run):
    def m(spans, calls):
        for s in ours(spans):
            if s.get("trailbeam.receipt_handle.key_id"):
                setattr_(s, "trailbeam.receipt_handle.key_id", "string", "b5e633f7")
    assert "every receipt-handle hash was made with another key (key ID b5e633f7" in broken(run, m)


def test_span_no_call_explains(run):
    def m(spans, calls):
        extra = copy.deepcopy(ours(spans, op="DeleteMessage")[0])
        setattr_(extra, "messaging.operation.name", "string", "ChangeMessageVisibility")
        setattr_(extra, "trailbeam.batch_entry_id", "string", "zz")
        spans.append(extra)
    assert "unexpected span: no call in the ground truth explains it" in broken(run, m)


def test_missing_receive(run):
    def m(spans, calls):
        spans.remove(ours(spans, op="ReceiveMessage")[2])
    text = broken(run, m)
    assert "missing span: no 'trailbeam receive' was exported with messaging.message.id = " in text
    assert "19 of 20 expected spans found" in text


def test_two_spans_for_one_message(run):
    def m(spans, calls):
        spans.append(copy.deepcopy(ours(spans, op="SendMessageBatch")[0]))
    assert "duplicate span" in broken(run, m)


def test_wrong_receive_count(run):
    def m(spans, calls):
        r = [s for s in ours(spans, op="ReceiveMessage") if s.get("trailbeam.receive_count") == 2][0]
        setattr_(r, "trailbeam.receive_count", "int", 1)
    assert "trailbeam.receive_count: expected 2, got 1" in broken(run, m)


def test_int_sent_as_string(run):
    def m(spans, calls):
        r = ours(spans, op="ReceiveMessage")[0]
        setattr_(r, "trailbeam.receive_count", "string", str(r.get("trailbeam.receive_count")))
    assert "trailbeam.receive_count: expected int 1, got string" in broken(run, m)


def test_failed_entry_reported_ok(run):
    def m(spans, calls):
        bad = [s for s in ours(spans, op="DeleteMessageBatch") if s.get("trailbeam.batch_entry_id") == "bogus"][0]
        setattr_(bad, "trailbeam.result", "string", "ok")
        del bad.attributes["trailbeam.error_code"]
    text = broken(run, m)
    assert "entry bogus" in text
    assert "trailbeam.result: expected failed, got ok" in text
    assert "trailbeam.error_code: missing" in text


def test_visibility_timeout_zero_dropped(run):
    def m(spans, calls):
        del ours(spans, op="ChangeMessageVisibility")[0].attributes["trailbeam.visibility_timeout"]
    assert "trailbeam.visibility_timeout: missing (expected 0)" in broken(run, m)


def test_dlq_source_missing(run):
    def m(spans, calls):
        r = [s for s in ours(spans, op="ReceiveMessage") if s.get("trailbeam.dead_letter_queue_source_arn")][0]
        del r.attributes["trailbeam.dead_letter_queue_source_arn"]
    assert "trailbeam.dead_letter_queue_source_arn: missing (expected arn:aws:sqs:" in broken(run, m)


def test_call_order_reversed(run):
    def m(spans, calls):
        # The delete after the receives claims to have been issued before the receive returned.
        d = ours(spans, op="DeleteMessage")[0]
        setattr_(d, "trailbeam.order.issued", "int", 1)
    text = broken(run, m)
    assert "call order: call 3 returned at tick" in text and "this call, made after it, was issued at tick 1" in text


def test_spans_of_one_call_disagree(run):
    def m(spans, calls):
        b = ours(spans, op="SendMessageBatch")[1]
        setattr_(b, "trailbeam.order.returned", "int", b.get("trailbeam.order.returned") + 1)
    assert "spans of one call differ in trailbeam.order.returned" in broken(run, m)


def test_empty_string_instead_of_absent(run):
    def m(spans, calls):
        setattr_(ours(spans, op="ReceiveMessage")[0], "trailbeam.aws_trace_header", "string", "")
    assert 'trailbeam.aws_trace_header: sent as ""; an attribute without a value must be omitted' in broken(run, m)


def test_misspelt_attribute(run):
    def m(spans, calls):
        setattr_(ours(spans, op="ReceiveMessage")[0], "trailbeam.receipt_handle.sha256", "string", "abc")
    assert "trailbeam.receipt_handle.sha256: not an attribute of the contract" in broken(run, m)


def test_raw_handle_exported(run):
    def m(spans, calls):
        handle = calls[2]["messages"][0]["receipt_handle"]
        setattr_(ours(spans, op="ReceiveMessage")[0], "trailbeam.aws_trace_header", "string", handle)
    assert "a raw receipt handle appears in an exported attribute" in broken(run, m)


def test_wrong_message_id_on_settle(run):
    def m(spans, calls):
        setattr_(ours(spans, op="DeleteMessage")[0], "messaging.message.id", "string", "not-the-id")
    assert "messaging.message.id: expected string" in broken(run, m)


def test_message_id_on_unknown_handle(run):
    def m(spans, calls):
        inv = [s for s in ours(spans, op="DeleteMessage") if s.get("trailbeam.result") == "failed"][0]
        setattr_(inv, "messaging.message.id", "string", "made-up")
    assert "messaging.message.id: must not be set here" in broken(run, m)


def test_span_name_with_queue(run):
    def m(spans, calls):
        d = ours(spans, op="DeleteMessage")[0]
        d.name = f"trailbeam delete {d.get('messaging.destination.name')}"
    assert "span name: expected 'trailbeam delete', got 'trailbeam delete kit-fixture-" in broken(run, m)


def test_client_id_not_instance_id(run):
    def m(spans, calls):
        setattr_(ours(spans, op="SendMessage")[0], "messaging.client.id", "string", "someone-else")
    assert "is not the resource's service.instance.id" in broken(run, m)


def test_purge_missing(run):
    def m(spans, calls):
        spans.remove(ours(spans, op="PurgeQueue")[0])
    assert "call 17 PurgeQueue on kit-fixture-" in broken(run, m)


def test_no_spans_at_all(run):
    def m(spans, calls):
        spans[:] = [s for s in spans if s.scope != "trailbeam"]
    text = broken(run, m)
    assert "no span with instrumentation scope 'trailbeam'" in text
    assert "0 of 20 expected spans found" in text


def test_hooks_did_not_ask_for_system_attributes(run):
    def m(spans, calls):
        for c in calls:
            for msg in c.get("messages", []):
                msg["attributes"].pop("ApproximateReceiveCount", None)
    assert "SQS's response has no ApproximateReceiveCount: the package did not add them" in broken(run, m)


def test_driver_skipped_a_step(run):
    def m(spans, calls):
        calls[:] = [c for c in calls if c["op"] != "ChangeMessageVisibilityBatch"]
    text = broken(run, m)
    assert "the ground truth has no ChangeMessageVisibilityBatch: the driver did not run the whole scenario" in text


def test_parent_not_exported(run):
    def m(spans, calls):
        spans[:] = [s for s in spans if s.scope == "trailbeam"]
    assert "spans have a parent that was not exported" in broken(run, m)


def _orphan(op):
    def m(spans, calls):
        for s in ours(spans, op=op):
            s.parent_span_id = ""
    return m


def test_receive_roots_allows_root_receives_only(run):
    # A poll loop under a Java agent: the receives are roots, everything else is still a child of a stock span.
    settings, header, calls, spans = copy.deepcopy(run)
    _orphan("ReceiveMessage")(spans, calls)
    assert "spans have no parent" in checker.render(run_check(settings, header, calls, spans))
    report = run_check(settings, header, calls, spans, receive_roots=True)
    assert report.ok, checker.render(report)


def test_receive_roots_still_checks_other_operations(run):
    text = broken(run, _orphan("DeleteMessage"), receive_roots=True)
    assert "4 of 20 Trailbeam spans have no parent" in text
    assert "receives may be roots in this variant; this is another operation" in text


def test_receive_roots_still_checks_a_receive_parent_that_was_not_exported(run):
    def m(spans, calls):  # drop the stock spans the receives are children of
        stock_parents = {s.parent_span_id for s in ours(spans, op="ReceiveMessage")}
        spans[:] = [s for s in spans if s.scope == "trailbeam" or s.span_id not in stock_parents]
    assert "spans have a parent that was not exported" in broken(run, m, receive_roots=True)


def test_sampled_run_that_did_not_sample(run):
    text = broken(run, lambda spans, calls: None, parent="unexported")
    assert "the application's sampler was set to 0, yet" in text


def test_capture_expected_but_off(run):
    text = broken(run, lambda spans, calls: None, capture=True)
    assert "trailbeam.body.capture_status: expected captured, got off" in text


def test_cli_check_exit_codes(tmp_path):
    name = FIXTURES[0]
    f = HERE / "fixtures"
    base = [sys.executable, str(KIT / "kit.py"), "check", "--truth", str(f / f"{name}.truth.jsonl"),
            "--settings", str(f / f"{name}.run.json")]
    ok = subprocess.run(base + ["--spans", str(f / f"{name}.spans.jsonl")], capture_output=True, text=True)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "PASS" in ok.stdout
    empty = tmp_path / "spans.jsonl"
    empty.write_text("")
    bad = subprocess.run(base + ["--spans", str(empty), "--json"], capture_output=True, text=True)
    assert bad.returncode == 1
    assert json.loads(bad.stdout)["ok"] is False
