"""Checks the RabbitMQ part of the span contract (contract/span-contract-amqp.md, schema version 4).

This test holds a small reference reading of sections A4, A7, A8 and A12 and checks that the document, the
vectors and the fixtures agree with it and with each other. The server passes the same ingest vectors
(tests/server/test_amqp_ingest.py, QV-306) and stores the fixture (tests/conformance/test_amqp_conformance.py);
the packages must pass the emit vectors when they land (QV-309, QV-310).
"""
import importlib.util
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = (ROOT / "contract" / "span-contract-amqp.md").read_text("utf-8")
VECTORS = json.loads((ROOT / "contract" / "span-contract-amqp-vectors.json").read_text("utf-8"))
FIXTURE = ROOT / "contract" / "fixtures" / "amqp-4.0-spans.json"

ENUMS = {
    "trailbeam.amqp.cancelled_by": ("client", "broker"),
    "trailbeam.amqp.close.initiator": ("client", "broker", "network"),
    "trailbeam.receive_count.source": ("x-acquired-count", "x-delivery-count", "first_delivery"),
}
BOOLS = {"trailbeam.amqp.confirmed", "trailbeam.amqp.mandatory", "trailbeam.amqp.redelivered",
         "trailbeam.amqp.multiple", "trailbeam.amqp.requeue"}
INTS = {"rabbitmq.message.delivery_tag", "trailbeam.amqp.reply_code"}


def _section(title_start: str) -> str:
    start = CONTRACT.index(f"## {title_start}")
    end = CONTRACT.find("\n## ", start + 3)
    return CONTRACT[start:end if end != -1 else None]


def _span_table() -> dict:
    """kind -> (operation type, set of operation names), from the table at the top of A4."""
    out = {}
    for line in _section("A4.").splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 5 and cells[1].startswith("`amqp."):
            names = set(re.findall(r"`([a-z]+\.[a-z_-]+)`", cells[3]))
            out[cells[1].strip("`")] = (cells[2].strip("`"), names)
    return out


def _required_table() -> dict:
    """kind -> required attributes, from the table in A12 ("as `amqp.ack`, and ..." expanded)."""
    out = {}
    for line in _section("A12.").splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 2 and cells[0].startswith("`amqp."):
            kind = cells[0].strip("`")
            attrs = re.findall(r"`([a-z_.]+)`", cells[1])
            if cells[1].startswith("as "):
                attrs = out[attrs[0]] + attrs[1:]
            out[kind] = attrs
    return out


SPANS = _span_table()
REQUIRED = _required_table()


def _int(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and re.fullmatch(r"-?\d+", v):
        return int(v)
    return None


def check(attrs: dict) -> str:
    """The reference reading of A12 for amqp spans: "" if accepted, else the reason."""
    kind = attrs.get("trailbeam.kind", "")
    if not kind.startswith("amqp."):
        # Only the version rule of A1 is checked for other families; section 4.5 covers the rest.
        return "" if attrs.get("trailbeam.schema_version") in (2, 3, 4) else "schema version"
    version = attrs.get("trailbeam.schema_version")
    if version != 4:
        return f"amqp span with trailbeam.schema_version {version}; needs 4"
    system = attrs.get("messaging.system.name")
    if system != "rabbitmq":
        return f"amqp span with messaging.system.name {system!r}; needs 'rabbitmq'"
    op = attrs.get("messaging.operation.name")
    if kind not in SPANS or op not in SPANS[kind][1]:
        return f"unknown trailbeam.kind {kind!r} / messaging.operation.name {op!r}"
    missing = []
    for name in REQUIRED[kind]:
        v = attrs.get(name)
        ok = (isinstance(v, bool) if name in BOOLS else _int(v) is not None if name in INTS
              else isinstance(v, str) and v != "")
        if not ok:
            missing.append(name)
    if missing:
        return f"{kind} span without {', '.join(missing)}"
    for name, allowed in ENUMS.items():
        if name in attrs and attrs[name] not in allowed:
            return f"{kind} span with {name} {attrs[name]!r}; not one of {', '.join(allowed)}"
    if "trailbeam.result" in attrs and attrs["trailbeam.result"] not in ("ok", "failed"):
        return f"trailbeam.result {attrs['trailbeam.result']!r} is not ok or failed"
    return ""


def receive_count(redelivered: bool, headers: dict):
    """A7: (count, source), or (None, None). The counter headers count only on a redelivery or with a usable
    x-death; on any other delivery the publisher set them."""
    if not redelivered and not dead_letter(headers):
        headers = {}
    for header in ("x-acquired-count", "x-delivery-count"):
        v = headers.get(header)
        if isinstance(v, int) and not isinstance(v, bool):
            return v + 1, header
    if not redelivered:
        return 1, "first_delivery"
    return None, None


def dead_letter(headers: dict) -> dict:
    """A8: the trailbeam.dead_letter.* attributes."""
    deaths = headers.get("x-death")
    if not isinstance(deaths, list) or not deaths or not isinstance(deaths[0], dict):
        return {}
    d = deaths[0]
    if not d.get("queue") or not d.get("reason"):
        return {}
    out = {"trailbeam.dead_letter.source_queue": d["queue"], "trailbeam.dead_letter.reason": d["reason"],
           "trailbeam.dead_letter.count": d.get("count")}
    if "time" in d:
        out["trailbeam.dead_letter.time_ms"] = d["time"] * 1000
    if "exchange" in d:
        out["trailbeam.dead_letter.exchange"] = d["exchange"] or "amq.default"
    if headers.get("x-first-death-queue"):
        out["trailbeam.dead_letter.first_queue"] = headers["x-first-death-queue"]
    if headers.get("x-first-death-reason"):
        out["trailbeam.dead_letter.first_reason"] = headers["x-first-death-reason"]
    return {k: v for k, v in out.items() if v is not None}


def test_contract_tables_cover_every_kind():
    kinds = {"amqp.publish", "amqp.return", "amqp.deliver", "amqp.ack", "amqp.nack", "amqp.reject",
             "amqp.cancel", "amqp.channel_closed"}
    assert set(SPANS) == kinds
    assert set(REQUIRED) == kinds
    assert SPANS["amqp.deliver"] == ("receive", {"basic.deliver", "basic.get"})
    assert SPANS["amqp.channel_closed"][1] == {"channel.close", "connection.close", "connection.lost"}
    assert "trailbeam.amqp.requeue" in REQUIRED["amqp.nack"] and "trailbeam.amqp.multiple" in REQUIRED["amqp.nack"]
    for attrs in REQUIRED.values():
        assert "messaging.message.id" not in attrs and "trailbeam.receive_count" not in attrs  # A6, A7


def test_receive_count_vectors():
    for case in VECTORS["receive_count"]:
        assert receive_count(case["redelivered"], case["headers"]) == (case["receive_count"], case["source"]), case["name"]


def test_dead_letter_vectors():
    for case in VECTORS["dead_letter"]:
        assert dead_letter({"x-death": case["x-death"]}) == case["attributes"], case["name"]


def _process_attrs(kind: str) -> dict:
    extra = {name: "x" for name in VECTORS["emit_present"]}
    extra["process.pid"] = 1
    extra["trailbeam.order.returned"] = 2
    if kind not in ("amqp.publish", "amqp.return"):
        extra.update({name: "c" for name in VECTORS["emit_present_on_channel_spans"]})
    return extra


def test_emit_vectors_are_valid_spans():
    assert len(VECTORS["emit"]) >= 20
    for case in VECTORS["emit"]:
        for span in case["spans"]:
            attrs = span["attributes"]
            kind = attrs["trailbeam.kind"]
            assert span["span_name"] == "trailbeam " + kind.split(".", 1)[1], case["name"]
            assert attrs["messaging.operation.type"] == SPANS[kind][0], case["name"]
            assert check({**attrs, **_process_attrs(kind)}) == "", (case["name"], check({**attrs, **_process_attrs(kind)}))
            assert not set(span["absent"]) & set(attrs), case["name"]
            assert all(v not in ("", None) for v in attrs.values()), case["name"]  # absent values are omitted
            if attrs.get("trailbeam.start_observed") is False:  # A4: what the broker sent, or a close
                assert "trailbeam.order.issued" in span["absent"], case["name"]


def test_emit_deliveries_follow_a7_and_a8():
    seen = 0
    for case in VECTORS["emit"]:
        event = case["event"]
        if event.get("method") not in ("basic.deliver", "basic.get") or event.get("empty"):
            continue
        (span,) = case["spans"]
        attrs = span["attributes"]
        count, source = receive_count(event["redelivered"], event["headers"])
        assert attrs.get("trailbeam.receive_count") == count, case["name"]
        assert attrs.get("trailbeam.receive_count.source") == source, case["name"]
        got = {k: v for k, v in attrs.items() if k.startswith("trailbeam.dead_letter.")}
        assert got == dead_letter(event["headers"]), case["name"]
        assert attrs.get("messaging.message.id") == event["properties"].get("message_id"), case["name"]
        seen += 1
    assert seen >= 5


def test_settles_cover_what_the_call_names():
    for case in VECTORS["emit"]:
        event = case["event"]
        if event.get("call") not in ("basic.ack", "basic.nack", "basic.reject"):
            continue
        tag, multiple = event["delivery_tag"], event.get("multiple", False)
        covered = [d for d in event["recorded"]
                   if d["delivery_tag"] == tag or (multiple and (tag == 0 or d["delivery_tag"] <= tag))]
        assert [s["attributes"]["rabbitmq.message.delivery_tag"] for s in case["spans"]] == \
            [d["delivery_tag"] for d in covered], case["name"]


def test_ingest_vectors():
    for case in VECTORS["ingest"]:
        reason = check(case["attributes"])
        if case["accept"]:
            assert reason == "", (case["name"], reason)
        else:
            assert reason.startswith(case["reason_starts_with"]), (case["name"], reason)


def _load_fixture_maker():
    spec = importlib.util.spec_from_file_location("make_amqp_spans", ROOT / "contract" / "fixtures" / "make-amqp-spans.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fixture_is_what_the_script_writes(tmp_path):
    maker = _load_fixture_maker()
    maker.OUT = tmp_path / "out.json"
    maker.main()
    assert maker.OUT.read_text("utf-8") == FIXTURE.read_text("utf-8")


def test_fixture_spans_are_accepted_and_consistent():
    doc = json.loads(FIXTURE.read_text("utf-8"))
    assert set(doc["scenarios"]) == {s["scenario"] for s in doc["spans"]}
    open_deliveries = {}
    for span in doc["spans"]:
        attrs = span["attributes"]
        assert check(attrs) == "", (span["scenario"], check(attrs))
        assert span["name"] == "trailbeam " + attrs["trailbeam.kind"].split(".", 1)[1]
        if attrs.get("trailbeam.start_observed") is False:
            assert span["start_time_unix_nano"] == span["end_time_unix_nano"]
            assert "trailbeam.order.issued" not in attrs
        else:
            assert attrs["trailbeam.order.issued"] < attrs["trailbeam.order.returned"]
        key = (attrs.get("trailbeam.amqp.channel.id"), attrs.get("rabbitmq.message.delivery_tag"))
        if attrs["trailbeam.kind"] == "amqp.deliver":
            assert key not in open_deliveries, span["scenario"]
            open_deliveries[key] = attrs
        elif attrs["trailbeam.kind"] in ("amqp.ack", "amqp.nack", "amqp.reject", "amqp.channel_closed"):
            delivered = open_deliveries.pop(key)  # every settle joins a delivery on its channel (A6)
            assert attrs.get("messaging.message.id") == delivered.get("messaging.message.id")
            assert attrs["messaging.destination.name"] == delivered["messaging.destination.name"]
    # Left open: the delivery the broker took back when it cancelled the consumer, and the stuck one.
    assert sorted(a["messaging.message.id"] for a in open_deliveries.values()) == ["m-stuck", "m-timeout"]
