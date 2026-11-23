"""Turn one recorded run of `kit.py run` into a test fixture that is safe to commit.

    python conformance/tests/make_fixture.py WORKDIR RUN NAME

Writes conformance/tests/fixtures/NAME.{truth,spans}.jsonl. Raw receipt handles never leave the bench, so
each one is replaced by a synthetic handle, and every HMAC and key ID is recomputed with FIXTURE_KEY. The
run's name becomes "kit-fixture-NAME" (queue names, URLs, ARNs and service.name).
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import checker  # noqa: E402

FIXTURE_KEY = "kit-fixture-key-0123456789abcdef"


def main(workdir: str, run: str, name: str) -> None:
    w = Path(workdir)
    settings = json.loads((w / f"{run}.run.json").read_text())
    key = settings["key"]
    truth_lines = (w / f"{run}.truth.jsonl").read_text().splitlines()
    _, calls = checker.read_truth(truth_lines)

    handles = []
    for c in calls:
        for m in c.get("messages", []):
            handles.append(m["receipt_handle"])
        for e in c.get("entries", []):
            if e.get("receipt_handle"):
                handles.append(e["receipt_handle"])
        if c.get("receipt_handle"):
            handles.append(c["receipt_handle"])
    from_sqs = {m["receipt_handle"] for c in calls for m in c.get("messages", [])}
    new_handle = {}
    for h in handles:
        if h not in new_handle:
            # Handles SQS never returned are the scenario's own invalid ones, not secrets: keep them.
            new_handle[h] = f"fixture-receipt-handle-{len(new_handle):03d}-" + "x" * 24 if h in from_sqs else h
    hmac_map = {checker.receipt_hmac(key, h): checker.receipt_hmac(FIXTURE_KEY, n) for h, n in new_handle.items()}
    hmac_map[checker.key_id(key)] = checker.key_id(FIXTURE_KEY)
    new_run = f"kit-fixture-{name}"

    def rename(text: str) -> str:
        return text.replace(run, new_run)

    out_truth = []
    for line in truth_lines:
        row = json.loads(line)
        for m in row.get("messages", []):
            m["receipt_handle"] = new_handle[m["receipt_handle"]]
        for e in row.get("entries", []):
            if e.get("receipt_handle"):
                e["receipt_handle"] = new_handle[e["receipt_handle"]]
        if row.get("receipt_handle"):
            row["receipt_handle"] = new_handle[row["receipt_handle"]]
        out_truth.append(rename(json.dumps(row)))

    out_spans = []
    for line in (w / "collector" / "spans.jsonl").read_text().splitlines():
        doc = json.loads(line)
        keep = []
        for rs in doc.get("resourceSpans", []):
            attrs = {a["key"]: a["value"].get("stringValue") for a in rs.get("resource", {}).get("attributes", [])}
            if attrs.get("service.name") != run:
                continue
            for ss in rs.get("scopeSpans", []):
                for s in ss.get("spans", []):
                    for a in s.get("attributes", []):
                        v = a["value"].get("stringValue")
                        if a["key"] in ("trailbeam.receipt_handle.hmac", "trailbeam.receipt_handle.key_id") and v in hmac_map:
                            a["value"]["stringValue"] = hmac_map[v]
            keep.append(rs)
        if keep:
            out_spans.append(rename(json.dumps({"resourceSpans": keep})))

    blob = "\n".join(out_truth + out_spans)
    leaked = [h for h, n in new_handle.items() if h != n and h in blob]
    if leaked or key in blob:
        raise SystemExit(f"a raw handle or the run's key is still in the fixture: {leaked[:2]} key={key in blob}")
    fx = HERE / "fixtures"
    fx.mkdir(exist_ok=True)
    (fx / f"{name}.truth.jsonl").write_text("\n".join(out_truth) + "\n")
    (fx / f"{name}.spans.jsonl").write_text("\n".join(out_spans) + "\n")
    (fx / f"{name}.run.json").write_text(json.dumps({**settings, "run": new_run, "key": FIXTURE_KEY}, indent=2) + "\n")
    print(f"wrote {fx}/{name}.*: {len(out_truth) - 1} calls, {len(new_handle)} handles")


if __name__ == "__main__":
    main(*sys.argv[1:4])
