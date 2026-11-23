/*
  The Body tab and body.* search on the shared expected outputs (../../contract/fixtures/body-views.json,
  written from the recorded contract 3.2 spans by make-body-views.py). The hosted app checks its bodyView
  against the same file. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { BodyPanel } from "../src/body/BodyPanel.tsx";
import { NO_BODY_TEXT } from "../src/body/rules.ts";
import { bodyMatches, parseBodyQuery, parsedBody } from "../src/body/search.ts";
import type { MessageBody } from "../src/body/types.ts";

type Case = { name: string; views: Record<string, MessageBody> };
const fixture = JSON.parse(readFileSync(new URL("../../contract/fixtures/body-views.json", import.meta.url), "utf8")) as {
  cases: Case[]; terms: { body: string; format: string; query: string; matches: boolean }[];
};
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/\s+/g, " ");
const render = (v: MessageBody) => renderToStaticMarkup(createElement(BodyPanel, { view: v, access: "ACCESS-NOTE", hints: { not_opted_in: "HINT-STORE" } }));

test("each term case gives what the spec says", () => {
  assert.ok(fixture.terms.length >= 30);
  for (const t of fixture.terms) assert.equal(bodyMatches(parsedBody(t.body, t.format), parseBodyQuery(t.query)), t.matches, `${t.query} on ${t.body}`);
});

test("a body search takes body terms, and plain text as body:<text>", () => {
  assert.throws(() => parseBodyQuery("queue:orders"), { message: "Can't read `queue:orders`. Use text, or `body.<path>:<value>`." });
  assert.throws(() => parseBodyQuery("ord-1003 -tenant:acme"), { message: "Can't read `-tenant:acme`. Use text, or `body.<path>:<value>`." });
  for (const q of ["", "   ", '""']) assert.throws(() => parseBodyQuery(q), { message: "the body search is empty" });
  assert.deepEqual(parseBodyQuery("ord-1003"), [{ field: "body", value: "ord-1003", neg: false }]);
  assert.deepEqual(parseBodyQuery('-acme "call me" body.order_id:ord-1003'), [
    { field: "body", value: "acme", neg: true }, { field: "body", value: "call me", neg: false }, { field: "body.order_id", value: "ord-1003", neg: false },
  ]);
  const order = { order_id: "ord-1003", customer: "acme" };
  assert.equal(bodyMatches(order, parseBodyQuery("ord-1003")), true);
  assert.equal(bodyMatches(order, parseBodyQuery("ORD-1003")), true);
  assert.equal(bodyMatches(order, parseBodyQuery("ord-1004")), false);
  assert.equal(bodyMatches(order, parseBodyQuery("ord-1003 -acme")), false);
});

test("every view the server gives for the recorded spans is drawn, with its reason or its body", () => {
  let bodies = 0, none = 0;
  for (const c of fixture.cases) for (const v of Object.values(c.views)) {
    const html = render(v);
    const t = text(html);
    assert.ok(!/undefined|NaN|null/.test(t), `${c.name} ${v.message_id}: ${t}`);
    assert.ok(t.includes("ACCESS-NOTE"), "says who can read bodies");
    if (v.kind === "none") {
      none++;
      assert.ok(t.includes(NO_BODY_TEXT[v.reason].replace(/'/g, "'")), `${v.message_id}: ${v.reason}`);
      assert.equal(t.includes("HINT-STORE"), v.reason === "not_opted_in");
      if (v.size !== null) assert.ok(t.includes(`${v.size} bytes`));
    } else {
      bodies++;
      const parsed = JSON.parse(v.body.body) as { customer: { id: string } };
      assert.ok(t.includes(parsed.customer.id), "the body is drawn");
      assert.ok(t.includes(v.body.from_kind === "send" ? "From the send by" : "From a receive by"));
    }
  }
  assert.ok(bodies >= 5 && none >= 10);
});

test("the notes under producer not instrumented", () => {
  const v = fixture.cases.find((c) => c.name === "consumer only")!.views["m-pay-1"];
  assert.ok(text(render(v)).includes("Capture is off for this queue in the consumer's configuration too."));
  const legacy: MessageBody = { message_id: "x", kind: "none", reason: "producer_not_instrumented", size: null, sends: 0, receives: 2, send_status: null, receive_status: null };
  assert.ok(text(render(legacy)).includes("The consumer's package is also from before contract revision 3.2."));
  const odd: MessageBody = { ...legacy, reason: "unknown", sends: 1, send_status: "captured" };
  assert.ok(text(render(odd)).includes("The send's capture status is captured."));
});

const POINTER = '["software.amazon.payloadoffloading.PayloadS3Pointer",{"s3BucketName":"payloads-123456789012","s3Key":"61fc25e3-8ae5-4569-9189-43c27e5ba06f"}]';
const keptPointer = {
  body: POINTER, format: "json", redacted_count: 0, envelope: null, capture_status: "offloaded_to_s3",
  from_kind: "send" as const, service: "checkout-api", queue: "orders", trace_id: "t1", span_id: "s1", time_unix_nano: "1",
};
const span = (kind: string, spanId: string, bodySize: number, offloadedSize?: number | null) => ({
  kind, captureStatus: "offloaded_to_s3", bodySize, offloadedSize, tenant: null, tenantSource: null, bodyDropped: false, traceId: "t1", spanId,
});

test("bodyView: capture status and offloaded size of the kept span, else the first span with a size", async () => {
  const { bodyView } = await import("../src/body/rules.ts");
  const v = bodyView("m1", keptPointer, [span("send", "s1", 145, 300000), span("receive", "s2", 145, 7)], { sends: 1, receives: 1 });
  assert.equal(v.kind, "body");
  if (v.kind !== "body") return;
  assert.equal(v.body.capture_status, "offloaded_to_s3");
  assert.equal(v.offloaded_size, 300000);
  const w = bodyView("m1", keptPointer, [span("receive", "s2", 145, 7), span("send", "s1", 145)], { sends: 1, receives: 1 });
  assert.equal(w.kind === "body" && w.offloaded_size, 7);
  const { capture_status: _, ...older } = keptPointer;
  const x = bodyView("m1", older, [span("send", "s1", 145)], { sends: 1, receives: 0 });
  assert.ok(x.kind === "body" && x.body.capture_status === null && x.offloaded_size === null);
});

test("an S3 pointer says the payload is in S3, with its size, and is not called redacted", () => {
  const v: MessageBody = { message_id: "m1", kind: "body", body: keptPointer, size: 145, offloaded_size: 300000, tenant: null, tenant_source: null };
  const t = text(render(v));
  assert.ok(t.includes("The payload is in S3."), t);
  assert.ok(t.includes("payload in S3: 300,000 bytes") && t.includes("pointer: 145 bytes"), t);
  assert.ok(!t.includes("values redacted") && !t.includes("before redaction"), t);
  assert.ok(t.includes("61fc25e3-8ae5-4569-9189-43c27e5ba06f"), "the pointer is drawn");
  const unsized = text(render({ ...v, offloaded_size: null }));
  assert.ok(unsized.includes("payload in S3") && !/null|undefined/.test(unsized));
  const captured = text(render({ ...v, body: { ...keptPointer, capture_status: "captured" }, offloaded_size: null }));
  assert.ok(!captured.includes("The payload is in S3.") && captured.includes("0 values redacted"));
});

test("not opted in: the body is not shown, and may still be stored (QV-247)", () => {
  const s = NO_BODY_TEXT.not_opted_in;
  assert.doesNotMatch(s, /dropped before it was stored/);
  assert.match(s, /isn't shown\. Either it was dropped when it arrived, or .* is still stored, only hidden\.$/);
});
