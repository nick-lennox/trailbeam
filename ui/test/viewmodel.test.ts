/*
  QV-578 (One UI row 0d): the shared view-model functions (@trailbeam/ui/model) on fixtures/viewmodel-parity.json.
  The same spans went through each backend's API; what eventsOfTimeline, overviewData and toCause make of either
  answer must be the same, and must be `expected`. The hosted app runs the same check on its live engine
  (test/viewmodel-parity.test.ts there). Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { viewModels, type Answers, type ViewModels } from "./viewmodel.ts";
import {
  bodyPathsOf, custom, eventsOfTimeline, handlerFailed, metaSummary, parseQueueUrl, parseRange, preset, queryLanguage, rangeFromParams, toCause, withRange, HOUR,
} from "../src/model/index.ts";

const f = JSON.parse(readFileSync(new URL("./fixtures/viewmodel-parity.json", import.meta.url), "utf8")) as {
  nowMs: number; fromMs: number; answers: { python: Answers; hosted: Answers }; expected: ViewModels;
};

for (const backend of ["python", "hosted"] as const) {
  test(`${backend}: the recorded answers give the expected events, Overview and cause rows`, () => {
    const got = viewModels(f.answers[backend], f.nowMs, f.fromMs);
    assert.deepEqual(got.events, f.expected.events);
    assert.deepEqual(got.overview, f.expected.overview);
    assert.deepEqual(got.causes, f.expected.causes);
  });
}

test("the fixture covers inferred events, both cause outcomes and every flow", () => {
  const kinds = new Set(f.expected.events.map((e) => e.event));
  for (const k of ["send", "receive", "delete", "processing_error", "visibility_expired", "dead_letter"]) assert.ok(kinds.has(k as never), k);
  assert.deepEqual(new Set(f.expected.causes.map((c) => c.outcome)), new Set(["dead_lettered", "stuck"]));
  assert.equal(f.expected.overview.queues.length, f.answers.python.flows.length);
  assert.ok(Object.keys(f.expected.overview.avgBytes).length > 0, "body sizes from the 3.2 spans");
});

test("eventsOfTimeline: queue place from a URL, a kept body on the span that carried it, a CloudTrail-only send", () => {
  const t = { message_id: "m1", current_queue: "q", events: [
    { id: "e1", type: "received", queue: "q", delivery: 1, receive_count: 1, end_unix_nano: "2000000000", consumer: { service_name: "w" }, span: { trace_id: "t", span_id: "s" } },
  ] };
  const [send, recv] = eventsOfTimeline(t, { urls: { q: "https://sqs.eu-west-1.amazonaws.com/123456789012/q" }, body: { value: { a: 1 }, traceId: "t", spanId: "s" },
    trailSend: { queue: "q", timeMs: 1000, call: "SendMessage", producer: "sns.amazonaws.com", byService: true } });
  assert.equal(send.source, "cloudtrail");
  assert.equal(send.detail, "Recorded by CloudTrail (whole seconds). Sent by the AWS service sns.amazonaws.com");
  assert.equal(recv.producer, "sns.amazonaws.com");
  assert.deepEqual(recv.body, { a: 1 });
  assert.equal(recv.attrs?.["queue.region"], "eu-west-1");
  assert.equal(recv.attrs?.["queue.account"], "123456789012");
  assert.deepEqual(parseQueueUrl("http://localhost:9324/000000000000/orders"), { name: "orders", account: "000000000000", region: null });
  assert.equal(handlerFailed("KeyError", "orderId", "failed_exception"), "The handler threw: KeyError: orderId");
  assert.equal(metaSummary([{ kind: "receive", tenant: "b", tenantSource: null, groupId: null, sequenceNumber: null, bodySize: 3 },
    { kind: "send", tenant: "a", tenantSource: "attribute", groupId: "g", sequenceNumber: null, bodySize: null }]).tenant, "a");
});

test("toCause takes the host's cause link", () => {
  const [g] = f.answers.python.causes;
  assert.equal(toCause(g, g.queue, f.nowMs - HOUR, f.nowMs, (id) => `/causes/${id}`).href, `/causes/${g.id}`);
});

test("time ranges: now is the host's, and a range in a link reads back", () => {
  const now = Date.UTC(2026, 10, 27, 12, 0);
  assert.deepEqual(preset("4h", now), { from: now - 4 * HOUR, to: now, label: "Past 4 hours", preset: "4h" });
  assert.equal(parseRange("45m", now)?.from, now - 45 * 60_000);
  const fixed = custom(now - HOUR, now);
  const href = withRange("/events?q=x", fixed);
  const back = rangeFromParams((k) => new URL(href, "http://h").searchParams.get(k), now);
  assert.deepEqual(back, fixed);
});

test("the query language reads what events carry, or what the host looks up", () => {
  const [e] = f.expected.events.filter((x) => x.event === "receive");
  const q = queryLanguage();
  assert.ok(q.matches(e, q.parse(`queue:${e.queue} message.ApproximateReceiveCount:>=1`)));
  assert.equal(q.parse("queue.VisibilityTimeout:30s", true)[0].error, "queue.VisibilityTimeout isn't recorded on your own events, so it would match nothing.");
  const sample = queryLanguage({ attr: (_, n) => (n === "queue.VisibilityTimeout" ? 30_000 : undefined), body: () => ({ customer: "cus_1" }) });
  assert.ok(sample.matches(e, sample.parse("queue.VisibilityTimeout:30s body.customer:cus_1")));
  assert.ok(!q.matches(e, q.parse("body.customer:cus_1")), "no stored body, no match");
  assert.deepEqual(bodyPathsOf([{ a: { b: 1 }, c: [1] }]), ["a.b", "c"]);
});
