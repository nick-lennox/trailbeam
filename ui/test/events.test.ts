/*
  QV-399: the Event explorer on GET /api/events (src/events): its query language, what a step's row says, the chart's
  bars, the asks for a window and a picked span, the CSV export, and the view drawn from an answer. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  EventExplorer, barsOf, collectSteps, eventsQuery, exportRow, fmtStepTime, kindsOf, niceMax, onlyKind, parseQuery, perLabel, pickedWindow,
  setField, stepFailed, stepResult, toCsv, type EventPage, type EventStep,
} from "../src/index.ts";

const step = (over: Partial<EventStep> = {}): EventStep => ({
  time: "2026-11-16T18:05:37.716123Z", time_unix_nano: "1794852337716123000", start_unix_nano: "1794852337715000000",
  kind: "delete", message_id: "m-1", queue: "orders", service_name: "orders-worker", service_version: "1.4.2",
  result: "ok", error_code: null, outcome: null, error_type: null, receive_count: null, visibility_timeout: null,
  trace_id: "bf3be8567ccb1400f14bfc650a51150f", span_id: "57dccf6cc3c7c18e", ...over,
});
const zero = { send: 0, receive: 0, delete: 0, change_visibility: 0, outcome: 0 };

test("a query becomes the API's filters", () => {
  assert.deepEqual(parseQuery("").params, {});
  assert.deepEqual(parseQuery("queue:orders service:orders-worker").params, { queue: "orders", service: "orders-worker" });
  assert.deepEqual(parseQuery("m-123").params, { message_id: "m-123" });
  assert.deepEqual(parseQuery('message_id:"m 1"').params, { message_id: "m 1" });
  // several kinds are alternatives, in the API's order; -kind leaves one out; every kind is no filter at all
  assert.equal(parseQuery("kind:delete kind:send").params.kind, "send,delete");
  assert.equal(parseQuery("-kind:receive").params.kind, "send,delete,change_visibility,outcome");
  assert.equal(parseQuery("kind:send kind:receive kind:delete kind:change_visibility kind:outcome").params.kind, undefined);
  assert.equal(parseQuery("event:lambda").params.kind, "outcome");
  assert.equal(parseQuery("QUEUE:orders").params.queue, "orders");
});

test("a query the API can't answer says why, and asks nothing", () => {
  assert.match(parseQuery("tenant:acme").error!, /Spans don't record tenant/);
  assert.match(parseQuery("processing_time:>1s").error!, /Spans don't record processing_time/);
  assert.match(parseQuery("status:error").error!, /can't filter on status yet/);
  assert.match(parseQuery("body.customer:acme").error!, /doesn't search message bodies/);
  assert.match(parseQuery("colour:red").error!, /no field called colour/);
  assert.match(parseQuery("kind:purge").error!, /kind is one of send, receive/);
  assert.match(parseQuery("queue:orders queue:payments").error!, /one queue at a time, not 2/);
  assert.match(parseQuery("-queue:orders").error!, /can't leave out a queue/);
  assert.match(parseQuery("queue:ord*").error!, /doesn't match with \*/);
  assert.match(parseQuery("kind:send -kind:send").error!, /leaves out every kind/);
  // the same value twice is one filter
  assert.equal(parseQuery("queue:orders queue:orders").error, undefined);
});

test("editing a query: one field's value, and the kind legend", () => {
  assert.equal(setField("queue:a kind:send", "queue", "b"), "kind:send queue:b");
  assert.equal(setField("queue:a kind:send", "queue", null), "kind:send");
  assert.equal(setField("", "service", "my svc"), 'service:"my svc"');
  // an unknown field's term is kept as typed, so its error stays in view
  assert.equal(setField("tenant:x", "queue", "orders"), "tenant:x queue:orders");
  assert.equal(onlyKind("queue:a", "delete"), "queue:a kind:delete");
  assert.equal(onlyKind("queue:a kind:delete", "delete"), "queue:a");
  assert.equal(onlyKind("kind:send kind:delete", "delete"), "kind:delete");
  assert.deepEqual(kindsOf(parseQuery("kind:send")), ["send"]);
  assert.equal(kindsOf(parseQuery("")).length, 5);
});

test("what a step's row says", () => {
  assert.equal(fmtStepTime(step()), "2026-11-16 18:05:37.716");
  assert.equal(stepResult(step()), "Accepted");
  assert.equal(stepResult(step({ result: "failed", error_code: "ReceiptHandleIsInvalid" })), "Failed: ReceiptHandleIsInvalid");
  assert.equal(stepResult(step({ kind: "change_visibility", visibility_timeout: 120 })), "Timeout set to 120 s");
  assert.equal(stepResult(step({ kind: "outcome", result: null, outcome: "failed_exception", error_type: "TimeoutError" })), "Raised an exception: TimeoutError");
  assert.equal(stepResult(step({ kind: "receive", result: null })), "");
  assert.equal(stepFailed(step({ kind: "outcome", result: null, outcome: "success" })), false);
  assert.equal(stepFailed(step({ kind: "outcome", result: null, outcome: "failed_reported" })), true);
  assert.equal(stepFailed(step({ result: "failed" })), true);
});

test("asks: the window, the filters, the page and the cursor", () => {
  const w = { from_ms: 1000.4, to_ms: 2000.9 };
  assert.deepEqual(eventsQuery(w, parseQuery("queue:orders kind:send")), { from_ms: "1000", to_ms: "2000", queue: "orders", kind: "send", limit: "100" });
  assert.equal(eventsQuery(w, parseQuery(""), 1000, "abc").cursor, "abc");
});

test("the chart's bars, their scale, and a picked span inside the range", () => {
  const h = { bucket_ms: 60_000, total: 7, totals: { ...zero, send: 4, delete: 3 },
    buckets: [{ start_ms: 0, ...zero, send: 1 }, { start_ms: 60_000, ...zero, send: 3, delete: 3 }] };
  const bars = barsOf(h);
  assert.deepEqual(bars.map((b) => [b.start, b.end, b.total]), [[0, 60_000, 1], [60_000, 120_000, 6]]);
  assert.equal(bars[1].counts.delete, 3);
  assert.deepEqual([niceMax(0), niceMax(6), niceMax(130), niceMax(1001)], [4, 6, 150, 1500]);
  assert.deepEqual([perLabel(60_000), perLabel(3_600_000), perLabel(172_800_000)], ["minute", "hour", "2 days"]);
  // the range starts and ends inside its first and last bars; a span picked across them stays inside it
  assert.deepEqual(pickedWindow({ from_ms: 30_000, to_ms: 100_000 }, bars[1], bars[0]), { from_ms: 30_000, to_ms: 100_000 });
  assert.deepEqual(pickedWindow({ from_ms: 0, to_ms: 200_000 }, bars[1], bars[1]), { from_ms: 60_000, to_ms: 119_999 });
});

test("CSV: quoted where needed, and no cell read as a formula", () => {
  assert.equal(toCsv(["a", "b"], [["x,y", 'say "hi"'], ["=SUM(A1)", "ok"]]), 'a,b\r\n"x,y","say ""hi"""\r\n\'=SUM(A1),ok\r\n');
  assert.deepEqual(exportRow(step()), ["2026-11-16 18:05:37.716", "Delete", "orders", "orders-worker", "1.4.2", "m-1", "", "Accepted",
    "bf3be8567ccb1400f14bfc650a51150f", "57dccf6cc3c7c18e"]);
});

test("an export follows the cursor up to its limit, and says when there were more", async () => {
  const asks: Record<string, string>[] = [];
  const load = async (q: Record<string, string>) => {
    asks.push(q);
    const page = Number(q.cursor ?? 0);
    return { events: Array.from({ length: Number(q.limit) }, (_, i) => step({ span_id: `s${page}-${i}` })), next_cursor: String(page + 1) };
  };
  const got = await collectSteps(load, { from_ms: 0, to_ms: 1 }, parseQuery("queue:orders"), 2500);
  assert.equal(got.steps.length, 2500);
  assert.equal(got.more, true);
  assert.deepEqual(asks.map((q) => [q.limit, q.cursor, q.queue]), [["1000", undefined, "orders"], ["1000", "1", "orders"], ["500", "2", "orders"]]);
  const short = await collectSteps(async () => ({ events: [step()], next_cursor: null }), { from_ms: 0, to_ms: 1 }, parseQuery(""));
  assert.deepEqual([short.steps.length, short.more], [1, false]);
});

test("the view draws its query bar, and the query's error instead of asking", () => {
  const page: EventPage = { from_ms: 0, to_ms: 1, events: [], next_cursor: null, histogram: null };
  let asked = 0;
  const props = { onQuery: () => {}, window: { from_ms: 0, to_ms: 1 }, rangeKey: "24h", rangeText: "in the last 24 hours",
    load: async () => { asked++; return page; }, messageHref: (id: string) => `/message?id=${id}` };
  const ok = renderToStaticMarkup(createElement(EventExplorer, { ...props, query: "queue:orders" }));
  assert.match(ok, /data-event-explorer/);
  assert.match(ok, /value="queue:orders"/);
  assert.match(ok, /Loading steps/);
  const bad = renderToStaticMarkup(createElement(EventExplorer, { ...props, query: "tenant:acme" }));
  assert.match(bad, /Spans don&#x27;t record tenant yet/);
  assert.match(bad, /Fix the query to see steps/);
  assert.equal(asked, 0); // effects don't run when rendering to a string
});

test("the shell's /events page: no range in the URL is the last 24 hours, and the window always has both ends", async () => {
  const { eventsRange, eventsWindow } = await import("../shell/events.tsx");
  const NOW = Date.UTC(2026, 10, 24, 12, 0, 0);
  assert.deepEqual(eventsRange({}), { kind: "preset", preset: "24h" });
  assert.deepEqual(eventsRange({ range: "7d" }), { kind: "preset", preset: "7d" });
  assert.match((eventsRange({ range: "2d" }) as { error: string }).error, /range must be/);
  assert.deepEqual(eventsWindow({ kind: "preset", preset: "1h" }, NOW), { from_ms: NOW - 3_600_000, to_ms: NOW });
  assert.deepEqual(eventsWindow({ kind: "custom", from_ms: 5, to_ms: 9 }, NOW), { from_ms: 5, to_ms: 9 });
  assert.deepEqual(eventsWindow({ kind: "custom", to_ms: NOW }, NOW + 1), { from_ms: NOW - 86_400_000, to_ms: NOW });
  assert.deepEqual(eventsWindow({ kind: "custom", from_ms: 5 }, NOW), { from_ms: 5, to_ms: NOW });
});
