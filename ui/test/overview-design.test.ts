/*
  QV-579 stage 4: the design review's list for the Overview (One UI spike, QV-477): rates to one decimal, a neutral
  mark beside "No data on their health", padded flow-map edge labels, a counted DLQ node labelled "DLQ", service
  names truncated in the middle, a wrapping causes heading, the hosted freshness mark, and PartMessages on real
  spans (partMessagesOf). Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { Ev } from "../src/model/events.ts";
import type { QueueLive } from "../src/model/queue-live.ts";
import { fmtRate, splitTail } from "../src/model/format.ts";
import { outcomeOf, partMessagesOf } from "../src/model/part-messages.ts";
import { dlqNodeCount, labelTop, OverviewFlows, type OverviewCopy, type OverviewFlowsData, type OverviewFlowsHrefs } from "../src/now/OverviewFlows.tsx";
import { PartMessages } from "../src/now/PartMessages.tsx";
import { MiddleTruncate } from "../src/design/MiddleTruncate.tsx";
import { Since, sinceText } from "../src/design/Since.tsx";
import * as design from "../src/design/index.ts";
import * as model from "../src/model/index.ts";

const NOW = Date.UTC(2026, 10, 27, 14, 0, 0);
const html = (el: Parameters<typeof renderToStaticMarkup>[0]) => renderToStaticMarkup(el);
const ev = (event: Ev["event"], ms: number, o: Partial<Ev> = {}): Ev => ({
  key: `${o.messageId ?? "m1"}:${event}:${ms}`, ms, event, status: "ok", queue: "orders", messageId: "m1", service: "orders-worker",
  producer: "checkout-api", consumer: "orders-worker", detail: "", source: "package", hasPage: true, live: true, ...o,
});

test("rates read to one decimal, so rates side by side agree", () => {
  assert.deepEqual([fmtRate(8), fmtRate(6.25), fmtRate(6.34), fmtRate(0), fmtRate(1204.46)], ["8.0", "6.3", "6.3", "0.0", "1,204.5"]);
});

test("a long name gives way in the middle, so names that share a prefix stay apart", () => {
  assert.deepEqual(splitTail("orders-worker"), ["orders-worker", ""]);
  assert.deepEqual(splitTail("trailbeam-demo-order-worker"), ["trailbeam-demo-or", "der-worker"]);
  const out = html(createElement(MiddleTruncate, { text: "trailbeam-demo-order-worker" }));
  assert.match(out, /<span class="sr-only">trailbeam-demo-order-worker<\/span>/);
  assert.match(out, /class="min-w-0 truncate" aria-hidden="true">trailbeam-demo-or<\/span><span class="shrink-0" aria-hidden="true">der-worker</);
});

test("a wire's label sits clear of the wire, on the side away from its bend", () => {
  // a wire that bends down from its start: the label goes above the start, its bottom 5 px over the wire
  assert.equal(labelTop({ y1: 100, y2: 140, at: "start", two: true }) + 30, 95);
  // a wire that bends up to its end: the label goes below the end
  assert.equal(labelTop({ y1: 100, y2: 140, at: "end", two: false }), 145);
});

test("the DLQ node is counted: depth from AWS, else the receives on it from spans, else on the sample the dead-lettered", () => {
  const base = { real: true, queue: "orders", dlq: "orders-dlq", depth: null, received: 4, moved: 1, range: "the past hour" };
  assert.deepEqual(dlqNodeCount(base), { n: 4, note: "Receives on orders-dlq in the past hour by your services that SQS marked as coming from orders. 1 message moved to it in the past hour." });
  assert.equal(dlqNodeCount({ ...base, depth: 9 }).n, 9);
  assert.match(dlqNodeCount({ ...base, depth: 9 }).note, /^Messages in orders-dlq now, from CloudWatch\. 1 message moved/);
  assert.deepEqual(dlqNodeCount({ ...base, real: false, moved: 3 }), { n: 3, note: "Messages from orders dead-lettered in the past hour" });
});

test("the freshness mark says how long ago the figures were read", () => {
  assert.deepEqual([sinceText(0), sinceText(12_400), sinceText(61_000), sinceText(7_300_000)], ["0s ago", "12s ago", "1 min ago", "2 h ago"]);
  assert.match(html(createElement(Since, { ms: Date.now() - 3_000 })), /^<time dateTime="[^"]+">3s ago<\/time>$/);
  assert.equal(typeof design.Since, "function");
  assert.equal(typeof design.MiddleTruncate, "function");
  assert.equal(typeof model.partMessagesOf, "function");
});

/* ── the page ── */
const HREFS: OverviewFlowsHrefs = {
  range: (id) => `/?range=${id}`, queue: ({ name }) => `/queues/${name}`, issues: (q) => `/issues?queue=${q}`, cause: () => "/issues",
  events: (q) => `/events?q=${encodeURIComponent(q)}`, deadLettered: () => "/dlq", message: (id) => `/messages/${id}`,
};
const COPY: OverviewCopy = { noHealth: (n) => `${n} queues, with no data on their health`, spansOnly: "", noFigures: () => "No AWS figures." };
const noLive = { status: "not_configured", detail: null, visible: null, inFlight: null, dlq: "orders-dlq", dlqDepth: null, maxReceiveCount: 3 } as unknown as QueueLive;
const queue = (name: string, sentPerMin: number) => ({ name, type: "Standard" as const, consumer: "", sentPerMin, p95WaitMs: 0, stuck: 0, dlq: 0, health: "good" as const, spark: [] });
const data = (o: Partial<OverviewFlowsData> = {}): OverviewFlowsData => ({
  workspace: "local", range: "1h", queues: [queue("orders", 8), queue("emails", 6.25)],
  sends: [{ from: "trailbeam-demo-checkout-api", to: "orders", perMin: 8 }], receives: [{ from: "orders", to: "orders-worker", perMin: 6.3 }],
  consumers: [{ name: "orders-worker", runtime: "", queue: "orders" }],
  consumerStats: {}, consumerStatsCapped: false, producerPlatform: {}, avgBytes: {}, dlq: { orders: { name: "orders-dlq", received: 2 } } as OverviewFlowsData["dlq"], places: {}, functionEnds: {},
  info: { orders: { live: noLive, health: null, why: null, healthBasis: null, triggers: null, inQueue: null, heldTotal: 0, moved: 0, dlqReceives: 2, dlqChange: null } } as unknown as OverviewFlowsData["info"],
  causes: [], causesCapped: false, now: NOW, ...o,
});

test("the page: one decimal everywhere, a neutral mark when no queue has health data, a counted DLQ node, the causes heading wraps", () => {
  const out = html(createElement(OverviewFlows, { data: data(), copy: COPY, hrefs: HREFS }));
  assert.ok(out.includes("2 queues, with no data on their health"));
  assert.match(out, /data-mark="neutral" class="[^"]*bg-ink-3/);
  assert.doesNotMatch(out, /bg-good/, "no green mark beside no health data");
  for (const r of [">8.0<", ">6.3<"]) assert.ok(out.includes(r), r);
  assert.doesNotMatch(out, />8<\/span>|>6\.25</);
  assert.match(out, />DLQ<\/span>/);
  assert.match(out, /data-dlq-count="">2</);
  assert.match(out, /id="causes-h" class="(?![^"]*truncate)/);
  // the producer's name gives way in the middle
  assert.match(out, /aria-hidden="true">eckout-api</);
  assert.doesNotMatch(out, / title="/);
});

test("the freshness mark shows on recorded data when the host passes it, and not otherwise", () => {
  assert.doesNotMatch(html(createElement(OverviewFlows, { data: data(), copy: COPY, hrefs: HREFS })), /data-fresh/);
  const out = html(createElement(OverviewFlows, { data: data(), copy: COPY, hrefs: HREFS, fresh: "12s ago", slots: { search: createElement("input", { "aria-label": "Search" }) } }));
  assert.match(out, /data-fresh="">.*Updated 12s ago/);
  assert.doesNotMatch(out, /aria-label="Search"/, "recorded data keeps its top bar without the sample's search");
});

/* ── PartMessages on real spans ── */
const EVENTS: Ev[] = [
  // newest first, as GET /api/timeline-events lists them
  ev("dead_letter", NOW - 1_000, { messageId: "m3", status: "error", detail: "Moved to orders-dlq", receiveCount: 3, service: "sqs" }),
  ev("processing_error", NOW - 2_000, { messageId: "m3", status: "error", detail: "TimeoutError", receiveCount: 3 }),
  ev("receive", NOW - 2_500, { messageId: "m3", receiveCount: 3 }),
  ev("delete", NOW - 3_000, { messageId: "m1" }),
  ev("receive", NOW - 4_000, { messageId: "m2", consumer: "emails-worker", receiveCount: 1 }),
  ev("visibility_expired", NOW - 5_000, { messageId: "m1", service: "sqs", receiveCount: 1 }),
  ev("receive", NOW - 5_000, { messageId: "m1", receiveCount: 2 }),
  ev("receive", NOW - 6_000, { messageId: "m1", receiveCount: 1 }),
  ev("send", NOW - 7_000, { messageId: "m1", sizeBytes: 900, tenant: "globex", body: { id: 1 } }),
  ev("send", NOW - 8_000, { messageId: "m3", producer: "billing" }),
  ev("send", NOW - 9_000, { messageId: "m9", queue: "emails" }),
];

test("partMessagesOf: one row per message the part handled, newest first, with its whole history oldest first", () => {
  const con = partMessagesOf(EVENTS, { queue: "orders", part: "consumer", name: "orders-worker" });
  assert.deepEqual(con.map((m) => [m.id, m.outcome, m.receiveCount]), [["m3", "dead_lettered", 3], ["m1", "deleted", 2]]);
  const m1 = con[1];
  assert.deepEqual(m1.events.map((e) => e.event), ["send", "receive", "receive", "visibility_expired", "delete"], "same-ms events keep the server's order");
  assert.deepEqual([m1.sentMs, m1.lastMs, m1.producer, m1.payload], [NOW - 7_000, NOW - 3_000, "checkout-api", { id: 1 }]);
  assert.equal(con[0].lastError, "TimeoutError", "the handler's error, not the move to the DLQ");
  assert.deepEqual(partMessagesOf(EVENTS, { queue: "orders", part: "producer", name: "billing" }).map((m) => m.id), ["m3"]);
  assert.deepEqual(partMessagesOf(EVENTS, { queue: "orders", part: "producer", name: "checkout-api" }).map((m) => m.id), ["m1"]);
  assert.deepEqual(partMessagesOf(EVENTS, { queue: "orders", part: "dlq" }).map((m) => m.id), ["m3"]);
  assert.deepEqual(partMessagesOf(EVENTS, { queue: "orders", part: "consumer", name: "emails-worker" }).map((m) => [m.id, m.outcome]), [["m2", "in_flight"]]);
  assert.deepEqual(partMessagesOf(EVENTS, { queue: "emails", part: "consumer", name: "orders-worker" }), []);
});

test("outcomeOf: what a message's events say happened to it so far", () => {
  assert.equal(outcomeOf([]), "visible");
  assert.equal(outcomeOf([ev("send", 1)]), "visible");
  assert.equal(outcomeOf([ev("send", 1), ev("receive", 2)]), "in_flight");
  assert.equal(outcomeOf([ev("send", 1), ev("receive", 2), ev("processing_error", 3, { status: "error" })]), "failed");
  assert.equal(outcomeOf([ev("send", 1), ev("receive", 2), ev("visibility_expired", 3)]), "visible");
});

test("PartMessages draws real rows: the DLQ's receive count against maxReceiveCount, sizes and tenants from the spans", () => {
  const dlq = html(createElement(PartMessages, { queue: "orders", part: "dlq", rows: partMessagesOf(EVENTS, { queue: "orders", part: "dlq" }), max: 3, eventsHref: (q) => `/events?q=${encodeURIComponent(q)}` }));
  assert.ok(dlq.includes("3 of 3"));
  assert.ok(dlq.includes("TimeoutError"));
  assert.ok(dlq.includes(`href="/events?q=queue%3Aorders%20event%3Adead_letter"`));
  const sent = html(createElement(PartMessages, { queue: "orders", part: "producer", producer: "checkout-api", rows: partMessagesOf(EVENTS, { queue: "orders", part: "producer", name: "checkout-api" }), max: 3, eventsHref: () => "/events" }));
  for (const w of ["900 B", "globex", "Deleted"]) assert.ok(sent.includes(w), w);
});
