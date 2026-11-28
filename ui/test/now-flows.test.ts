/*
  QV-579 stage 2, second half: the hosted Overview's page (OverviewFlows) and "In ‹queue› now" (QueueNowLive) in
  @trailbeam/ui. Their links, the sample's own parts and the host's copy are props; the data is either backend's
  InQueueView and OverviewData. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { Ev } from "../src/model/events.ts";
import { heldAt, notDeletedText, notSeenOnDlqText, type HeldMessage, type InQueueView } from "../src/model/in-queue.ts";
import type { QueueLive } from "../src/model/queue-live.ts";
import { QueueNowLive, type QueueNowHrefs } from "../src/now/QueueNowLive.tsx";
import { OverviewFlows, type OverviewCopy, type OverviewFlowsData, type OverviewFlowsHrefs, type OverviewSample, type PartMessagesAt } from "../src/now/OverviewFlows.tsx";
import * as design from "../src/design/index.ts";
import * as model from "../src/model/index.ts";

const NOW = Date.UTC(2026, 10, 27, 14, 0, 0);
const html = (el: Parameters<typeof renderToStaticMarkup>[0]) => renderToStaticMarkup(el);
const ev = (event: Ev["event"], ms: number, o: Partial<Ev> = {}): Ev => ({
  key: `m-${event}-${ms}`, ms, event, status: "ok", queue: "orders", messageId: "m1-0000-aaaa", service: "orders-worker",
  producer: "checkout-api", consumer: "orders-worker", detail: "", source: "package", hasPage: true, live: true, ...o,
});
const held = (o: Partial<HeldMessage>): HeldMessage => ({
  id: "m1-0000-aaaa", producer: "checkout-api", consumer: "orders-worker", receiveCount: 1, state: "in_flight", sentMs: NOW - 20_000,
  lastReceiveMs: NOW - 10_000, untilMs: NOW + 20_000, leftS: 20, used: 1 / 3, timeoutS: 30, events: [ev("send", NOW - 20_000), ev("receive", NOW - 10_000, { receiveCount: 1 })], ...o,
});
const view = (o: Partial<InQueueView> = {}): InQueueView => ({
  nowMs: NOW, config: { visibilityTimeoutS: 30, retentionS: 345_600, maxReceiveCount: 3, dlq: "orders-dlq", arn: null, visibilityKnown: true },
  held: [
    held({}),
    held({ id: "m2-0000-bbbb", state: "visible", receiveCount: 2, untilMs: null, leftS: 0, used: 0, lastError: "Handler failed: TimeoutError" }),
    held({ id: "m3-0000-cccc", producer: "", state: "visible", receiveCount: 0, lastReceiveMs: null, untilMs: null, leftS: 0, used: 0, timeoutS: null, consumer: "" }),
  ],
  capped: false, instrumented: { visible: 2, inFlight: 1 }, totals: null, uninstrumented: null,
  notSeenOnDlq: [{ id: "m9", timedOutMs: NOW - 5_000, receiveCount: 3, producer: "checkout-api", consumer: "orders-worker", sentMs: NOW - 90_000, events: [] }],
  seenOnDlq: 1, deleted: [{ id: "m8", atMs: NOW - 4_000 }], producers: [{ name: "checkout-api", n: 2 }, { name: "No send observed", n: 1 }], ...o,
});
const NOW_HREFS: QueueNowHrefs = { message: (id) => `/messages/${id}`, events: (q) => `/events?q=${encodeURIComponent(q)}` };
const noLive = { status: "none", detail: "No AWS account is read for this queue.", visible: null, inFlight: null } as unknown as QueueLive;

test("the views are exported from @trailbeam/ui/design, and the InQueueView shape from @trailbeam/ui/model", () => {
  assert.equal(typeof design.OverviewFlows, "function");
  assert.equal(typeof design.QueueNowLive, "function");
  assert.equal(typeof model.heldAt, "function");
  assert.equal(model.SQS_DEFAULT_VISIBILITY_S, 30);
});

test("heldAt counts a visibility timeout down, and says what is inferred once it runs out", () => {
  const m = held({});
  assert.deepEqual([heldAt(m, NOW + 5_000, 3).leftS, heldAt(m, NOW + 5_000, 3).state], [15, "in_flight"]);
  const back = heldAt(m, NOW + 21_000, 3, true);
  assert.equal(back.state, "visible");
  assert.match(back.lastError!, /^Inferred: receive 1 was not deleted.*no delete has reached the page.*SQS's default 30 s/);
  const last = heldAt(held({ receiveCount: 3 }), NOW + 21_000, 3);
  assert.equal(last.left, true);
  assert.equal(last.lastError, notSeenOnDlqText(3, 3));
  assert.equal(heldAt(m, NOW + 21_000, null).left, undefined);
  assert.equal(notDeletedText(2, false), "Inferred: receive 2 was not deleted before its visibility timeout ran out (no delete of it was stored)");
});

test("In ‹queue› now on spans alone: the list, its filters, the inferred moves and the host's links", () => {
  const out = html(createElement(QueueNowLive, { queue: "orders", view: view(), live: noLive, heldTotal: 5, consumer: "orders-worker", fifo: false, hrefs: NOW_HREFS }));
  for (const w of ["In orders now", "Visibility timeout", "maxReceiveCount", "orders-dlq", "Receive count", "In flight", "No send observed",
    "Handler failed: TimeoutError", "20 s left", "No AWS account is read for this queue.", "The counts are the messages your instrumented services sent or received.",
    "has reached maxReceiveCount and is not seen on the DLQ yet (inferred)", "Closest to the dead-letter queue first"]) assert.ok(out.includes(w.replace(/'/g, "&#x27;")), w);
  // closest to the dead-letter queue first: receive count 2, then 1, then 0
  assert.ok(out.indexOf("m2-0000-") < out.indexOf("m1-0000-") && out.indexOf("m1-0000-") < out.indexOf("m3-0000-"));
  // three listed, and the two the backend worked out but did not send
  assert.match(out, />5<\/b> messages/);
  assert.ok(out.includes(`href="/events?q=queue%3Aorders"`));
  assert.doesNotMatch(out, / title="/);
});

test("In ‹queue› now with SQS's totals says what services without Trailbeam account for", () => {
  const v = view({ totals: { visible: 10, inFlight: 1, delayed: 2 }, uninstrumented: { visible: 8, inFlight: 0, over: false } });
  const out = html(createElement(QueueNowLive, { queue: "orders", view: v, live: null, heldTotal: 3, consumer: "", fifo: false, hrefs: NOW_HREFS }));
  assert.match(out, /SQS holds <b[^>]*>10<\/b> visible and <b[^>]*>1<\/b> in flight, and <b[^>]*>2<\/b> delayed/);
  assert.match(out, /The other <b[^>]*>8<\/b> visible/);
  assert.ok(out.includes("Visibility timeout <span"));
});

test("In ‹queue› now with nothing read, and with the queue's timeout unread", () => {
  assert.ok(html(createElement(QueueNowLive, { queue: "orders", view: null, live: null, heldTotal: 0, consumer: "", fifo: false, hrefs: NOW_HREFS })).includes("No messages to show in orders"));
  const v = view({ held: [], producers: [], notSeenOnDlq: [], config: { ...view().config, visibilityTimeoutS: null, maxReceiveCount: null, visibilityKnown: false } });
  const out = html(createElement(QueueNowLive, { queue: "orders", view: v, live: null, heldTotal: 0, consumer: "", fifo: false, hrefs: NOW_HREFS }));
  assert.ok(out.includes("Visibility timeout not read, so SQS&#x27;s default"));
  assert.ok(out.includes("No messages in orders right now"));
  assert.ok(out.includes("Highest receive count first"));
});

/* ── the page ── */
const HREFS: OverviewFlowsHrefs = {
  range: (id) => `/?range=${id}`, queue: ({ name, range }) => `/queues/${name}?range=${range}`, issues: (q) => `/issues?queue=${q}`,
  cause: (c) => `/issues/${encodeURIComponent(c.title)}`, events: (q) => `/events?q=${encodeURIComponent(q)}`,
  deadLettered: ({ dlq }) => `/messages?queue=${dlq}&state=dlq`, message: (id) => `/messages/${id}`,
};
const COPY: OverviewCopy = { noHealth: (n) => `None of the ${n} queues has health data`, spansOnly: " (from spans)", noFigures: () => "No AWS figures for this queue." };
const queue = (name: string, sentPerMin: number) => ({ name, type: "Standard" as const, consumer: "orders-worker", sentPerMin, p95WaitMs: 0, stuck: 0, dlq: 0, health: "good" as const, spark: [] });
const data = (o: Partial<OverviewFlowsData> = {}): OverviewFlowsData => ({
  workspace: "local", range: "1h", queues: [queue("orders", 12.5), queue("emails", 2)],
  sends: [{ from: "checkout-api", to: "orders", perMin: 12.5 }, { from: "checkout-api", to: "emails", perMin: 2 }],
  receives: [{ from: "orders", to: "orders-worker", perMin: 12 }], consumers: [{ name: "orders-worker", runtime: "", queue: "orders" }],
  consumerStats: {}, consumerStatsCapped: false, producerPlatform: {}, avgBytes: { orders: 800 }, dlq: {}, places: {}, functionEnds: {},
  info: {
    orders: { live: noLive, health: "warning", why: "Received again", healthBasis: "spans", triggers: null, inQueue: view(), heldTotal: 3, moved: 0, dlqReceives: 0, dlqChange: null },
  },
  causes: [{ queue: "orders", outcome: "stuck", title: "Not deleted after receive", count: 4 } as OverviewFlowsData["causes"][number]],
  causesCapped: false, now: NOW, ...o,
});

test("the page on recorded data: worst flow first, span-only health marked, In ‹queue› now under it, links from the host", () => {
  const out = html(createElement(OverviewFlows, { data: data(), copy: COPY, hrefs: HREFS }));
  assert.ok(out.indexOf(">orders<") < out.indexOf(">emails<"), "the flow with unfinished messages is listed first");
  for (const w of ["1 of 2 queues needs attention; 1 has no health data (from spans)", "Messages through orders", "Why messages didn&#x27;t finish on orders",
    "Not deleted after receive", "No AWS figures for this queue.", "In orders now", "12.5", "Issues on orders"]) assert.ok(out.includes(w), w);
  for (const h of [`href="/issues?queue=orders"`, `href="/queues/orders?range=1h"`, `href="/issues/Not%20deleted%20after%20receive"`]) assert.ok(out.includes(h), h);
  assert.doesNotMatch(out, / title="/);
});

test("the page with no flows: one message and the host's way forward", () => {
  const out = html(createElement(OverviewFlows, { data: data({ queues: [], sends: [], receives: [], consumers: [], info: {}, causes: [] }), copy: COPY, hrefs: HREFS, slots: { emptyActions: createElement("a", { href: "/setup" }, "Connect a service") } }));
  assert.ok(out.includes("Waiting to observe your first message"));
  assert.ok(out.includes("Nothing is reporting to local."));
  assert.ok(out.includes("Connect a service"));
});

test("the page on the sample: the host's simulated queue and search come through its slots, and the sample's scale applies", () => {
  const sample: OverviewSample = {
    queues: [queue("orders", 10)], causes: [], sends: [{ from: "checkout-api", to: "orders", perMin: 10 }], receives: [{ from: "orders", to: "orders-worker", perMin: 10 }],
    consumers: [{ name: "orders-worker", runtime: "Lambda", queue: "orders" }], producerPlatform: { "checkout-api": "EKS" },
    settings: { orders: { visibilityTimeoutS: 30, maxReceiveCount: 3, dlq: "orders-dlq", retentionDays: 4 } }, stats: { orders: { visible: 4, notVisible: 2, oldestS: 75 } },
    consumerStats: { "orders-worker": { p95Ms: 120, failPct: 0.4, timeouts: 0 } }, avgBytes: { orders: 1000 }, rangeScale: { "1h": { k: 3, c: 1 } },
  };
  const asked: PartMessagesAt[] = [];
  const out = html(createElement(OverviewFlows, {
    sample, copy: COPY, hrefs: HREFS,
    slots: { sampleNow: (q) => createElement("p", null, `simulated ${q}`), search: createElement("input", { "aria-label": "Search messages" }), partMessages: (o) => { asked.push(o); return null; } },
  }));
  assert.ok(out.includes("simulated orders"));
  assert.ok(out.includes(`aria-label="Search messages"`));
  assert.ok(out.includes("The queue is healthy"));
  assert.ok(out.includes(">30.0<"), "the sample's rate times its range scale, to one decimal");
  assert.ok(out.includes("orders-dlq"));
  assert.ok(out.includes("1m 15s"));
  assert.equal(asked.length, 0, "the queue is the part selected, so no part's messages are asked for");
});
