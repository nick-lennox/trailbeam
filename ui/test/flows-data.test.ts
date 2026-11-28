/*
  QV-579 stage 3: the open-source Overview on the shared OverviewFlows. Its data from GET /api/flows, GET /api/causes
  and GET /api/queues/{queue}/now (overview/flows-data.ts), with health from spans alone, and the host's figures
  (the range's totals from spans, the footnote) in the view's slots. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { flowOfGroup, flowsHref, flowsWindow, readFlowsUrl, selfHostedFlowsData, spanOnlyLive } from "../src/overview/flows-data.ts";
import type { Flow } from "../src/queue/selfhosted.ts";
import type { CauseGroup, CausePage } from "../src/causes/types.ts";
import type { InQueueView } from "../src/model/in-queue.ts";
import { OverviewFlows, type OverviewFlowsHrefs } from "../src/now/OverviewFlows.tsx";

const NOW = Date.UTC(2026, 10, 27, 14, 0, 0);
const flow = (queue: string, o: Partial<Flow> = {}): Flow => ({
  key: queue, queue, name: queue, url: null, account: null, region: null, lastSeen: NOW - 1000,
  sent: 120, received: 130, deleted: 110, visibilityChanges: 4,
  producers: [{ name: "checkout", n: 120, lambda: false }], consumers: [{ name: "worker", n: 130, lambda: false }],
  avgBytes: 512, dlq: null, ...o,
});
const group = (queue: string, o: Partial<CauseGroup> = {}): CauseGroup => ({
  id: "abc123def456", key: "k", kind: "exc", signature: "s", label: "ValueError in handle", basis: "spans", inferred: false, why: null,
  queue, queueKey: queue, consumer: "worker", outcome: "stuck", count: 3, counts: { stuck: 3, dead_lettered: 0, filtered: 0 }, dlqInferred: 0,
  firstMs: NOW - 30 * 60_000, lastMs: NOW - 60_000, times: [NOW - 30 * 60_000, NOW - 10 * 60_000, NOW - 60_000], tenants: [], sampleError: null,
  sampleMid: "m-1", examples: ["m-1"], earlier: [], retrying: 0, versions: [], ...o,
});
const page = (groups: CauseGroup[]): CausePage => ({
  older_than_s: 300, from_ms: NOW - 3_600_000, to_ms: NOW, capped: false, read_limit: 5000, total: groups.reduce((a, g) => a + g.count, 0),
  in_dead_letter_queue: 0, groups, dead_letter_queues: [], cause: null, listed: 0, messages: [],
});
const view = (o: Partial<InQueueView> = {}): InQueueView => ({
  nowMs: NOW, config: { visibilityTimeoutS: 30, retentionS: null, maxReceiveCount: 3, dlq: "orders-dlq", arn: null, visibilityKnown: true },
  held: [], capped: false, instrumented: { visible: 0, inFlight: 0 }, totals: null, uninstrumented: null, notSeenOnDlq: [], seenOnDlq: 0,
  deleted: [], producers: [], ...o,
});
const W = { fromMs: NOW - 3_600_000, toMs: NOW, minutes: 60 };
const data = (flows: Flow[], causes: CausePage | null, now: Record<string, InQueueView | null> = {}) =>
  selfHostedFlowsData({ range: "1h", window: W, flows, causes, now, causeHref: (id) => `/stuck?cause=${id}` });

test("the URL: range=15m|1h|24h and flow=, with the default range left out and an older link's range read as the default", () => {
  assert.deepEqual(readFlowsUrl(new URLSearchParams("range=24h&flow=orders")), { range: "24h", flow: "orders" });
  assert.deepEqual(readFlowsUrl(new URLSearchParams("range=7d")), { range: "1h", flow: undefined });
  assert.equal(flowsHref({ range: "1h", flow: undefined }), "/overview");
  assert.equal(flowsHref({ range: "15m", flow: "a b" }), "/overview?range=15m&flow=a+b");
});

test("the window is clipped to what the server keeps, and says so", () => {
  assert.deepEqual(flowsWindow("24h", NOW, null), { fromMs: NOW - 86_400_000, toMs: NOW, minutes: 1440, clipped: false });
  const w = flowsWindow("24h", NOW, 6 * 3_600_000);
  assert.equal(w.fromMs, NOW - 6 * 3_600_000);
  assert.equal(w.minutes, 360);
  assert.equal(w.clipped, true);
});

test("a cause group finds its flow by queue key, else by a name only one flow has", () => {
  const a = flow("orders", { key: "111/eu-west-1/orders", queue: "orders (111)", account: "111" });
  const b = flow("orders", { key: "222/eu-west-1/orders", queue: "orders (222)", account: "222" });
  const c = flow("billing");
  assert.equal(flowOfGroup([a, b, c], { queue: "orders", queueKey: "222/eu-west-1/orders" }), b);
  assert.equal(flowOfGroup([a, b, c], { queue: "orders", queueKey: "x" }), undefined);
  assert.equal(flowOfGroup([a, b, c], { queue: "billing", queueKey: "x" }), c);
});

test("a queue's figures with no AWS read: settings from the spans, depth and age unknown", () => {
  const l = spanOnlyLive({ view: view(), fifo: false, dlq: null, stuck: 2 });
  assert.equal(l.status, "not_configured");
  assert.equal(l.maxReceiveCount, 3);
  assert.equal(l.visibilityTimeoutS, 30);
  assert.equal(l.dlq, "orders-dlq");
  assert.equal(l.visible, null);
  assert.equal(l.stuck, 2);
  assert.equal(spanOnlyLive({ view: view({ config: { ...view().config, visibilityKnown: false } }), fifo: true, dlq: "x", stuck: 0 }).visibilityTimeoutS, null);
  assert.equal(spanOnlyLive({ view: null, fifo: false, dlq: "seen-dlq", stuck: 0 }).dlq, "seen-dlq");
});

test("health is from spans alone: DLQ moves are critical, stuck messages a warning, a quiet queue healthy", () => {
  const d = data([flow("orders"), flow("billing"), flow("quiet")], page([group("billing")]), { orders: view({ seenOnDlq: 2 }), billing: view(), quiet: view() });
  assert.deepEqual([d.info.orders.health, d.info.orders.why, d.info.orders.healthBasis], ["critical", "DLQ moves", "spans"]);
  assert.equal(d.info.billing.health, "warning");
  assert.equal(d.info.billing.live.stuck, 3);
  assert.equal(d.info.quiet.health, "good");
  // rates per minute over the range, one decimal
  assert.equal(d.queues.find((q) => q.name === "orders")!.sentPerMin, 2);
  assert.deepEqual(d.causes.map((c) => [c.queue, c.count, c.href]), [["billing", 3, "/stuck?cause=abc123def456"]]);
});

test("a queue whose 'now' couldn't be read still has a verdict, and an unreadable /api/causes leaves no causes", () => {
  const d = data([flow("orders")], null, { orders: null });
  assert.equal(d.info.orders.inQueue, null);
  assert.equal(d.info.orders.health, "good");
  assert.deepEqual(d.causes, []);
});

const hrefs: OverviewFlowsHrefs = {
  range: (id) => `/overview?range=${id}`, queue: ({ name }) => `/queue?name=${name}`, issues: (q) => `/stuck?queue=${q}`, cause: (c) => c.href ?? "/stuck",
  events: (q) => `/events?q=${q}`, deadLettered: ({ queue }) => `/stuck?queue=${queue}`, message: (id) => `/message?id=${id}`,
};
const copy = { noHealth: (n: number) => `${n} queues, with no data on their health`, spansOnly: ". Health is from your services' spans only", noFigures: () => "Trailbeam makes no AWS calls." };

test("the open-source page draws its totals, Copy link and footnote in the view's slots, and says health is from spans", () => {
  const d = data([flow("orders")], page([]), { orders: view() });
  const out = renderToStaticMarkup(createElement(OverviewFlows, {
    data: d, copy, hrefs,
    slots: { actions: createElement("button", null, "Copy link"), queueFacts: (q: string) => createElement("p", null, `Totals for ${q}`), foot: createElement("p", null, "Counts are of the spans") },
  }));
  assert.match(out, /Copy link/);
  assert.match(out, /Totals for orders/);
  assert.match(out, /<footer[^>]*><p>Counts are of the spans<\/p><\/footer>/);
  assert.match(out, /All 1 queues are healthy. Health is from your services&#x27; spans only/);
  assert.match(out, /Trailbeam makes no AWS calls\./);
});

test("the sample draws none of the open-source slots", () => {
  const out = renderToStaticMarkup(createElement(OverviewFlows, {
    copy, hrefs, slots: { actions: createElement("b", null, "Copy link"), foot: createElement("p", null, "Counts are of the spans") },
  }));
  assert.doesNotMatch(out, /Copy link|Counts are of the spans/);
});
