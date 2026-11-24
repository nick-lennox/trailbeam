/*
  QV-364: the queue page (src/queue, the QV-360 design) and the self-hosted shell's /queue and /queues.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  collapseMarkers, consoleUrl, coverageText, fmtMs, grid, onGrid, pageStep, queueQuery, queueWindow, readQueueUrl, spanFiguresOf, spanHealth, totalsIn,
} from "../src/queue/model.ts";
import { QueuePage, type QueueHrefs, type QueuePageProps } from "../src/queue/QueuePage.tsx";
import { fromSelfHosted, flowFor, type Flow, type SeriesResponse } from "../src/queue/selfhosted.ts";
import type { Bucket, QueuePageData } from "../src/queue/types.ts";

const MIN = 60_000;
const NOW = Date.UTC(2026, 10, 24, 15, 0, 0);
const url = (q: string) => { const p = new URLSearchParams(q); return readQueueUrl((k) => p.get(k)); };

test("the page's step: max(60 s, nice(range / 120)), the server's rule", () => {
  assert.equal(pageStep(0, 3_600_000), 60);
  assert.equal(pageStep(0, 86_400_000), 900);
  assert.equal(pageStep(0, 7 * 86_400_000), 10_800);
  assert.equal(pageStep(0, 5 * MIN), 60);
  assert.equal(pageStep(0, 400 * 86_400_000), 21_600);
});

test("the URL is the page's state, and reads back to the same query", () => {
  const u = url("name=orders&range=4h&sel=1000-61000&split=version&at=123456789012:eu-west-1");
  assert.deepEqual({ ...u, problems: undefined }, { name: "orders", at: "123456789012:eu-west-1", range: { kind: "preset", preset: "4h" }, sel: [1000, 61000], split: true, problems: undefined });
  assert.equal(queueQuery(u), "name=orders&at=123456789012%3Aeu-west-1&range=4h&sel=1000-61000&split=version");
  // the default range is left out; a fixed range is from/to
  assert.equal(queueQuery(url("name=orders")), "name=orders");
  assert.equal(queueQuery(url("name=orders&from=1000&to=2000")), "name=orders&from=1000&to=2000");
  assert.equal(queueQuery(url("name=o"), { withName: false }), "");
  // what can't be read falls back and says so
  assert.deepEqual(url("name=o&range=9y").range, { kind: "preset", preset: "1h" });
  assert.equal(url("name=o&range=9y").problems.length, 1);
  assert.equal(url("name=o&from=5&to=4").range.kind, "preset");
  assert.equal(url("name=o&sel=9-3").sel, null);
});

test("a preset ends now; a fixed range ends where it says", () => {
  assert.deepEqual(queueWindow({ kind: "preset", preset: "1h" }, NOW), { fromMs: NOW - 3_600_000, toMs: NOW, stepS: 60, endsNow: true });
  assert.deepEqual(queueWindow({ kind: "fixed", fromMs: 0, toMs: 86_400_000 }, NOW), { fromMs: 0, toMs: 86_400_000, stepS: 900, endsNow: false });
});

test("health from spans: the hosted rules, and no verdict for a queue with no activity", () => {
  const f = { seenOnDlq: 0, moved: 0, stuck: 0, failingNow: false, retrying: false, active: true };
  assert.deepEqual(spanHealth(f), { tone: "good", why: "Healthy", basis: "spans" });
  assert.equal(spanHealth({ ...f, seenOnDlq: 1 })?.why, "DLQ moves");
  assert.equal(spanHealth({ ...f, moved: 2 })?.why, "Near the DLQ");
  assert.equal(spanHealth({ ...f, stuck: 2 })?.tone, "warning");
  assert.equal(spanHealth({ ...f, active: false }), null);
  // DLQ moves seen count only from the last hour of the range; worked-out moves from all of it
  const b = (t: number, o: Partial<Bucket> = {}): Bucket => ({ t, sends: 1, receives: 1, reReceives: 0, handled: 1, failed: 0, dlqSeen: 0, dlqInferred: 0, p95QueueMs: 10, p95HandlerMs: 20, noSendReceives: 0, ...o });
  const figs = spanFiguresOf([b(NOW - 2 * 3_600_000, { dlqSeen: 3, dlqInferred: 1 }), b(NOW - MIN)], NOW, { stuck: 0, failingNow: false, retrying: false });
  assert.deepEqual([figs.seenOnDlq, figs.moved, figs.active], [0, 1, true]);
});

test("the SQS console link: the queue, else the region's list, else none", () => {
  assert.equal(consoleUrl({ name: "orders", account: "123456789012", region: "eu-west-1" }),
    "https://console.aws.amazon.com/sqs/v3/home?region=eu-west-1#/queues/https%3A%2F%2Fsqs.eu-west-1.amazonaws.com%2F123456789012%2Forders");
  assert.equal(consoleUrl({ name: "orders", account: null, region: "eu-west-1" }), "https://console.aws.amazon.com/sqs/v3/home?region=eu-west-1#/queues");
  assert.equal(consoleUrl({ name: "orders", account: "123456789012", region: null }), null);
  assert.equal(consoleUrl({ name: "orders", account: "000000000000", region: "elasticmq" }), null);
  assert.match(consoleUrl({ name: "o", account: null, region: "us-gov-west-1" }) ?? "", /region=us-gov-west-1/);
});

test("the coverage line: within 2% matches, CloudWatch above says by how much, spans above says nothing", () => {
  assert.equal(coverageText(1000, 990), "CloudWatch's sends match what your services reported.");
  assert.equal(coverageText(12480, 11960), "CloudWatch counted 12,480 sends; spans recorded 11,960. 520 came from services without Trailbeam, or spans that didn't arrive.");
  assert.equal(coverageText(900, 1000), null);
  assert.equal(coverageText(null, 1000), null);
});

test("durations, the grid, CloudWatch points on it, deploy markers and a window's totals", () => {
  assert.deepEqual([fmtMs(840), fmtMs(2800), fmtMs(41_000), fmtMs(372_000), fmtMs(3 * 3_600_000 + 5 * MIN)], ["840 ms", "2.8 s", "41 s", "6 min 12 s", "3 h 5 min"]);
  const times = grid(NOW - 5 * MIN + 10, NOW, 60);
  assert.equal(times.length, 6);
  assert.deepEqual(onGrid([[times[1] + 5_000, 7], [times[3], 2]], times, 60), [null, 7, null, 2, null, null]);
  const ms = [{ firstMs: 0 }, { firstMs: 10 }, { firstMs: 20 }, { firstMs: 900 }];
  assert.deepEqual(collapseMarkers(ms, 0, 1000).map((g) => g.items.length), [3, 1]);
  assert.deepEqual(collapseMarkers(ms.slice(0, 2), 0, 1000).map((g) => g.items.length), [1, 1]);
  const bs: Bucket[] = [0, 1, 2].map((i) => ({ t: i * MIN, sends: 2, receives: 3, reReceives: 1, handled: 2, failed: 1, dlqSeen: 1, dlqInferred: 0, p95QueueMs: 1, p95HandlerMs: 1, noSendReceives: 0 }));
  assert.deepEqual(totalsIn(bs, MIN, 3 * MIN), { sends: 4, receives: 6, handled: 4, failedOrRetried: 4, dlq: 2 });
});

/* ── the view ────────────────────────────────────────────────────────── */

const series: SeriesResponse = {
  queue: { key: "orders", name: "orders", account: "000000000000", region: "elasticmq", fifo: false },
  from_ms: NOW - 3_600_000, to_ms: NOW, step_s: 60, max_receive_count: 3,
  series: Array.from({ length: 60 }, (_, i) => ({
    t_ms: NOW - 3_600_000 + i * MIN, sends: 60 + i, receives: 62, re_receives: 2, handled: 58, failed: 1, dlq_seen: i === 30 ? 1 : 0, dlq_inferred: i === 40 ? 1 : 0,
    no_send_receives: 0, p95_queue_ms: 400 + i * 10, p95_handler_ms: 1200, queue_samples: 60, handler_samples: 60,
    by_version: [{ version: i < 30 ? "1.4.2" : "1.4.3", handled: 58, p95_queue_ms: 400, p95_handler_ms: 1200, queue_samples: 60, handler_samples: 60 }],
  })),
  versions: [{ service: "orders-worker", version: "1.4.3", first_ms: NOW - 30 * MIN }],
};
const flow: Flow = {
  key: "orders", queue: "orders", name: "orders", url: null, account: null, region: null, lastSeen: NOW, sent: 3600, received: 3700, deleted: 3500, visibilityChanges: 0,
  producers: [{ name: "checkout-api", n: 3600, lambda: false }], consumers: [{ name: "orders-worker", n: 3700, lambda: false }], avgBytes: null, dlq: { name: "orders-dlq", received: 3 },
};
const href: QueueHrefs = {
  queues: "/queues", queue: (n) => `/queue?name=${n}`, stuck: (q) => `/stuck?queue=${q}`, range: (p) => `/queue?name=orders&range=${p}`,
  messages: ({ queue, fromMs, toMs, view }) => (view === "sent" ? `/?queue=${queue}&from_ms=${fromMs}&to_ms=${toMs - 1}` : null),
  connect: "/app/ws/connections",
};
const render = (o: Partial<QueuePageProps>) => renderToStaticMarkup(createElement(QueuePage, {
  name: "orders", data: null, href, sel: null, onSelect: () => {}, split: false, onSplit: () => {}, ...o,
}));
const selfHosted = () => fromSelfHosted({ series, flow, stuck: { count: 4, more: false }, endsNow: true });

test("self-hosted: spans only, a verdict labelled as such, and what Trailbeam can't see", () => {
  const d = selfHosted();
  assert.equal(d.aws, null);
  assert.deepEqual(d.health, { tone: "critical", why: "DLQ moves", basis: "spans" });
  assert.deepEqual(d.dlqSeen, { name: "orders-dlq" });
  const html = render({ data: d });
  assert.match(html, /from your services&#x27; spans only/);
  assert.match(html, /What Trailbeam can’t see here/);
  assert.match(html, /how many messages are waiting or in flight/);
  assert.doesNotMatch(html, /From CloudWatch/);
  assert.match(html, /Sends vs handled/);
  assert.match(html, /p95 time in handler/);
  assert.match(html, /Not known/); // the visibility timeout: no AWS calls
  assert.match(html, /seen on a move/);
  assert.match(html, />3 <span class="font-serif text-ink-3">set on this server/);
  assert.match(html, /orders-worker 1\.4\.3/); // the deploy marker
  assert.match(html, /By version/);
  assert.match(html, />4<\/span> stuck messages/);
  assert.match(html, /Sends\/min<\/span><span class="font-mono tabular-nums text-\[1.05rem\] text-ink">117</); // the last 5 complete minutes: (115+…+119)/5
  assert.doesNotMatch(html, /messages in the queue|queue contents|0 messages waiting/i);
  // the fixture's queue is on an emulator ("elasticmq" is no AWS region): no console link
  assert.doesNotMatch(html, /Open in the SQS console/);
  // self-hosted counts only receives per consumer: the other columns are left out, not filled with dashes
  assert.doesNotMatch(html, />p95 handler</);
});

test("the spike's links: only the ones this product can open, with their counts", () => {
  const d = selfHosted();
  const html = render({ data: d, sel: [NOW - 30 * MIN, NOW - 28 * MIN] });
  assert.match(html, /Messages in <span class="font-mono">14:30–14:32<\/span>/);
  assert.match(html, /Sent <span class="font-mono tabular-nums">181<\/span>/); // 90 + 91
  assert.doesNotMatch(html, /Waited longest/); // no sort on this server's lists yet: dropped, not unsorted
});

test("hosted, no AWS account: the CloudWatch block with its reason and a Connect link", () => {
  const d: QueuePageData = { ...selfHosted(), configured: null, health: null, noHealth: "No verdict without AWS", aws: { status: "no_account", detail: null, readAt: null, settings: null, series: null, errors: [] } };
  const html = render({ data: d });
  assert.match(html, /Depth, in flight, the oldest message’s age and the DLQ’s depth come from CloudWatch/);
  assert.match(html, /No connected AWS account names this queue/);
  assert.match(html, /Connect an AWS account →/);
  assert.match(html, /No verdict without AWS/);
  assert.match(html, /Not read/);
  assert.doesNotMatch(html, /What Trailbeam can’t see here/);
});

test("hosted, AWS read: settings from SQS, the Now row's AWS figures, the CloudWatch charts and the coverage line", () => {
  const base = selfHosted();
  const t0 = base.range.fromMs;
  const pts = (v: (i: number) => number): [number, number][] => Array.from({ length: 60 }, (_, i) => [t0 + i * MIN, v(i)]);
  const d: QueuePageData = {
    ...base, configured: null, health: { tone: "warning", why: "Backlog growing", basis: "aws" },
    aws: {
      status: "ok", detail: null, readAt: NOW - 30_000,
      settings: { visibilityTimeoutS: 30, retentionS: 345_600, delayS: 0, maxReceiveCount: 5, dlqName: "orders-dlq" },
      series: { visible: pts((i) => i * 20), inFlight: pts(() => 40), oldestS: pts((i) => i * 6), sent: pts(() => 120), dlq: pts(() => 17) },
      now: { visible: 1284, inFlight: 40, oldestS: 372, oldestAt: NOW - 60_000, dlqDepth: 17, dlqAt: NOW - 60_000, dlqChange: 9 },
      errors: [],
    },
    consumers: [{ name: "orders-worker", kind: "lambda", receives: 9104, handled: 8880, failed: 224, p95HandlerMs: 2800, versions: ["1.4.2", "1.4.3"], lastMs: NOW,
      trigger: { functionName: "orders-worker", state: "Enabled", batchSize: 10, batchingWindowS: 0, reportBatchItemFailures: true, maxConcurrency: 5, filters: 1 } }],
    silentTriggers: [{ functionName: "orders-audit", state: "Disabled", batchSize: 10, batchingWindowS: null, reportBatchItemFailures: false, maxConcurrency: null, filters: 0 }],
  };
  const html = render({ data: d });
  assert.match(html, /from SQS and CloudWatch/);
  assert.match(html, /SQS · read 14:59:30/);
  assert.match(html, /Visibility timeout<\/span> <span class="font-mono tabular-nums text-ink-2">30 s/);
  assert.match(html, /1,284/);
  assert.match(html, /6 min 12 s/);
  assert.match(html, /From CloudWatch, read with your role/);
  assert.match(html, /Visible and in flight/);
  assert.match(html, /DLQ depth/);
  assert.match(html, /visibility timeout 30 s/); // the rule on the p95 charts
  assert.match(html, /90% of retention/);
  // CloudWatch 60 × 120 = 7,200 sends against the spans' 5,370
  assert.match(html, /CloudWatch counted 7,200 sends; spans recorded 5,370\. 1,830 came from services without Trailbeam/);
  assert.match(html, /trigger: .*batch 10.*max concurrency 5.*reports batch item failures.*1 filter/);
  assert.match(html, /No spans from this function in the range/);
  assert.match(html, />p95 handler</);
  assert.match(html, /text-critical-text">Disabled/);
  assert.doesNotMatch(html, /What Trailbeam can’t see here/);
});

test("states: loading draws the frames, a failed query says so, an idle range offers longer ones, an unknown queue says what we can't know", () => {
  const loading = render({});
  assert.match(loading, /aria-busy="true"/);
  assert.doesNotMatch(loading, /spinner/);
  assert.match(render({ error: "Trailbeam answered 500: boom", onRetry: () => {} }), /Couldn’t load this queue’s spans: Trailbeam answered 500: boom.*Try again/);
  const idle = render({ data: { ...selfHosted(), series: [], health: null } });
  assert.match(idle, /No send or receive on/);
  assert.match(idle, /Past 24 hours/);
  assert.match(idle, /No verdict: no span reported on this queue in the range/);
  const unknown = render({ unknown: true });
  assert.match(unknown, /No service has reported a call on/);
  assert.doesNotMatch(unknown, /doesn’t exist|does not exist/);
});

test("flowFor: by key, else by a name only one flow has", () => {
  const a = { ...flow, key: "1/eu/orders" }, b = { ...flow, key: "2/eu/orders" };
  assert.equal(flowFor([a, b], { ...series.queue, key: "2/eu/orders" }), b);
  assert.equal(flowFor([a, b], series.queue), null);
  assert.equal(flowFor([a], series.queue), a);
});

test("a chart's rule far above its data stays off the scale; y labels fit and aren't repeated", async () => {
  const { Chart } = await import("../src/queue/Chart.tsx");
  const base = { title: "t", source: "spans", times: [NOW, NOW + MIN], stepS: 60, legend: [], summary: "s", hover: null, onHover: () => {}, sel: null };
  const line = (values: (number | null)[]) => [{ key: "a", label: "a", color: "red", values }];
  const fmtS = (v: number) => (v >= 60 ? `${Math.floor(v / 60)} min ${Math.round(v % 60)} s` : `${v} s`);
  // a 4-day retention over ages of seconds: named at the top, the scale stays on the data
  const far = renderToStaticMarkup(createElement(Chart, { ...base, lines: line([2, 4]), fmt: fmtS, rules: [{ value: 345_600 * 0.9, label: "90% of retention" }] }));
  assert.match(far, /90% of retention: above this scale/);
  assert.doesNotMatch(far, />[^<]* min[^<]*<\/text>/, "no axis label in minutes or days for data in seconds");
  // a visibility timeout near the data is drawn on the scale
  const near = renderToStaticMarkup(createElement(Chart, { ...base, lines: line([40, 50]), fmt: fmtS, rules: [{ value: 90, label: "visibility timeout" }] }));
  assert.match(near, />visibility timeout</);
  assert.doesNotMatch(near, /above this scale/);
  // long labels get room: the plot starts right of "1 min 30 s"
  const x1 = Number(/<line x1="([\d.]+)"/.exec(near)![1]);
  assert.ok(x1 >= 10 + 6.2 * "1 min 30 s".length - 0.01, String(x1));
  // all zeros: 0 and 1 once each, not "0, 1, 1"
  const zero = renderToStaticMarkup(createElement(Chart, { ...base, lines: line([0, 0]), fmt: (v: number) => String(Math.round(v)) }));
  assert.equal((zero.match(/>1<\/text>/g) ?? []).length, 1);
});
