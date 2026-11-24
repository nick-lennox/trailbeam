/*
  QV-398: the Overview (src/overview, the QV-392 design). The derived parts first (derive.ts), then the view's
  drawing of them. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  drawingLabel, filterFlows, fmtRate, fmtSeen, idleToggleText, isIdle, lambdaDeletes, notesOf, openKey, orderFlows, perMinute, servicesText,
  splitIdle, summaryOf, summaryText, NO_RECEIVE, NO_SEND, type Flow,
} from "../src/overview/derive.ts";
import { Overview, type OverviewProps } from "../src/overview/Overview.tsx";
import type { OverviewHrefs } from "../src/overview/FlowDrawing.tsx";

const HOUR = 3_600_000;
const NOW = Date.UTC(2026, 10, 24, 14, 5, 0);
const svc = (name: string, n: number, lambda = false) => ({ name, n, lambda });
const flow = (name: string, o: Partial<Flow> = {}): Flow => ({
  key: `123456789012/eu-west-1/${name}`, queue: name, name, url: null, account: "123456789012", region: "eu-west-1", lastSeen: NOW - 60_000,
  sent: 0, received: 0, deleted: 0, visibilityChanges: 0, producers: [], consumers: [], avgBytes: null, dlq: null, ...o,
});

const orders = flow("orders", {
  sent: 4120, received: 4388, deleted: 4097, visibilityChanges: 12, avgBytes: 812,
  producers: [svc("checkout-api", 4120)], consumers: [svc("orders-worker", 4388)], dlq: { name: "orders-dlq", received: 3 },
});
const payments = flow("payments.fifo", { sent: 1002, received: 1002, deleted: 0, producers: [svc("billing", 1002)], consumers: [svc("ledger", 1002, true)] });
const emails = flow("emails", { sent: 880, received: 901, deleted: 880, producers: [svc("notify", 500), svc("checkout-api", 380)], consumers: [svc("mailer", 901)] });
const audit = flow("audit", { sent: 312, producers: [svc("audit-writer", 312)] });
const dlq = flow("orders-dlq", { received: 3, deleted: 3, consumers: [svc("dlq-inspector", 3)] });
const old = flow("legacy", { lastSeen: Date.UTC(2026, 10, 21, 9, 14) });
const older = flow("legacy-2", { lastSeen: Date.UTC(2026, 10, 20, 9, 14) });
const FLOWS = [orders, payments, emails, audit, dlq, older, old];

test("a queue is idle when nothing at all happened on it in the range", () => {
  assert.equal(isIdle(old), true);
  assert.equal(isIdle(flow("x", { visibilityChanges: 1 })), false);
  assert.equal(isIdle(flow("x", { deleted: 1 })), false);
  const { active, idle } = splitIdle(FLOWS);
  assert.deepEqual(active.map((f) => f.name), ["orders", "payments.fifo", "emails", "audit", "orders-dlq"], "active rows keep the API's order");
  assert.deepEqual(idle.map((f) => f.name), ["legacy", "legacy-2"], "idle rows, most recently seen first");
  assert.equal(idleToggleText(3, "in the last hour"), "Show 3 queues with no activity in the last hour");
  assert.equal(idleToggleText(1, "in the last hour"), "Show 1 queue with no activity in the last hour");
});

test("the summary counts queues with activity and distinct service names, unknown_service once", () => {
  const s = summaryOf(FLOWS);
  assert.deepEqual(s, { queues: 5, senders: 4, receivers: 4 }, "checkout-api sends to two queues and counts once");
  assert.equal(summaryText(s, "in the last hour"), "5 queues had activity in the last hour. 4 services sent, 4 received.");
  const u = summaryOf([flow("a", { sent: 1, producers: [svc("unknown_service", 1)] }), flow("b", { sent: 1, producers: [svc("unknown_service", 1)] })]);
  assert.deepEqual(u, { queues: 2, senders: 1, receivers: 0 });
  assert.equal(summaryText({ queues: 1, senders: 1, receivers: 1 }, "between 13:05 and 14:05 UTC"), "1 queue had activity between 13:05 and 14:05 UTC. 1 service sent, 1 received.");
  assert.equal(summaryText({ queues: 0, senders: 0, receivers: 0 }, "in the last hour"), "No queue had activity in the last hour.");
});

test("notes stand in for a missing side, as what was reported", () => {
  assert.deepEqual(notesOf(orders), { senders: null, receivers: null });
  assert.deepEqual(notesOf(audit), { senders: null, receivers: NO_RECEIVE }, "sent and none received");
  assert.deepEqual(notesOf(dlq), { senders: NO_SEND, receivers: null }, "received and none sent: SQS moves to a DLQ");
  assert.deepEqual(notesOf(flow("x", { sent: 5, received: 5, consumers: [svc("w", 5)] })), { senders: NO_SEND, receivers: null }, "no producer reported");
  assert.deepEqual(notesOf(flow("x", { sent: 5, received: 5, producers: [svc("p", 5)] })), { senders: null, receivers: NO_RECEIVE }, "no consumer reported");
  assert.deepEqual(notesOf(old), { senders: null, receivers: null }, "idle rows show last seen instead");
});

test("service lists: at most two names, then +N, every name in the hover, (Lambda) on a Lambda consumer only", () => {
  const many = [svc("a", 9), svc("b", 8, true), svc("c", 7), svc("d", 6)];
  assert.deepEqual(servicesText(many, { side: "producers" }), { shown: "a, b", more: 2, all: "a, b, c, d" });
  assert.deepEqual(servicesText(many, { side: "consumers" }), { shown: "a, b (Lambda)", more: 2, all: "a, b (Lambda), c, d" });
  assert.deepEqual(servicesText([svc("a", 0)], { side: "producers" }), { shown: "", more: 0, all: "" }, "a service with nothing in the range isn't listed");
});

test("Lambda deletes: every consumer Lambda reads By Lambda, some is a count with + Lambda", () => {
  assert.equal(lambdaDeletes(payments), "all");
  assert.equal(lambdaDeletes(orders), "none");
  assert.equal(lambdaDeletes(flow("x", { consumers: [svc("w", 3), svc("fn", 4, true)] })), "mixed");
  assert.equal(lambdaDeletes(audit), "none", "no consumer at all");
});

test("the order freezes: a refresh updates figures in place and adds new queues at the bottom", () => {
  const fresh = orderFlows(FLOWS, null);
  assert.deepEqual(fresh.map((f) => f.name), FLOWS.map((f) => f.name), "the API's order without a host sort");
  const keys = fresh.map((f) => f.key);
  /* on refresh the API reorders (emails overtook payments), audit is gone, a new queue appeared */
  const next = [emails, { ...orders, sent: 5000 }, flow("new"), payments, dlq, old, older];
  const kept = orderFlows(next, keys);
  assert.deepEqual(kept.map((f) => f.name), ["orders", "payments.fifo", "emails", "orders-dlq", "legacy-2", "legacy", "new"]);
  assert.equal(kept[0].sent, 5000, "figures update in place");
  const sorted = orderFlows(FLOWS, null, (a, b) => a.sent - b.sent);
  assert.equal(sorted[0].sent, 0, "the host's sort when it gives one");
});

test("flow= opens a row by key, by an old link's label, the first by default, none when empty or unknown", () => {
  assert.equal(openKey(FLOWS, undefined), orders.key);
  assert.equal(openKey(FLOWS, null), orders.key);
  assert.equal(openKey(FLOWS, ""), null);
  assert.equal(openKey(FLOWS, emails.key), emails.key);
  assert.equal(openKey(FLOWS, "emails"), emails.key, "?flow=emails from before keys");
  assert.equal(openKey(FLOWS, "nope"), null);
  assert.equal(openKey([], undefined), null);
});

test("the filter matches queue and service names, ignoring case", () => {
  assert.deepEqual(filterFlows(FLOWS, "CHECKOUT").map((f) => f.name), ["orders", "emails"]);
  assert.deepEqual(filterFlows(FLOWS, "dlq").map((f) => f.name), ["orders-dlq"]);
  assert.equal(filterFlows(FLOWS, "  ").length, FLOWS.length);
});

test("figures: rates over the range, last seen in UTC, the drawing's spoken label", () => {
  assert.equal(perMinute(4120, NOW - HOUR, NOW), 4120 / 60);
  assert.equal(fmtRate(4120 / 60), "68.7");
  assert.equal(fmtRate(1234.4), "1,234");
  assert.equal(fmtRate(0), "0");
  assert.equal(fmtSeen(Date.UTC(2026, 10, 21, 9, 14)), "Nov 21, 09:14");
  assert.equal(drawingLabel(orders, NOW - HOUR, NOW), "orders: checkout-api sends 68.7 a minute; orders-worker receives 73.1 a minute; dead-letter queue orders-dlq, 3 received.");
  assert.equal(drawingLabel(audit, NOW - HOUR, NOW), "audit: audit-writer sends 5.2 a minute; no receive reported.");
});

/* ── the view ─────────────────────────────────────────────────────────── */

const href: OverviewHrefs = {
  queue: (q, part) => `/queue?name=${encodeURIComponent(q.name)}${part ? `#${part}` : ""}`,
  stuck: (q) => `/stuck?queue=${encodeURIComponent(q.name)}`,
  setUp: "/",
};
const props = (o: Partial<OverviewProps> = {}): OverviewProps => ({
  flows: FLOWS, range: { fromMs: NOW - HOUR, toMs: NOW, during: "in the last hour", endsNow: true }, rangeControl: "[range]",
  open: undefined, onOpen: () => {}, showIdle: false, onShowIdle: () => {}, state: { kind: "ok", updatedAt: NOW },
  refresh: { on: true, toggle: () => {}, now: () => {} }, href, ...o,
});
const html = (o: Partial<OverviewProps> = {}) => renderToStaticMarkup(createElement(Overview, props(o)));

test("the table: a row per active queue, the first open, idle ones behind the toggle", () => {
  const h = html();
  assert.match(h, /5 queues had activity in the last hour\. 4 services sent, 4 received\./);
  assert.match(h, /<th scope="col"[^>]*>Queue<\/th>/);
  assert.equal((h.match(/aria-expanded="true"/g) ?? []).length, 1, "one row open");
  assert.match(h, /aria-label="Close orders"/);
  assert.match(h, /role="img" aria-label="orders: checkout-api sends 68\.7 a minute/);
  assert.match(h, /Show 2 queues with no activity in the last hour/);
  assert.doesNotMatch(h, /legacy/, "idle rows hidden until asked for");
  assert.match(h, /By Lambda/);
  assert.match(h, new RegExp(NO_RECEIVE));
  assert.match(h, /Stuck on orders →/);
  assert.match(h, /href="\/queue\?name=orders#senders"/);
  assert.match(h, /fifo<\/span>/);
  assert.match(h, /⟳ Updated 14:05:00/);
  assert.doesNotMatch(h, /Needs attention|Healthy|healthy|[Bb]acklog|depth/, "no hosted column, no health words in self-hosted");
});

test("idle rows appear with last seen when shown; flow= empty opens none", () => {
  const h = html({ showIdle: true, open: "" });
  assert.match(h, /last seen Nov 21, 09:14/);
  assert.equal((h.match(/aria-expanded="true"/g) ?? []).length, 0);
  assert.doesNotMatch(h, /Show 2 queues/);
});

test("hosted slots draw their columns; absent slots draw nothing", () => {
  const h = html({
    stuckWord: "Needs attention",
    slots: {
      health: (f) => `[pill ${f.name}]`,
      columns: [{ id: "visible", header: "Visible", cell: (f) => (f.name === "orders" ? "1,284" : "Not read") }],
      attention: (f) => (f.name === "orders" ? { count: 31, tone: "dead_lettered" } : { count: 0, tone: "stuck" }),
      alert: () => "[bell]",
      queueCard: () => "[card]",
      summary: "2 of 6 queues need attention.",
    },
  });
  assert.match(h, />Visible<\/th>/);
  assert.match(h, /Needs attention<\/th>/);
  assert.match(h, /\[pill orders\]/);
  assert.match(h, /text-critical-text[^>]*>.*31/);
  assert.match(h, /\[card\]/);
  assert.match(h, /Needs attention on orders →/);
  assert.match(h, /2 of 6 queues need attention\./);
});

test("edge states: empty store, nothing in range, failure, 501, loading, a fixed range", () => {
  assert.match(html({ state: { kind: "empty-store" }, flows: [] }), /No queue yet\..*Set up a service →/);
  const none = html({ flows: [old], widen: [{ label: "Last 24 hours", href: "/overview?range=24h" }] });
  assert.match(none, /No send or receive on any queue in the last hour\./);
  assert.match(none, /Last 24 hours/);
  assert.match(none, /last seen Nov 21/, "the idle list, open by default");
  assert.doesNotMatch(none, /queues had activity/);
  assert.match(html({ state: { kind: "error", message: "HTTP 500" }, onRetry: () => {} }), /Couldn’t load the flows: HTTP 500\..*Try again/);
  const stale = html({ state: { kind: "error", message: "HTTP 500", staleAt: NOW, at: NOW + 15_000 } });
  assert.match(stale, /Couldn’t update at 14:05:15: HTTP 500\. Showing 14:05:00\./);
  assert.match(stale, /orders/, "the figures stay");
  assert.match(html({ state: { kind: "unsupported" } }), /This store doesn’t keep the counts the Overview needs/);
  assert.equal((html({ state: { kind: "loading" } }).match(/<tr aria-hidden/g) ?? []).length, 3);
  const fixed = html({ range: { fromMs: NOW - HOUR, toMs: NOW, during: "between 13:05 and 14:05 UTC", endsNow: false } });
  assert.doesNotMatch(fixed, /Updated|Pause/, "a fixed range never refreshes and shows no control");
});

test("many queues: the filter box from 13 rows, the first 50, then Show N more", () => {
  const many = Array.from({ length: 60 }, (_, i) => flow(`q${i}`, { sent: 100 - i, producers: [svc("p", 100 - i)] }));
  const h = html({ flows: many, onQuery: () => {} });
  assert.match(h, /aria-label="Filter queues"/);
  assert.match(h, /Show 10 more/);
  assert.doesNotMatch(html({ flows: many.slice(0, 12), onQuery: () => {} }), /Filter queues/);
});

/* ── self-hosted Trailbeam's /overview (overview/selfhosted.ts) ──────── */

import { overviewHref, overviewWindow, pinnedOverview, readOverviewUrl, selfHostedHrefs, widerPresets, type OverviewUrl } from "../src/overview/selfhosted.ts";

const ou = (q: string) => { const r = readOverviewUrl(Object.fromEntries(new URLSearchParams(q))); if ("error" in r) throw new Error(r.error); return r; };

test("/overview's URL: the past hour by default, no all time, flow= idle=1 refresh=0 q= read back the same", () => {
  assert.deepEqual(ou(""), { range: { kind: "preset", preset: "1h" }, flow: undefined, idle: false, refresh: true, q: "" });
  assert.equal(overviewHref(ou("")), "/overview");
  for (const q of ["range=24h", "from_ms=1000&to_ms=2000", "flow=a%2Fb%2Corders&idle=1&refresh=0&q=check", "flow="]) {
    assert.equal(overviewHref(ou(q)), `/overview?${q}`);
  }
  assert.deepEqual(readOverviewUrl({ range: "30d" }), { error: "range must be 1h, 24h or 7d, not “30d”." });
});

test("/overview's window: a preset ends now, a fixed range doesn't, and Copy link pins it", () => {
  assert.deepEqual(overviewWindow(ou("").range, NOW), { fromMs: NOW - HOUR, toMs: NOW, during: "in the last hour", endsNow: true });
  const fixed = overviewWindow(ou(`from_ms=${NOW - HOUR}&to_ms=${NOW}`).range, NOW + 5 * HOUR);
  assert.equal(fixed.endsNow, false);
  assert.match(fixed.during, /^between 2026-11-24 13:05 UTC and 2026-11-24 14:05 UTC$/);
  assert.equal(pinnedOverview(ou("range=24h&refresh=0&flow=k"), NOW), `/overview?from_ms=${NOW - 24 * HOUR}&to_ms=${NOW}&flow=k`);
  assert.deepEqual(widerPresets(ou("")).map((w) => w.label), ["Last 24 hours", "Last 7 days"]);
  assert.deepEqual(widerPresets(ou("range=7d")), []);
});

test("/overview's links carry the range: the queue page in its own terms, Stuck with range or from_ms/to_ms", () => {
  const h = selfHostedHrefs(ou("range=24h"), NOW);
  assert.equal(h.queue(orders), "/queue?name=orders&range=1d");
  assert.equal(h.queue(orders, "consumers"), "/queue?name=orders&range=1d#consumers");
  assert.equal(h.queue({ ...orders, queue: "orders (123456789012)" }), "/queue?name=orders&at=123456789012%3Aeu-west-1&range=1d", "at= only when the label told two apart");
  assert.equal(h.stuck(orders), "/stuck?queue=orders&range=24h");
  assert.equal(selfHostedHrefs(ou(""), NOW).queue(orders), "/queue?name=orders", "1h is both pages' default");
  const f = selfHostedHrefs(ou("from_ms=1000&to_ms=2000"), NOW);
  assert.equal(f.queue(orders), "/queue?name=orders&from=1000&to=2000");
  assert.equal(f.stuck(orders), "/stuck?queue=orders&from_ms=1000&to_ms=2000");
});
