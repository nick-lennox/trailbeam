/*
  QV-580: the hosted Event explorer in @trailbeam/ui/explorer (src/explorer). Columns and filters no event fills are
  left out, Columns and Export are in the results bar, body size and tenant are columns, the exports write what is
  shown, and the step chart is the server's when the loaded events miss part of the range. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  DEFAULT_COLUMNS, EventDetail, EventExplorer, TimePicker, cellText, cutShort, filledColumns, stepChart, stepChartQuery, toCsv, type ExplorerData,
} from "../src/explorer/index.ts";
import { eventsOfTimeline, preset, queryLanguage, type Ev } from "../src/model/index.ts";

const NOW = Date.UTC(2026, 10, 27, 12, 0, 0);
const ev = (over: Partial<Ev> = {}): Ev => ({
  key: `m-1:${over.event ?? "send"}:${over.ms ?? NOW - 60_000}`, ms: NOW - 60_000, event: "send", status: "ok", queue: "orders", messageId: "m-1",
  service: "checkout", producer: "checkout", consumer: "orders-worker", detail: "", source: "package", hasPage: true, live: true, attrs: {}, ...over,
});
const data = (events: Ev[], over: Partial<ExplorerData> = {}): ExplorerData => ({ events, now: NOW, live: true, capped: false, ...over });
const draw = (events: Ev[], over: Partial<ExplorerData> = {}, query = "") => renderToStaticMarkup(h(EventExplorer, {
  query, setQuery: () => {}, time: preset("1h", NOW), data: data(events, over), messageHref: (id: string) => `/messages/${id}`, page: true,
}));
const headers = (html: string) => [...(html.match(/<thead[\s\S]*?<\/thead>/)?.[0] ?? "").matchAll(/<th(?:\s[^>]*)?>(?:<span[^>]*>)?([^<]*)/g)].map((m) => m[1]);

test("a column no event in the range fills is left out; date, event, message ID and source are always there", () => {
  const plain = [ev(), ev({ event: "receive", ms: NOW - 30_000, service: "orders-worker", receiveCount: 1, waitMs: 30_000 })];
  const got = filledColumns(DEFAULT_COLUMNS, plain);
  for (const c of ["date", "event", "message_id", "source", "queue", "service", "receive_count", "time_in_queue"]) assert.ok(got.has(c), c);
  for (const c of ["tenant", "body_size", "processing_time", "body", "detail"]) assert.ok(!got.has(c), c);
  // what fills them: a tenant, a body size, a kept body and words
  const full = filledColumns(DEFAULT_COLUMNS, [...plain, ev({ ms: NOW - 10_000, tenant: "acme", sizeBytes: 812, body: { id: 1 }, detail: "Received from the dead-letter queue" })]);
  for (const c of ["tenant", "body_size", "body", "detail"]) assert.ok(full.has(c), c);
});

test("the table shows only the filled columns, with body size and tenant once an event carries them", () => {
  assert.deepEqual(headers(draw([ev()])), ["Date", "Event", "Queue", "Message ID", "Service", "Source"]);
  const html = draw([ev({ tenant: "acme", sizeBytes: 812 })]);
  assert.deepEqual(headers(html), ["Date", "Event", "Queue", "Message ID", "Service", "Tenant", "Body size", "Source"]);
  assert.match(html, />812 B</);
  assert.match(html, />acme</);
});

test("Columns and Export are in the results bar over the list, not in the table's header", () => {
  const html = draw([ev(), ev({ event: "receive", ms: NOW - 1000 })]);
  const bar = html.match(/<div[^>]*data-results-bar[\s\S]*?<\/div><\/div>/)?.[0] ?? "";
  assert.match(bar, /<b[^>]*>2<\/b> events, newest first/);
  assert.match(bar, /Columns/);
  assert.match(bar, /Export/);
  const thead = html.match(/<thead[\s\S]*?<\/thead>/)![0];
  assert.doesNotMatch(thead, /Columns|Export|<button/);
  // and the footer it replaces is gone
  assert.doesNotMatch(html, />Newest first</);
});

test("a filter no event has a value for is left out of the filter panel, unless the query names it", () => {
  const html = draw([ev()]);
  assert.match(html, />Queue</);
  assert.doesNotMatch(html, />Tenant</);
  assert.doesNotMatch(html, /MessageGroupId/);
  assert.match(draw([ev()], {}, "tenant:acme"), />Tenant</);
});

test("the inferred events and visibility changes of a timeline are rows, with a level bar on the ones to look at", () => {
  const t = {
    message_id: "m-9", current_queue: "orders", events: [
      { id: 1, type: "sent", queue: "orders", end_unix_nano: String(BigInt(NOW - 50_000) * 1_000_000n), consumer: { service_name: "checkout" } },
      { id: 2, type: "received", queue: "orders", delivery: 1, receive_count: 1, end_unix_nano: String(BigInt(NOW - 40_000) * 1_000_000n), consumer: { service_name: "orders-worker" } },
      { id: 3, type: "visibility_changed", queue: "orders", delivery: 1, result: "ok", visibility_timeout_s: 120, end_unix_nano: String(BigInt(NOW - 39_000) * 1_000_000n), consumer: { service_name: "orders-worker" } },
      { id: 4, type: "visibility_timeout_expired", inferred: true, after_delivery: 1, window: { from: new Date(NOW - 39_000).toISOString(), to: new Date(NOW - 20_000).toISOString() } },
      { id: 5, type: "received", queue: "orders", delivery: 2, receive_count: 2, end_unix_nano: String(BigInt(NOW - 20_000) * 1_000_000n), consumer: { service_name: "orders-worker" } },
    ],
  };
  const evs = eventsOfTimeline(t);
  const vis = evs.find((e) => e.event === "visibility_change")!;
  assert.equal(vis.call, "ChangeMessageVisibility");
  assert.equal(vis.detail, "Visibility timeout set to 120 s");
  assert.equal(vis.receiveCount, 1);
  const html = draw(evs);
  assert.match(html, />ChangeMessageVisibility</);
  assert.match(html, />Visibility timeout expired</);
  assert.match(html, /--bar:var\(--color-warning\)/);
  assert.equal(queryLanguage().matches(vis, queryLanguage().parse("event:visibility_change")), true);
});

test("the exports write each shown column in full, and CSV keeps a cell from running as a formula", () => {
  const e = ev({ tenant: "acme", sizeBytes: 2048, messageId: "6f1c2a0e-0000-4000-a000-000000000001", detail: "=HYPERLINK(1)" });
  assert.equal(cellText("message_id", e), "6f1c2a0e-0000-4000-a000-000000000001");
  assert.equal(cellText("body_size", e), "2.0 KB");
  assert.equal(cellText("tenant", e), "acme");
  assert.equal(cellText("event", e), "Send");
  const csv = toCsv({ headers: ["Detail", "Count"], rows: [[e.detail, "-3"], ['say "hi", twice', "4"]] });
  assert.equal(csv, '﻿Detail,Count\r\n\'=HYPERLINK(1),-3\r\n"say ""hi"", twice",4\r\n');
});

test("the step chart is the server's histogram, asked for when the loaded events miss the start of the range", () => {
  assert.equal(cutShort([{ ms: NOW - 1000 }], false, NOW - 3_600_000), false);
  assert.equal(cutShort([{ ms: NOW - 1000 }], true, NOW - 3_600_000), true);
  assert.equal(cutShort([{ ms: NOW - 3_600_000 }], true, NOW - 3_600_000), false);
  assert.deepEqual(stepChartQuery(1000.4, 2000), { from_ms: "1000", to_ms: "1999", limit: "1" });
  const c = stepChart({ histogram: { bucket_ms: 60_000, total: 5, buckets: [{ start_ms: 0, send: 2, receive: 1, delete: 0, change_visibility: 1, outcome: 0 }, { start_ms: 60_000, send: 1 }] } })!;
  assert.deepEqual(c, { from: 0, span: 60_000, per: "minute", total: 5, buckets: [{ ok: 4, err: 0 }, { ok: 1, err: 0 }] });
  assert.equal(stepChart({ histogram: null }), null);
  // the explorer says the list is cut short
  assert.match(draw([ev()], { capped: true }), /Only the most recent messages are loaded/);
});

test("an opened row lists only what the event carries, with the way to its message", () => {
  const html = renderToStaticMarkup(h(EventDetail, { event: ev({ tenant: "acme", attrs: { "queue.region": "eu-west-1" } }), messageHref: (id: string) => `/m/${id}` }));
  assert.match(html, /Tenant/);
  assert.match(html, /eu-west-1/);
  assert.doesNotMatch(html, /Body size|Receive count|Account|Message group/);
  assert.match(html, /href="\/m\/m-1"/);
});

test("the time picker names the range", () => {
  assert.match(renderToStaticMarkup(h(TimePicker, { time: preset("1d", NOW), setTime: () => {}, now: NOW })), />Past 1 day</);
});

test("no explorer part uses a native title for a hover note, an arrow or tracked capitals", () => {
  const dir = new URL("../src/explorer/", import.meta.url).pathname;
  for (const f of readdirSync(dir)) {
    const src = readFileSync(dir + f, "utf8");
    assert.doesNotMatch(src, /\stitle=\{|\stitle="/, f);
    assert.doesNotMatch(src, /ArrowRight|→|tracking-|uppercase/, f);
  }
});
