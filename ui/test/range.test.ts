/*
  QV-366: the time range on Stuck's list, and the pinned links "Copy link" makes (src/messages/range.ts,
  shell/lists.tsx, shell/chrome.tsx). Messages and Duplicates use the shared list's TimePicker (shell-messages.test.ts).
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  apiQuery, formQuery, fromInputUtc, pinnedQuery, rangeText, rangeWindow, readRange, refreshOn, toInputUtc, type TimeRange,
} from "../src/messages/range.ts";
import { Filters } from "../shell/lists.tsx";
import { CopyLink } from "../shell/chrome.tsx";

const NOW = Date.UTC(2026, 10, 23, 12, 0, 0); // 2026-11-23 12:00 UTC
const ok = (params: Record<string, string>): TimeRange => {
  const r = readRange(params);
  assert.ok(!("error" in r), JSON.stringify(r));
  return r as TimeRange;
};

test("the URL's range: a preset, a fixed window, or all history", () => {
  assert.deepEqual(readRange({}), { kind: "all" });
  assert.deepEqual(readRange({ range: "24h" }), { kind: "preset", preset: "24h" });
  assert.deepEqual(readRange({ from_ms: "1000", to_ms: "2000" }), { kind: "custom", from_ms: 1000, to_ms: 2000 });
  assert.deepEqual(readRange({ from_ms: "1000" }), { kind: "custom", from_ms: 1000, to_ms: undefined });
  // a preset wins over stray times: range= is what the control last said
  assert.deepEqual(readRange({ range: "1h", from_ms: "5" }), { kind: "preset", preset: "1h" });
});

test("a range the API would refuse is said on the page instead", () => {
  assert.match((readRange({ range: "2h" }) as { error: string }).error, /1h, 24h or 7d/);
  assert.match((readRange({ from_ms: "yesterday" }) as { error: string }).error, /from_ms/);
  assert.match((readRange({ to_ms: "-1" }) as { error: string }).error, /to_ms/);
  assert.match((readRange({ from_ms: "3000", to_ms: "2000" }) as { error: string }).error, /starts after it ends/);
});

test("a preset is a window ending now, worked out again at each ask", () => {
  assert.deepEqual(rangeWindow(ok({ range: "1h" }), NOW), { from_ms: NOW - 3_600_000, to_ms: NOW });
  assert.deepEqual(rangeWindow(ok({ range: "7d" }), NOW), { from_ms: NOW - 604_800_000, to_ms: NOW });
  // fifteen seconds later (a refresh), the window has moved along
  assert.deepEqual(rangeWindow(ok({ range: "1h" }), NOW + 15_000), { from_ms: NOW - 3_600_000 + 15_000, to_ms: NOW + 15_000 });
  assert.deepEqual(rangeWindow(ok({}), NOW), {});
  assert.deepEqual(rangeWindow(ok({ from_ms: "10", to_ms: "20" }), NOW), { from_ms: 10, to_ms: 20 });
});

test("the API gets from_ms/to_ms, never range= or refresh=; Stuck gets no to_ms", () => {
  const params = { queue: "orders", range: "24h", refresh: "15" };
  assert.deepEqual(apiQuery(params, ok(params), NOW), { queue: "orders", from_ms: String(NOW - 86_400_000), to_ms: String(NOW) });
  assert.deepEqual(apiQuery(params, ok(params), NOW, false), { queue: "orders", from_ms: String(NOW - 86_400_000) });
  assert.deepEqual(apiQuery({ queue: "orders", refresh: "15" }, { kind: "all" }, NOW), { queue: "orders" });
  const fixed = { from_ms: "100", to_ms: "200", limit: "10" };
  assert.deepEqual(apiQuery(fixed, ok(fixed), NOW), { limit: "10", from_ms: "100", to_ms: "200" });
});

test("a pinned link names absolute times, so it shows the same rows later", () => {
  const params = { queue: "orders", range: "1h", refresh: "15" };
  // range=1h becomes the hour that ended when the rows were fetched; refresh goes, a fixed window has nothing new
  assert.deepEqual(pinnedQuery(params, ok(params), NOW), { queue: "orders", from_ms: String(NOW - 3_600_000), to_ms: String(NOW) });
  // all history is pinned at its end: what has happened up to now
  assert.deepEqual(pinnedQuery({ reason: "received_more_than_once" }, { kind: "all" }, NOW), { reason: "received_more_than_once", to_ms: String(NOW) });
  // a link that was already pinned stays the same
  const pinned = { from_ms: "100", to_ms: "200", cursor: "abc" };
  assert.deepEqual(pinnedQuery(pinned, ok(pinned), NOW), pinned);
  // Stuck's API has only from_ms: that much is pinned
  assert.deepEqual(pinnedQuery({ older_than_s: "300", range: "24h" }, ok({ range: "24h" }), NOW, false), { older_than_s: "300", from_ms: String(NOW - 86_400_000) });
});

test("the filter form: a custom range becomes from_ms/to_ms in UTC, the whole last minute included", () => {
  assert.deepEqual(formQuery([["queue", "orders"], ["range", "1h"], ["limit", ""]]), { queue: "orders", range: "1h" });
  assert.deepEqual(formQuery([["queue", ""], ["range", ""]]), {});
  assert.deepEqual(formQuery([["range", "custom"], ["from", "2026-11-23T10:00"], ["to", "2026-11-23T11:30"]]),
    { from_ms: String(Date.UTC(2026, 10, 23, 10, 0)), to_ms: String(Date.UTC(2026, 10, 23, 11, 30) + 59_999) });
  // an open end: only a start (Stuck has no To field)
  assert.deepEqual(formQuery([["range", "custom"], ["from", "2026-11-23T10:00"]]), { from_ms: String(Date.UTC(2026, 10, 23, 10, 0)) });
  // a custom range with neither end is all history
  assert.deepEqual(formQuery([["range", "custom"], ["from", ""], ["to", ""]]), {});
});

test("datetime-local values are read and written in UTC, and round-trip", () => {
  assert.equal(toInputUtc(Date.UTC(2026, 10, 23, 9, 5)), "2026-11-23T09:05");
  assert.equal(fromInputUtc("2026-11-23T09:05"), Date.UTC(2026, 10, 23, 9, 5));
  assert.equal(fromInputUtc("2026-11-23T09:05:30"), Date.UTC(2026, 10, 23, 9, 5, 30));
  assert.equal(fromInputUtc(""), undefined);
  assert.equal(fromInputUtc("not a time"), undefined);
  const t = Date.UTC(2026, 0, 2, 3, 4);
  assert.equal(fromInputUtc(toInputUtc(t)), t);
});

test("the range in a sentence, and refresh=15", () => {
  assert.equal(rangeText({ kind: "all" }), "at any time");
  assert.equal(rangeText({ kind: "preset", preset: "24h" }), "in the last 24 hours");
  assert.equal(rangeText({ kind: "custom", from_ms: Date.UTC(2026, 10, 23, 10), to_ms: Date.UTC(2026, 10, 23, 11) }), "between 2026-11-23 10:00 UTC and 2026-11-23 11:00 UTC");
  assert.equal(rangeText({ kind: "custom", from_ms: Date.UTC(2026, 10, 23, 10) }), "since 2026-11-23 10:00 UTC");
  assert.equal(refreshOn({ refresh: "15" }), true);
  assert.equal(refreshOn({}), false);
  assert.equal(refreshOn({ refresh: "1" }), false);
});

const list = (params: Record<string, string>, opt: { what: "last seen" | "first seen" | "cause seen"; upper: boolean } = { what: "cause seen", upper: true }) =>
  renderToStaticMarkup(createElement(Filters, { action: "/stuck", values: {}, fields: [], refresh: false, range: { r: ok(params), opt } }));

test("the filters draw the range control from the URL", () => {
  const preset = list({ range: "7d" });
  assert.match(preset, /<select name="range"[^>]*><option value="">any time<\/option><option value="1h">last hour<\/option><option value="24h">last 24 hours<\/option><option value="7d" selected="">last 7 days<\/option>/);
  assert.doesNotMatch(preset, /name="from"/);
  assert.match(preset, /Cause seen/);
  // a fixed window opens as custom, its times shown in UTC
  const fixed = list({ from_ms: String(Date.UTC(2026, 10, 23, 10)), to_ms: String(Date.UTC(2026, 10, 23, 11)) });
  assert.match(fixed, /<option value="custom" selected="">custom<\/option>/);
  assert.match(fixed, /name="from" value="2026-11-23T10:00"/);
  assert.match(fixed, /name="to" value="2026-11-23T11:00"/);
});

test("a range with a start and no end", () => {
  const stuck = list({ from_ms: String(Date.UTC(2026, 10, 23, 10)) }, { what: "first seen", upper: false });
  assert.match(stuck, /First seen/);
  assert.match(stuck, /name="from"/);
  assert.doesNotMatch(stuck, /name="to"/);
});

test("a range the API would refuse is said instead of asking", () => {
  assert.deepEqual(readRange({ range: "2h" }), { error: "range must be 1h, 24h or 7d, not “2h”." });
});

test("Copy link is a button", () => {
  assert.match(renderToStaticMarkup(createElement(CopyLink, { link: () => "/stuck" })), /<button type="button"[^>]*>Copy link<\/button>/);
});
