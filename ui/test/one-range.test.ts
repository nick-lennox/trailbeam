/*
  QV-711: one time range per page, in the time picker's ids and names. The Event explorer's chart sets the page's range
  rather than keeping a window of its own, and its hover note is the design system's Tooltip; the queue page's range
  control is the time picker. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { EventChart } from "../src/explorer/index.ts";
import { custom, preset } from "../src/model/index.ts";
import { queueRangeOfTime, queueTime } from "../shell/queue.tsx";

const NOW = Date.UTC(2026, 10, 27, 12, 0, 0);
const MIN = 60_000;

test("the chart keeps no range of its own: no from-to chip, no clear button, no hand-built hover note", () => {
  const html = renderToStaticMarkup(h(EventChart, {
    buckets: [{ ok: 2, err: 1 }, { ok: 0, err: 0 }, { ok: 4, err: 0 }], from: NOW - 3 * MIN, to: NOW, span: MIN, per: "minute", onPick: () => {},
  }));
  assert.match(html, /Events per minute/);
  assert.doesNotMatch(html, /Show the whole range again/);
  assert.doesNotMatch(html, /role="tooltip"/, "the note is drawn by Tooltip, on hover only");
  assert.doesNotMatch(html, /aria-pressed/);
  assert.match(html, /Select a bar to show its time range/);
  const src = readFileSync(new URL("../src/explorer/EventChart.tsx", import.meta.url), "utf8");
  assert.match(src, /import \{ Tooltip \} from "\.\.\/design\/Tooltip\.tsx"/);
  assert.doesNotMatch(src, /role="tooltip"/);
  const explorer = readFileSync(new URL("../src/explorer/EventExplorer.tsx", import.meta.url), "utf8");
  assert.doesNotMatch(explorer, /setWin|\bwin\b \?/, "the explorer narrows nothing beside the page's range");
  assert.match(explorer, /setTime\(custom\(w\[0\], w\[1\]\)\)/);
});

test("the queue page's range is the time picker's: its presets end now, anything else is a fixed from and to", () => {
  assert.deepEqual(queueTime({ kind: "preset", preset: "4h" }, NOW), preset("4h", NOW));
  assert.deepEqual(queueTime({ kind: "fixed", fromMs: NOW - 60 * MIN, toMs: NOW }), custom(NOW - 60 * MIN, NOW));
  assert.deepEqual(queueRangeOfTime(preset("7d", NOW)), { kind: "preset", preset: "7d" });
  assert.deepEqual(queueRangeOfTime(custom(NOW - 60 * MIN, NOW)), { kind: "fixed", fromMs: NOW - 60 * MIN, toMs: NOW });
  // a typed 45m is not one of the page's presets: it is the 45 minutes it covers now
  assert.deepEqual(queueRangeOfTime({ from: NOW - 45 * MIN, to: NOW, label: "Past 45 minutes", preset: "45m" }), { kind: "fixed", fromMs: NOW - 45 * MIN, toMs: NOW });
  const src = readFileSync(new URL("../shell/queue.tsx", import.meta.url), "utf8");
  assert.doesNotMatch(src, /<select|not kept/, "no native select of presets");
});

test("every open-source page with a range names it range= (or from=&to=), never since_s=", () => {
  for (const page of ["events", "tenants", "queue", "overview", "messages"]) {
    const src = readFileSync(new URL(`../shell/${page}.tsx`, import.meta.url), "utf8");
    assert.match(src, /TimePicker/, `${page} uses the time picker`);
    assert.doesNotMatch(src, /["`]\/(tenants|events|queue|overview)?\?since_s=/, `${page} writes no since_s link`);
  }
});
