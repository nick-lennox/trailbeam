/*
  QV-591 part 2: the dashboard views in @trailbeam/ui/dashboards/views (the list and one dashboard), moved from the
  hosted app's components/dashboards so both products draw the same dashboards. Drawn here on the measures parity
  fixture's own-data events (../contract/fixtures/measures-parity.json) with each starter. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement as h, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { dashboardMeasures, STARTERS, type Dashboard, type EventData } from "../src/dashboards/index.ts";
import { DashboardsEnvProvider, DashboardsPage, DashboardView, Mini, type DashboardActions, type DashboardsEnv } from "../src/dashboards/views.ts";
import { queryLanguage } from "../src/model/index.ts";

const f = JSON.parse(readFileSync(new URL("../../contract/fixtures/measures-parity.json", import.meta.url), "utf8"));
const data: EventData = f.datasets.own;
const refused = async () => ({ ok: false as const, error: "Not in this test." });
const actions: DashboardActions = { create: refused, save: refused, star: refused, lock: refused, duplicate: refused, remove: refused };
const env = (over: Partial<DashboardsEnv> = {}): DashboardsEnv => ({
  engine: dashboardMeasures(), lang: queryLanguage(), data, href: (p = "") => `/ws${p}`, actions, ...over,
});
const draw = (el: ReactNode, over: Partial<DashboardsEnv> = {}) => renderToStaticMarkup(h(DashboardsEnvProvider, { value: env(over), children: el }));
const board = (i: number, over: Partial<Dashboard> = {}): Dashboard => ({
  id: `d${i}`, name: STARTERS[i].name, def: STARTERS[i].make(), starred: i === 0, lockedBy: null, lockedById: null,
  createdBy: "Ada", updatedBy: "Ada", updatedAt: data.now, ...over,
});

test("the views need the product's environment", () => {
  assert.throws(() => renderToStaticMarkup(h(DashboardsPage, { dashboards: [] })), /DashboardsEnvProvider/);
});

test("the list links each dashboard into the product and offers every starter", () => {
  const html = draw(h(DashboardsPage, { dashboards: STARTERS.map((_, i) => board(i)) }));
  for (const [i, s] of STARTERS.entries()) {
    assert.ok(html.includes(`href="/ws/dashboards/d${i}"`), s.id);
    assert.ok(html.includes(s.name), s.name);
  }
  assert.match(html, /aria-label="Remove the star"/);
});

test("the list shows the server's alerts and when it last checked", () => {
  const d = board(0);
  const w = d.def.widgets.find((x) => x.rule)!;
  const html = draw(h(DashboardsPage, {
    dashboards: [d],
    server: { states: [{ dashboardId: d.id, widgetId: w.id, title: w.title, tone: "critical", value: "12", limit: "above 0", queue: "orders", since: data.now }], error: null, check: { text: "Last checked 12:00 UTC.", late: false } },
  }));
  assert.ok(html.includes(w.title));
  assert.ok(html.includes("Last checked 12:00 UTC."));
  assert.ok(html.includes("orders"));
});

test("each starter draws, with its sections, and a locked one has no editing", () => {
  for (const [i, s] of STARTERS.entries()) {
    const def = s.make();
    const html = draw(h(DashboardView, { dashboard: board(i), range: { range: "1h" } }));
    assert.ok(html.includes(s.name), s.id);
    for (const sec of def.sections) assert.ok(html.includes(sec.title), `${s.id}: ${sec.title}`);
    assert.match(html, /Add widget/);
    const locked = draw(h(DashboardView, { dashboard: board(i, { lockedBy: "Ada", lockedById: "u1" }) }));
    assert.match(locked, /Locked by Ada/);
    assert.doesNotMatch(locked, /Add widget/);
  }
});

test("a live-refresh slot is drawn only while the dashboard follows the present", () => {
  const slot = h("span", { className: "slot-live" });
  assert.match(draw(h(DashboardView, { dashboard: board(0), range: { range: "1h" } }), { liveRefresh: slot }), /slot-live/);
  assert.doesNotMatch(draw(h(DashboardView, { dashboard: board(0), range: { from: String(data.now - 3_600_000), to: String(data.now) } }), { liveRefresh: slot }), /slot-live/);
});

test("Mini draws a cell per widget", () => {
  const def = STARTERS[2].make();
  assert.ok(renderToStaticMarkup(h(Mini, { def })).length > 0);
});

test("every widget type draws on own data, and a queue flow points at the Overview", async () => {
  const { WidgetBody } = await import("../src/dashboards/widgets.tsx");
  const { blank, WIDGET_TYPES } = await import("../src/dashboards/types.ts");
  const { preset } = await import("../src/model/index.ts");
  const ctx = { vars: [], time: preset("1h", data.now), setTime: () => {}, ws: (p = "") => `/ws${p}`, setVar: () => {}, data, E: dashboardMeasures(), lang: queryLanguage() };
  for (const t of WIDGET_TYPES) {
    const html = renderToStaticMarkup(h(WidgetBody, { w: blank(t, { x: 0, y: 0, w: 6, h: 4 }), ctx }));
    assert.ok(html.length > 0, t);
  }
  assert.match(renderToStaticMarkup(h(WidgetBody, { w: blank("queue_flow", { x: 0, y: 0, w: 6, h: 4 }), ctx })), /href="\/ws"/);
  const list = renderToStaticMarkup(h(WidgetBody, { w: { ...blank("event_list", { x: 0, y: 0, w: 8, h: 4 }), filter: "" }, ctx }));
  assert.match(list, /<table/);
  const queues = renderToStaticMarkup(h(WidgetBody, { w: blank("queue_table", { x: 0, y: 0, w: 8, h: 3 }), ctx }));
  for (const q of data.queues) assert.ok(queues.includes(`href="/ws/messages?queue=${q}"`), q);
});

test("on sample data the queue flow draws the host's sample figures", async () => {
  const { WidgetBody } = await import("../src/dashboards/widgets.tsx");
  const { blank } = await import("../src/dashboards/types.ts");
  const { preset } = await import("../src/model/index.ts");
  const sample: EventData = { live: false, events: [], now: data.now, queues: ["orders"], capped: false };
  const ctx = { vars: [], time: preset("1h", sample.now), setTime: () => {}, ws: (p = "") => p, setVar: () => {}, data: sample, E: dashboardMeasures(), lang: queryLanguage(),
    sampleFlow: () => ({ producers: [{ from: "checkout", perMin: 40 }], consumer: "fulfil", p95Ms: 900, dlq: "orders-dlq", dlqDepth: 3, sentPerMin: 40, visible: 12, inFlight: 2 }) };
  /* the drawing needs a size, which a server render has not got: the widget is there and has no "no queue" note */
  const html = renderToStaticMarkup(h(WidgetBody, { w: blank("queue_flow", { x: 0, y: 0, w: 8, h: 4 }), ctx }));
  assert.doesNotMatch(html, /No queue matched/);
  assert.match(renderToStaticMarkup(h(WidgetBody, { w: blank("queue_flow", { x: 0, y: 0, w: 8, h: 4 }), ctx: { ...ctx, sampleFlow: undefined } })), /No queue matched/);
});
