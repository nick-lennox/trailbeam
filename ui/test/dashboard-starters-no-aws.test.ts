/*
  QV-772: on a server that does not read AWS (self-hosted Trailbeam's default), a starter is built from what the spans
  record and says once, at the top, how to turn AWS reads on, so it never opens as boxes saying "No data". Drawn on
  the measures parity fixture's own-data events with every queue's AWS reads off. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement as h, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { AWS_OFF_NOTE, dashboardMeasures, makeStarter, STARTERS, TABLE_COLUMNS, type EventData } from "../src/dashboards/index.ts";
import { DashboardsEnvProvider, DashboardsPage, type DashboardActions } from "../src/dashboards/views.ts";
import { WidgetBody } from "../src/dashboards/widgets.tsx";
import { spanOnlyLive } from "../src/overview/flows-data.ts";
import { preset, queryLanguage } from "../src/model/index.ts";

const f = JSON.parse(readFileSync(new URL("../../contract/fixtures/measures-parity.json", import.meta.url), "utf8"));
const own: EventData = f.datasets.own;
/* the same events on a server with AWS reads off: every queue not_configured */
const data: EventData = { ...own, queueLive: Object.fromEntries(own.queues.map((q) => [q, spanOnlyLive({ view: null, fifo: q.endsWith(".fifo"), dlq: null, stuck: 0 })])) };
const E = dashboardMeasures();
const lang = queryLanguage();
const ctx = { vars: [], time: preset("1h", data.now), setTime: () => {}, ws: (p = "") => `/ws${p}`, setVar: () => {}, data, E, lang };
const drawn = (def: ReturnType<typeof makeStarter>) => def!.widgets.map((w) => ({ w, html: renderToStaticMarkup(h(WidgetBody, { w, ctx: { ...ctx, vars: def!.variables } })) }));

test("without AWS reads every starter's widgets have data, and one note says how to turn the reads on", () => {
  for (const s of STARTERS) {
    const def = makeStarter(s.id, { aws: false })!;
    assert.ok("def" in E.checkDef(def), s.id);
    // a variable starts at every queue: a hosted sample queue's name would match nothing on this server
    for (const v of def.variables) assert.equal(v.value, "*", `${s.id}: ${v.name}`);
    const notes = def.widgets.filter((w) => w.type === "note");
    assert.equal(notes.length, 1, s.id);
    assert.equal(notes[0].text, AWS_OFF_NOTE);
    assert.deepEqual(notes[0].layout, { x: 0, y: 0, w: 12, h: 2 }, `${s.id}: the note is at the top`);
    for (const w of def.widgets) {
      const ids = w.type === "queue_table" ? (w.columns?.length ? w.columns : TABLE_COLUMNS) : (w.queries ?? []).map((q) => q.measure);
      for (const id of ids) assert.ok(!E.measure(id)!.now, `${s.id}: ${w.title} reads ${id}, which comes from AWS`);
      assert.notEqual(w.type, "status_grid", s.id);
      assert.notEqual(w.type, "gauge", s.id);
    }
    for (const { w, html } of drawn(def)) {
      assert.doesNotMatch(html, /No data|AWS is not read/, `${s.id}: ${w.title}`);
      assert.ok(html.length > 0, `${s.id}: ${w.title}`);
    }
  }
});

test("the hosted starters are unchanged, and are the ones whose queue widgets have no data without AWS", () => {
  for (const s of STARTERS) {
    assert.deepEqual(stripIds(makeStarter(s.id)!), stripIds(s.make()), s.id);
    assert.deepEqual(stripIds(makeStarter(s.id, { aws: true })!), stripIds(s.make()), s.id);
  }
  assert.ok(drawn(makeStarter("queue-health")).some(({ html }) => /AWS is not read on this server/.test(html)));
  assert.equal(makeStarter("nope", { aws: false }), undefined);
});

test("the list previews the starters this server can fill", () => {
  const refused = async () => ({ ok: false as const, error: "Not in this test." });
  const actions: DashboardActions = { create: refused, save: refused, star: refused, lock: refused, duplicate: refused, remove: refused };
  const page = (aws?: boolean) => renderToStaticMarkup(h(DashboardsEnvProvider, { value: { engine: E, lang, data, href: (p = "") => `/ws${p}`, actions, aws }, children: h(DashboardsPage, { dashboards: [] }) as ReactNode }));
  assert.notEqual(page(false), page(undefined));
  assert.equal(page(true), page(undefined));
});

function stripIds(d: NonNullable<ReturnType<typeof makeStarter>>) {
  const secs = new Map(d.sections.map((s, i) => [s.id, i]));
  return { ...d, sections: d.sections.map(({ id: _, ...s }) => s), widgets: d.widgets.map(({ id: _, section, ...w }) => ({ ...w, section: section === undefined ? undefined : secs.get(section) })) };
}
