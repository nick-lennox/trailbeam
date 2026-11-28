/*
  QV-591: the shared dashboard measures (src/dashboards/measures.ts, the hosted app's lib/dashboards/measures.ts moved
  here) on every case in ../contract/fixtures/measures-parity.json, which the server's engine
  (server/src/trailbeam/server/measures.py) writes and runs. The hosted app runs the same file as
  @trailbeam/ui/fixtures/measures-parity.json. The cases are a workspace's own data; the sample figures are the hosted
  app's, and the last tests here check only that they are what a host passes in.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dashboardMeasures, fmt, p95Of, STARTERS, type DashboardDef, type EventData, type Unit, type Variable, type Widget } from "../src/dashboards/index.ts";
import { queryLanguage, preset, type TimeRange } from "../src/model/index.ts";

type C = Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
const f = JSON.parse(readFileSync(new URL("../../contract/fixtures/measures-parity.json", import.meta.url), "utf8")) as Record<string, C[]> & { datasets: Record<string, EventData>; groupFields: unknown; numberFields: unknown };
const E = dashboardMeasures();
const { parse } = queryLanguage();
const data = (name: string) => f.datasets[name];
const m = (id: string) => E.measure(id)!;
/* as the fixture has it: JSON, with Infinity and NaN as strings, and an absent field the same as undefined */
const plain = (v: unknown) => (v === undefined ? null : JSON.parse(JSON.stringify(v, (_k, x) => (typeof x === "number" && !Number.isFinite(x) ? String(x) : x))));
const num = (v: number | string) => (typeof v === "string" ? Number(v) : v);
const each = (section: string, run: (c: C) => unknown, label = (c: C, i: number) => c.name ?? `${section} ${i + 1}`) => {
  test(`${section}: the shared cases are there`, () => assert.ok(f[section]?.length > 0));
  f[section].forEach((c, i) => test(`${section}: ${label(c, i)}`, () => assert.deepEqual(plain(run(c)), c.expected)));
};

test("the measures are the same", () => {
  assert.deepEqual(E.MEASURES.map((x) => ({ id: x.id, label: x.label, col: x.col ?? null, ask: x.ask, unit: x.unit, agg: x.agg, queue: !!x.now, spans: !!x.spans, fold: x.ev?.fold ?? null })), f.measures);
  assert.deepEqual(E.GROUP_FIELDS, f.groupFields);
  assert.deepEqual(E.NUMBER_FIELDS, f.numberFields);
});

each("series", (c) => E.seriesFor(m(c.measure), c.filter, c.groupBy ?? undefined, c.time as TimeRange, c.n, data(c.data)));
each("queueValue", (c) => E.queueValue(m(c.measure), c.queue, data(c.data), c.time as TimeRange), (c) => `${c.measure} on ${c.queue}, ${c.time.preset}`);
each("depthMissing", (c) => E.depthMissing(m(c.measure), data(c.data)), (c) => `${c.measure} on ${c.data}`);
each("noHistory", (c) => E.noHistory(m(c.measure), data(c.data), c.time as TimeRange), (c) => `${c.measure} on ${c.data}, ${c.time.preset}`);
each("noDepth", (c) => E.noDepth(data(c.data)), (c) => c.data);
each("queueTone", (c) => E.queueTone(c.queue, data(c.data)), (c) => c.queue);
each("spanQueues", (c) => E.spanQueues(c.def as DashboardDef, c.names));
each("queuesFor", (c) => E.queuesFor(parse(c.filter), c.names), (c) => JSON.stringify(c.filter));
each("queueOnlyTerms", (c) => E.queueOnlyTerms(c.filter), (c) => JSON.stringify(c.filter));
each("effectiveFilter", (c) => E.effectiveFilter(c.widget as Widget, c.variables as Variable[]));
each("parse", (c) => parse(c.query), (c) => JSON.stringify(c.query));
each("fmt", (c) => fmt(num(c.value), c.unit as Unit), (c) => `${c.value} ${c.unit}`);
each("p95Of", (c) => p95Of(c.values), (c) => JSON.stringify(c.values).slice(0, 40));
each("eventBuckets", (c) => E.eventBuckets(c.filter, c.time as TimeRange, c.n, data(c.data)), (c) => `${JSON.stringify(c.filter)} in ${c.n}`);
each("eventsFor", (c) => E.eventsFor(c.filter, c.time as TimeRange, data(c.data)).map((e) => e.key), (c) => JSON.stringify(c.filter));
each("alertOf", (c) => E.alertOf(c.widget as Widget, c.variables as Variable[], data(c.data)));
each("problem", (c) => E.problem(c.widget));
each("checkDef", (c) => E.checkDef(c.def));

/* The hosted app's sample data: the figures come from the host, and without any a queue measure is 0. */
test("sample figures are the host's", () => {
  const sample: EventData = { live: false, events: [], now: 3_600_000, queues: ["a", "b"], capped: false };
  const time = preset("1h", sample.now);
  const visible = (e: ReturnType<typeof dashboardMeasures>) => e.seriesFor(e.measure("messages_visible")!, "", undefined, time, 4, sample)[0].now;
  assert.equal(visible(E), 0);
  const hosted = dashboardMeasures({ sample: { now: { messages_visible: (q) => (q === "a" ? 5 : 7) }, tone: (q) => (q === "a" ? "critical" : "good") } });
  assert.equal(visible(hosted), 12);
  assert.equal(hosted.queueTone("a", sample), "critical");
  assert.equal(E.queueTone("a", sample), "good");
});

test("every starter passes checkDef", () => {
  for (const s of STARTERS) assert.ok("def" in E.checkDef(s.make()), s.id);
});
