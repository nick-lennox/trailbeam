"use client";

import { useMemo, type ReactNode } from "react";
import { custom, preset, withRange, type TimeRange } from "../model/timerange.ts";
import type { QueryLanguage } from "../model/query.ts";
import { LEVEL_COLOR, TD, TH, colLabel, level } from "../explorer/columns.ts";
import { eventCell } from "../explorer/parts.tsx";
import { Bars, CAT, Heatmap, OTHER, Spark, TimeSeries, useSize, type Line } from "../charts/index.ts";
import { UiLink, useNav } from "../design/nav.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { effectiveFilter, fmt, noDepth, queuesFor, toneOf, TONE_WORD, type DashboardMeasures, type EventData, type Measure, type Tone } from "./measures.ts";
import { EVENT_COLUMNS, TABLE_COLUMNS, type Variable, type Widget } from "./types.ts";
import type { SampleFlow } from "./env.tsx";
import { StateMark, TONE_TEXT, TONE_VAR } from "./ui.tsx";

/* What every widget is given: the dashboard's variables and time, and how to change them. */
export type Ctx = {
  vars: Variable[]; time: TimeRange; setTime: (t: TimeRange) => void; ws: (path?: string) => string; setVar: (field: string, value: string) => void; data: EventData;
  /* the host's measures engine and query language (DashboardsEnv) */
  E: DashboardMeasures; lang: QueryLanguage; bodyPaths?: string[];
  /* the hosted app's sample data: a sample queue's flow */
  sampleFlow?: (queue: string) => SampleFlow | undefined;
};

const FIXED: Record<string, string> = { sent: "var(--color-series-2)", received: "var(--color-series-1)", deleted: "var(--color-series-3)" };
const N = 60;
export const timeOf = (w: Widget, ctx: Ctx) => (w.time ? preset(w.time, ctx.data.now) : ctx.time);
/* The widget's events in the explorer, over the widget's time range: its own, else the dashboard's (QV-365). */
export const exploreHref = (w: Widget, ctx: Ctx) => withRange(ctx.ws(`/events?q=${encodeURIComponent(effectiveFilter(w, ctx.vars))}`), timeOf(w, ctx));

const Empty = ({ children }: { children: ReactNode }) => <div className="grid flex-1 place-content-center px-3 text-center text-[13px] text-ink-3">{children}</div>;
/* A queue measure on a workspace's own data with no figure: short, with the reason on hover. */
function NoDepth({ data }: { data: EventData }) {
  const qs = Object.values(data.queueLive ?? {});
  const short = qs.some((q) => q.status === "ok") ? "Not reported for these queues" : qs.some((q) => q.status === "failed") ? "Reading AWS failed" : qs.some((q) => q.status === "not_configured") ? "AWS is not read on this server" : "Connect the queue's AWS account";
  return (
    <div className="grid flex-1 place-content-center px-3 text-center">
      <Tooltip content={noDepth(data)} className="grid" tabIndex={0}>
        <span className="text-[13px] text-ink-2">No data</span>
        <span className="mt-0.5 text-xs text-ink-3">{short}</span>
      </Tooltip>
    </div>
  );
}

/* Queue measures come from CloudWatch, which counts per queue. A filter term on anything else (a tenant,
   a producer) can't narrow them, and the widget says so instead of ignoring it quietly. */
function QueueOnly({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const queueish = w.type === "status_grid" || (w.type === "queue_table" ? (w.columns?.length ? w.columns : TABLE_COLUMNS) : (w.queries ?? []).map((q) => q.measure)).some((id) => !!ctx.E.measure(id)?.now);
  if (!queueish) return null;
  const terms = ctx.E.queueOnlyTerms(effectiveFilter(w, ctx.vars));
  if (!terms.length) return null;
  return (
    <p className="mt-1 shrink-0 truncate text-xs text-ink-3">
      <Tooltip content="CloudWatch's SQS metrics and a queue's settings are per queue. Only queue: terms apply to them." tabIndex={0}>
        Queue measures are per queue: {terms.map((t, i) => <span key={t}>{i ? ", " : ""}<code className="font-mono">{t}</code></span>)} {terms.length === 1 ? "does" : "do"} not apply.
      </Tooltip>
    </p>
  );
}
/* a measure on own data worked out from the spans as they are now, without history */
const NoHistory = ({ m }: { m: Measure }) => <Empty>{m.label} is worked out from your spans as they are now, so it has no history to draw. Show it as a number.</Empty>;

function TimeSeriesBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const router = useNav();
  const time = timeOf(w, ctx);
  const filter = effectiveFilter(w, ctx.vars);
  /* queue measures have no data on a workspace's own spans yet; the rest are drawn */
  const all = w.queries ?? [];
  const qs = all.filter((q) => { const m = ctx.E.measure(q.measure); return !m || (!ctx.E.depthMissing(m, ctx.data) && !ctx.E.noHistory(m, ctx.data, time)); });
  const first = ctx.E.measure(qs[0]?.measure ?? "");
  const kind = w.style ?? "line";
  const lines = useMemo<Line[]>(() => {
    if (!first) return [];
    if (kind === "bars" && first.id === "events") {
      const b = ctx.E.eventBuckets(filter, time, N, ctx.data);
      return [{ name: "OK", color: "var(--color-line-strong)", points: b.map((x) => x.ok) }, { name: "Error", color: "var(--color-critical)", points: b.map((x) => x.err) }];
    }
    if (qs.length === 1 && qs[0].group_by) {
      const all = ctx.E.seriesFor(first, filter, qs[0].group_by, time, N, ctx.data);
      const top = all.slice(0, 5).map((s, i) => ({ name: s.name, color: CAT[i], points: s.points }));
      const rest = all.slice(5);
      if (rest.length) top.push({ name: `Other (${rest.length})`, color: OTHER, points: Array.from({ length: N }, (_, i) => (first.agg === "sum" ? rest.reduce((a, s) => a + s.points[i], 0) : Math.max(...rest.map((s) => s.points[i])))) });
      return top;
    }
    return qs.flatMap((q, k) => {
      const m = ctx.E.measure(q.measure);
      if (!m) return [];
      const s = ctx.E.seriesFor(m, filter, undefined, time, N, ctx.data)[0];
      if (!s) return [];
      const stackedPair = kind === "stacked";
      return [{ name: m.label, color: FIXED[m.id] ?? (stackedPair ? CAT[0] : CAT[k % CAT.length]), points: s.points, hatch: stackedPair && m.id === "messages_in_flight" }];
    });
  }, [filter, time.from, time.to, JSON.stringify(qs), kind, ctx.data]);
  const flat = all.map((q) => ctx.E.measure(q.measure)).find((m) => m && ctx.E.noHistory(m, ctx.data, time) && !ctx.E.depthMissing(m, ctx.data));
  if (!first) return (flat ? <NoHistory m={flat} /> : all.length ? <NoDepth data={ctx.data} /> : <Empty>Pick a measure for this widget.</Empty>);
  if (!lines.length) return <Empty>Nothing matched this filter.</Empty>;
  return (
    <TimeSeries
      lines={lines}
      from={time.from}
      to={time.to}
      unit={first.unit}
      kind={kind}
      marker={w.marker}
      onZoom={w.time ? undefined : (a, b) => ctx.setTime(custom(a, b))}
      onPick={() => router.push(exploreHref(w, ctx))}
    />
  );
}

/* One figure, sized to its widget, over a line of how it got there. */
function NumberBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const m = ctx.E.measure(w.queries?.[0]?.measure ?? "");
  if (!m) return <Empty>Pick a measure for this widget.</Empty>;
  if (ctx.E.depthMissing(m, ctx.data)) return <NoDepth data={ctx.data} />;
  const s = ctx.E.seriesFor(m, effectiveFilter(w, ctx.vars), undefined, timeOf(w, ctx), 40, ctx.data)[0];
  const spark = !ctx.E.noHistory(m, ctx.data, timeOf(w, ctx));
  if (!s) return <Empty>Nothing matched this filter.</Empty>;
  const tone = toneOf(s.now, w.rule);
  const alerting = tone && tone !== "good";
  return (
    <div className="flex min-h-0 flex-1 flex-col" style={{ containerType: "size" }}>
      <div className="flex min-h-0 flex-1 flex-col justify-center">
        <div className={`font-mono leading-none font-medium tracking-tight whitespace-nowrap ${alerting ? TONE_TEXT[tone] : "text-ink"}`} style={{ fontSize: "clamp(26px, min(34cqh, 17cqw), 68px)" }}>{fmt(s.now, m.unit)}</div>
        {alerting && <div className="mt-2"><StateMark tone={tone}>{TONE_WORD[tone]}</StateMark></div>}
      </div>
      {spark && <Spark points={s.points} color={alerting ? TONE_VAR[tone] : CAT[0]} className="mr-1 mb-0.5 h-[30%] min-h-6" />}
    </div>
  );
}

/* Each measure for the queue where it is highest: the measure, the queue, the figure. Nothing is drawn that the definition does not name. */
function GaugeBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const filter = effectiveFilter(w, ctx.vars);
  const rows = (w.queries ?? []).flatMap((q) => {
    const m = ctx.E.measure(q.measure);
    const worst = m && ctx.E.seriesFor(m, filter, "queue", timeOf(w, ctx), 2, ctx.data)[0];
    return m && worst ? [{ m, queue: worst.name, v: worst.now }] : [];
  });
  if (!rows.length) return (w.queries ?? []).some((q) => { const m = ctx.E.measure(q.measure); return m && ctx.E.depthMissing(m, ctx.data); }) ? <NoDepth data={ctx.data} /> : <Empty>No queue matched this filter.</Empty>;
  return (
    <div className="min-h-0 min-w-0 flex-1 overflow-y-auto overflow-x-hidden">
      {rows.map(({ m, queue, v }) => {
        const tone = toneOf(v, w.rule);
        return (
          <div key={m.id} className="flex h-[34px] w-full min-w-0 items-center gap-2.5 border-b border-line text-[13px] last:border-b-0">
            <span className="min-w-0 shrink truncate text-ink">{m.label}</span>
            <span className="min-w-0 flex-1 shrink-[3] truncate font-mono text-caption text-ink-3">{queue}</span>
            <b className={`shrink-0 font-mono font-medium whitespace-nowrap ${tone && tone !== "good" ? TONE_TEXT[tone] : "text-ink"}`}>{fmt(v, m.unit)}</b>
          </div>
        );
      })}
    </div>
  );
}

/* Ranked rows. Each row's rule is drawn as far as its value reaches, so the list is also the chart. */
function TopListBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const q = w.queries?.[0];
  const m = ctx.E.measure(q?.measure ?? "");
  if (!m || !q) return <Empty>Pick a measure for this widget.</Empty>;
  if (ctx.E.depthMissing(m, ctx.data)) return <NoDepth data={ctx.data} />;
  const by = q.group_by ?? "queue";
  const rows = ctx.E.seriesFor(m, effectiveFilter(w, ctx.vars), by, timeOf(w, ctx), 2, ctx.data).slice(0, 10);
  const max = Math.max(0, ...rows.map((r) => (Number.isFinite(r.now) ? r.now : 0)));
  const canPick = ctx.vars.some((v) => v.field === by);
  if (!rows.length) return <Empty>Nothing matched this filter.</Empty>;
  return (
    <div className="min-h-0 flex-1 overflow-y-auto">
      {rows.map((r, i) => (
        <Tooltip key={r.name} content={canPick ? `Show only ${r.name} on this dashboard` : undefined} className="block">
        <button type="button" disabled={!canPick} onClick={() => ctx.setVar(by, r.name)} className="group relative flex h-8 w-full items-center gap-2.5 border-b border-line text-left font-mono text-caption text-ink">
          <span className="w-3.5 shrink-0 text-ink-3">{i + 1}</span>
          <span className={`min-w-0 flex-1 truncate ${canPick ? "group-hover:underline" : ""}`}>{r.name}</span>
          <b className="shrink-0 font-medium">{fmt(r.now, m.unit)}</b>
          {max > 0 && <span aria-hidden className="absolute -bottom-px left-0 h-0.5 bg-series-1" style={{ width: `${(100 * (Number.isFinite(r.now) ? r.now : max)) / max}%` }} />}
        </button>
        </Tooltip>
      ))}
    </div>
  );
}

/* A figure's recent shape, small enough to sit in a table cell. */
function Trend({ points }: { points: number[] }) {
  const max = Math.max(0, ...points.filter(Number.isFinite)) || 1;
  return (
    <svg width="72" height="16" viewBox="0 0 72 16" aria-hidden className="mr-3 inline-block align-middle">
      <path d={points.map((v, i) => `${i ? "L" : "M"}${((72 * i) / (points.length - 1)).toFixed(1)},${(14 - 12 * (Math.min(v, max) / max)).toFixed(1)}`).join(" ")} fill="none" stroke="var(--color-series-1)" strokeWidth={1.25} strokeLinejoin="round" />
    </svg>
  );
}

function QueueTableBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const names = queuesFor(ctx.lang.parse(effectiveFilter(w, ctx.vars)), ctx.data.queues);
  const cols = (w.columns?.length ? w.columns : TABLE_COLUMNS).map((id) => ctx.E.measure(id)).filter((m): m is Measure => !!m);
  const time = timeOf(w, ctx);
  /* a queue's figure: a queue measure's value now, or an event measure over the time range */
  const valueOf = (m: Measure, q: string) => (m.now ? ctx.E.queueValue(m, q, ctx.data, time) : ctx.E.seriesFor(m, `queue:"${q}"`, undefined, time, 2, ctx.data)[0]?.now ?? 0);
  if (!names.length) return <Empty>No queue matched this filter.</Empty>;
  return (
    <div className="min-h-0 flex-1 overflow-auto">
      <table className="w-full border-collapse font-mono text-caption">
        <thead className="sticky top-0 bg-surface">
          <tr><th className={`${TH} pl-3`}>Queue</th>{cols.map((m) => <th key={m!.id} className={`${TH} text-right`} ><Tooltip content={m!.col ? m!.label : undefined}>{m!.col ?? m!.label}</Tooltip></th>)}</tr>
        </thead>
        <tbody>
          {names.map((q) => (
            <tr key={q} className="group">
              <td className={`${TD} pl-3 group-hover:bg-sunken`}>
                <UiLink href={ctx.ws(`/messages?queue=${q}`)} className="inline-flex items-center gap-2 text-ink hover:underline">
                  <span className="size-2 shrink-0 rounded-[2px]" style={{ background: toneVar(ctx.E.queueTone(q, ctx.data)) }} />{q}
                </UiLink>
              </td>
              {cols.map((m) => {
                const v = valueOf(m, q);
                const trend = w.trend && m === cols[0] && v !== null && !ctx.E.noHistory(m, ctx.data, time) ? ctx.E.seriesFor(m, `queue:"${q}"`, undefined, time, 24, ctx.data)[0]?.points : undefined;
                return (
                  <td key={m.id} className={`${TD} text-right text-ink tabular-nums group-hover:bg-sunken`}>
                    {trend && <Trend points={trend} />}
                    {v === null ? <Tooltip content={noDepth(ctx.data)} className="text-ink-3" tabIndex={0}>No data</Tooltip> : fmt(v, m.unit)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const quiet = (t: Tone | "none") => t === "good" || t === "none";
const word = (t: Tone | "none") => (t === "none" ? "No data" : TONE_WORD[t]);

/* One cell per queue, in the queue's own health color: the same health the sidebar shows. */
/* Every queue as a tile in its health color, sized to fill the widget. With room, a tile names its queue;
   with many queues the tiles shrink to squares and the ones that are not healthy are listed under them. */
function StatusGridBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const [ref, { w: bw, h: bh }] = useSize<HTMLDivElement>();
  const names = queuesFor(ctx.lang.parse(effectiveFilter(w, ctx.vars)), ctx.data.queues);
  /* on own data a queue AWS was not read for has no health: it is drawn as having none, not as healthy */
  const tones = names.map((q) => ({ q, tone: (ctx.E.queueTone(q, ctx.data) ?? (ctx.data.live ? "none" : "good")) as Tone | "none" }))
    .sort((a, b) => ["critical", "warning", "good", "none"].indexOf(a.tone) - ["critical", "warning", "good", "none"].indexOf(b.tone));
  if (ctx.data.live && tones.every((x) => x.tone === "none")) return <NoDepth data={ctx.data} />;
  if (!names.length) return <Empty>No queue matched this filter.</Empty>;
  const n = tones.length, GAPPX = 6;
  /* the fewest rows whose tiles are still wide enough to carry a name */
  let rows = 1;
  while (rows < n && (bw - GAPPX * (Math.ceil(n / rows) - 1)) / Math.ceil(n / rows) < 120) rows++;
  const cols = Math.ceil(n / rows);
  const tw = (bw - GAPPX * (cols - 1)) / cols, th = Math.min(140, (bh - GAPPX * (rows - 1)) / rows);
  const labeled = tw >= 96 && th >= 40;
  return (
    <div ref={ref} className="min-h-0 flex-1 overflow-hidden">
      {bw > 0 && labeled && (
        <div className="grid h-full content-start" style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))`, gap: GAPPX }}>
          {tones.map(({ q, tone }) => (
            <UiLink key={q} href={ctx.ws(`/messages?queue=${q}`)} className={`flex min-w-0 flex-col justify-between rounded-lg px-2.5 py-2 ${quiet(tone) ? "bg-sunken hover:bg-line" : "hover:opacity-90"}`} style={{ height: th, background: quiet(tone) ? undefined : TONE_VAR[tone as Tone] }}>
              <span className={`truncate font-mono text-caption ${quiet(tone) ? "text-ink" : "font-medium text-canvas"}`}>{q}</span>
              <span className={`text-xs ${quiet(tone) ? "text-ink-3" : "font-medium text-canvas"}`}>{word(tone)}</span>
            </UiLink>
          ))}
        </div>
      )}
      {bw > 0 && !labeled && (
        <div className="h-full overflow-y-auto">
          <div className="flex flex-wrap gap-1">
            {tones.map(({ q, tone }) => (
              <Tooltip key={q} content={`${q}, ${word(tone)}`} className="flex">
                <UiLink href={ctx.ws(`/messages?queue=${q}`)} aria-label={`${q}, ${word(tone)}`} className="size-6 rounded-[4px]" style={{ background: quiet(tone) ? "var(--color-sunken)" : TONE_VAR[tone as Tone] }} />
              </Tooltip>
            ))}
          </div>
          <div className="mt-2.5 grid gap-1 text-xs">
            {tones.filter((x) => !quiet(x.tone)).map(({ q, tone }) => (
              <div key={q} className="flex items-center gap-1.5">
                <span className="size-2 shrink-0 rounded-[2px]" style={{ background: TONE_VAR[tone as Tone] }} />
                <span className="min-w-0 flex-1 truncate font-mono text-ink">{q}</span>
                <span className="shrink-0 text-ink-2">{word(tone)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

const HEAT_ROWS: [number, string][] = [[30_000, "30s"], [10_000, "10s"], [3000, "3s"], [1000, "1s"], [300, "300ms"], [100, "100ms"], [30, "30ms"], [0, "10ms"]];
function HeatmapBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const time = timeOf(w, ctx);
  const filter = effectiveFilter(w, ctx.vars);
  const COLS = 40;
  const rows = ctx.E.eventsFor(filter, time, ctx.data);
  const cells = HEAT_ROWS.map(() => Array<number>(COLS).fill(0));
  let any = false;
  for (const e of rows) {
    const v = e.durationMs;
    if (v === undefined) continue;
    any = true;
    cells[HEAT_ROWS.findIndex(([lo]) => v >= lo)][Math.min(COLS - 1, Math.floor(((e.ms - time.from) / (time.to - time.from)) * COLS))]++;
  }
  if (!any) return <Empty>No processing times were recorded for this filter in this range.</Empty>;
  return <Heatmap cells={cells} labels={HEAT_ROWS.map(([, l]) => l)} from={time.from} to={time.to} />;
}

function DistributionBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const router = useNav();
  const f = ctx.lang.def(w.field ?? "receive_count");
  const filter = effectiveFilter(w, ctx.vars);
  if (!f) return <Empty>Pick a field for this widget.</Empty>;
  const counts = new Map<string, number>();
  const order = f.buckets ? ctx.lang.ranges(f) : null;
  for (const e of ctx.E.eventsFor(filter, timeOf(w, ctx), ctx.data)) {
    const v = f.get(e);
    if (v === undefined) continue;
    const key = order ? order.find((r) => (v as number) >= r.lo && (v as number) < r.hi)!.value : ctx.lang.token(f.name, v);
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  const keys = order ? order.map((r) => r.value).filter((k) => counts.has(k)) : [...counts.keys()].sort((a, b) => parseFloat(a) - parseFloat(b));
  if (!keys.length) return <Empty>Nothing matched this filter.</Empty>;
  return (
    <Bars
      bars={keys.map((k) => ({ label: order?.find((r) => r.value === k)?.label ?? k, value: counts.get(k)! }))}
      caption={f.label}
      onPick={(label) => { const k = order?.find((r) => r.label === label)?.value ?? label; router.push(ctx.ws(`/events?q=${encodeURIComponent(`${filter} ${f.name}:${k}`.trim())}`)); }}
    />
  );
}

function EventListBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const router = useNav();
  const cols = w.columns?.length ? w.columns : EVENT_COLUMNS;
  const rows = ctx.E.eventsFor(effectiveFilter(w, ctx.vars), timeOf(w, ctx), ctx.data);
  if (!rows.length) return <Empty>No events match this filter in this range.</Empty>;
  return (
    <div className="min-h-0 flex-1 overflow-auto">
      <table className="w-full border-collapse font-mono text-caption">
        <thead className="sticky top-0 z-[1] bg-surface"><tr>{cols.map((c) => <th key={c} className={TH}>{colLabel(c, ctx.lang)}</th>)}</tr></thead>
        <tbody>
          {rows.slice(0, 50).map((e) => (
            <tr key={e.key} className="ev-row group cursor-pointer" style={{ ["--bar" as string]: LEVEL_COLOR[level(e)] }} onClick={() => router.push(e.hasPage ? ctx.ws(`/messages/${e.messageId}`) : exploreHref(w, ctx))}>
              {cols.map((c) => eventCell(c, e, `${TD} group-hover:bg-sunken`, false, undefined, ctx.lang))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const GLYPH = {
  producer: <><rect x="3" y="3" width="18" height="18" rx="2" /><path d="M8 12h8" /><path d="m12 16 4-4-4-4" /></>,
  queue: <><rect x="2" y="6" width="20" height="12" rx="2" /><path d="M8 10v4" /><path d="M12 10v4" /><path d="M16 10v4" /></>,
  consumer: <><path d="M6 4h2l8 16h2" /><path d="m11 11-5 9" /></>,
  dlq: <><rect x="2" y="6" width="20" height="12" rx="2" /><path d="m10 10 4 4" /><path d="m14 10-4 4" /></>,
};
function FlowBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  const [ref, { w: sw, h: sh }] = useSize<HTMLDivElement>();
  const names = queuesFor(ctx.lang.parse(effectiveFilter(w, ctx.vars)), ctx.data.queues);
  const q = names[0];
  if (ctx.data.live) return <Empty><span>The flow of your own queues is on the <UiLink href={ctx.ws("")} className="text-brand underline underline-offset-2">overview</UiLink>.</span></Empty>;
  const flow = q ? ctx.sampleFlow?.(q) : undefined;
  if (!q || !flow) return <Empty>No queue matched this filter.</Empty>;
  const prods = flow.producers.slice(0, 3);
  const TS = 30, xa = 70, xm = sw / 2, xb = sw - 90;
  const ys = (n: number, i: number) => (n === 1 ? sh / 2 - 44 : 10 + ((sh - 96) * i) / (n - 1));
  const ym = sh / 2 - 44;
  const right = [{ kind: "consumer" as const, role: "Consumer", name: flow.consumer, fact: `p95 ${fmt(flow.p95Ms, "ms")}`, bad: false }, ...(flow.dlq ? [{ kind: "dlq" as const, role: "Dead-letter queue", name: flow.dlq, fact: fmt(flow.dlqDepth, "count"), bad: false }] : [])];
  const node = (cx: number, y: number, kind: keyof typeof GLYPH, role: string, name: string, fact?: string, bad = false) => (
    <g key={`${role}${name}`}>
      <rect x={cx - TS / 2} y={y} width={TS} height={TS} rx={7} fill="var(--color-canvas)" stroke="var(--color-line-strong)" />
      <g transform={`translate(${cx - 8},${y + 7}) scale(0.667)`} fill="none" stroke="var(--color-ink)" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">{GLYPH[kind]}</g>
      <text x={cx} y={y + TS + 15} textAnchor="middle" className="fill-ink font-sans text-xs font-semibold">{role}</text>
      <text x={cx} y={y + TS + 29} textAnchor="middle" className="fill-ink-3 font-mono text-[10.5px]">{name}</text>
      {fact && <text x={cx} y={y + TS + 43} textAnchor="middle" className={`font-mono text-[10.5px] ${bad ? "fill-critical-text" : "fill-ink-2"}`}>{fact}</text>}
    </g>
  );
  const wire = (ax: number, ay: number, bx: number, by: number, col: string, label: string, dash = false, key = "") => {
    const x1 = ax + TS / 2 + 4, x2 = bx - TS / 2 - 4, y1 = ay + TS / 2, y2 = by + TS / 2, mx = (x1 + x2) / 2, out = ax > sw / 2 - 1;
    return (
      <g key={key}>
        <path d={`M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2 - 5},${y2}`} fill="none" stroke={col} strokeWidth={1.75} strokeDasharray={dash ? "3 3" : undefined} />
        <path d={`M${x2 - 6},${y2 - 4} L${x2},${y2} L${x2 - 6},${y2 + 4}`} fill="none" stroke={col} strokeWidth={1.75} strokeLinecap="round" strokeLinejoin="round" />
        {label && <text x={out ? x2 - 14 : x1 + 12} y={(out ? y2 : y1) + (dash || (!out && ay > by) ? 17 : -8)} textAnchor={out ? "end" : "start"} className="fill-ink-2 font-mono text-[10.5px]">{label}</text>}
      </g>
    );
  };
  return (
    <div ref={ref} className="min-h-0 flex-1">
      {sw > 0 && (
        <svg width={sw} height={sh} role="img" aria-label={`Producers, consumer and dead-letter queue of ${q}`}>
          {prods.map((p, i) => wire(xa, ys(prods.length, i), xm, ym, "var(--color-series-2)", `${fmt(p.perMin, "count")}/min`, false, p.from))}
          {right.map((r, i) => wire(xm, ym, xb, ys(right.length, i), r.kind === "dlq" ? "var(--color-lost)" : "var(--color-series-1)", r.kind === "dlq" ? (flow.dlqDepth ? fmt(flow.dlqDepth, "count") : "") : `${fmt(Math.round(flow.sentPerMin * 0.99), "count")}/min`, r.kind === "dlq", r.name))}
          {prods.map((p, i) => node(xa, ys(prods.length, i), "producer", "Producer", p.from))}
          {node(xm, ym, "queue", "Queue", q, `${fmt(flow.visible, "count")} visible, ${fmt(flow.inFlight, "count")} in flight`)}
          {right.map((r, i) => node(xb, ys(right.length, i), r.kind, r.role, r.name, r.fact, r.bad))}
        </svg>
      )}
    </div>
  );
}

/* A note is plain text with four conveniences: # headings, blank-line paragraphs, [links](url) and `code`. */
function NoteBody({ w }: { w: Widget }) {
  const inline = (s: string) => s.split(/(\[[^\]]+\]\([^)\s]+\)|`[^`]+`)/g).map((part, i) => {
    const link = /^\[([^\]]+)\]\(([^)\s]+)\)$/.exec(part);
    if (link) return /^(https?:\/\/|\/)/.test(link[2]) ? <a key={i} href={link[2]} target="_blank" rel="noreferrer" className="text-brand underline underline-offset-2">{link[1]}</a> : link[1];
    if (/^`[^`]+`$/.test(part)) return <code key={i} className="rounded bg-sunken px-1 py-px font-mono text-caption text-ink">{part.slice(1, -1)}</code>;
    return part;
  });
  return (
    <div className="min-h-0 flex-1 overflow-auto text-[13px] leading-relaxed text-ink-2">
      {(w.text ?? "").split(/\n\s*\n/).map((block, i) => (block.startsWith("# ") ? <h4 key={i} className="mb-1 text-sm font-semibold text-ink">{inline(block.slice(2))}</h4> : <p key={i} className="mb-2 whitespace-pre-line">{inline(block)}</p>))}
    </div>
  );
}

const toneVar = (t: Tone | null) => (t ? TONE_VAR[t] : "var(--color-line-strong)");

export const FLUSH = new Set(["queue_table", "event_list"]);

export function WidgetBody({ w, ctx }: { w: Widget; ctx: Ctx }) {
  return <><Body w={w} ctx={ctx} /><QueueOnly w={w} ctx={ctx} /></>;
}

function Body({ w, ctx }: { w: Widget; ctx: Ctx }) {
  switch (w.type) {
    case "timeseries": return <TimeSeriesBody w={w} ctx={ctx} />;
    case "number": return <NumberBody w={w} ctx={ctx} />;
    case "gauge": return <GaugeBody w={w} ctx={ctx} />;
    case "toplist": return <TopListBody w={w} ctx={ctx} />;
    case "queue_table": return <QueueTableBody w={w} ctx={ctx} />;
    case "status_grid": return <StatusGridBody w={w} ctx={ctx} />;
    case "heatmap": return <HeatmapBody w={w} ctx={ctx} />;
    case "distribution": return <DistributionBody w={w} ctx={ctx} />;
    case "event_list": return <EventListBody w={w} ctx={ctx} />;
    case "queue_flow": return <FlowBody w={w} ctx={ctx} />;
    case "note": return <NoteBody w={w} />;
  }
}
