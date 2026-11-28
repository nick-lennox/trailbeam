import type { Ev } from "../model/events.ts";
import { fmt, type Unit } from "../charts/fmt.ts";
import { p95Of } from "../model/overview.ts";
import { queryLanguage, type QueryLanguage, type Term } from "../model/query.ts";
import { bucketize, drainS, healthOf, retentionPct, visibilityPct, windowFor, type QueueLive } from "../model/queue-live.ts";
import { preset, type TimeRange } from "../model/timerange.ts";
import { TABLE_COLUMNS, WIDGET_TYPES, type DashboardDef, type Rule, type Variable, type Widget } from "./types.ts";

/*
  What a widget can measure, and the numbers behind it: the hosted app's lib/dashboards/measures.ts, shared, so both
  products draw a dashboard the same way. Self-hosted Trailbeam's server works out the same figures in Python
  (server/src/trailbeam/server/measures.py), and both are run on contract/fixtures/measures-parity.json.

  Event measures (counts, rates, processing time, time in queue) are worked out from the Event explorer's events with
  the widget's filter. Queue measures (depth, age, headroom) come from each queue's AWS reads and the spans of the
  messages in it (data.queueLive, ../model/queue-live.ts); a queue without them has no figure, and a widget with none
  at all says why (noDepth). On the hosted app's sample data (data.live false) they are the sample figures the host
  passes in (SampleFigures).

  Queue measures are per queue: CloudWatch has no tenant or producer, so a filter term on anything but the queue does
  not narrow them (queueOnlyTerms), and the widget says so. Pure.
*/

/**
  The events the dashboards read, and the "now" their time ranges end at. live: the events are a workspace's own
  (always, self-hosted), not the hosted sample stream. queues: the queues a filter can pick from. capped: more events
  were in the range than were loaded, so the oldest are missing. queueLive: each queue's figures from AWS and its
  spans, by queue name; a queue missing from it has none. bodies: "shown" when the reader may see message bodies and
  they came with the events (bodiesCapped: some were left out), "admins" when the reader may not.
*/
export type EventData = {
  live: boolean; events: Ev[]; now: number; queues: string[]; capped: boolean; queueLive?: Record<string, QueueLive>;
  bodies?: "shown" | "admins"; bodiesCapped?: boolean;
};

export type Measure = {
  id: string;
  label: string;
  /* a shorter name for a table column */
  col?: string;
  /* the question it answers, which is how the editor groups measures */
  ask: string;
  unit: Unit;
  /* how several queues become one figure */
  agg: "sum" | "max";
  /* a queue measure: its per-queue figure right now on sample data (0 when the host has no sample figures).
     Absent for measures counted from events. */
  now?: (queue: string) => number;
  /* the same figure from the queue's AWS reads and spans; null when it has none.
     p95: the queue's p95 processing time over the range, from its events. */
  live?: (q: QueueLive, p95: () => number | null) => number | null;
  /* its history in n buckets over [from, to); absent when there is none (a figure worked out from the spans as they are now) */
  history?: (q: QueueLive, from: number, to: number, n: number, p95: (from: number, to: number) => number | null) => number[] | null;
  /* its history is worked out from the spans for ranges within the past 24 hours only, and only for the queues a
     dashboard asked for: a longer range has none (noHistory) */
  spans?: true;
  /* counted from events: which events, what to take from each, and how to fold what was taken
     (the highest, the average or the 95th percentile). A rate is events per minute. */
  ev?: { when?: (e: Ev) => boolean; take?: (e: Ev) => number | undefined; fold?: "max" | "avg" | "p95" };
};

/** The queue measures, which the hosted app's sample data gives figures for. */
export const QUEUE_MEASURES = [
  "messages_visible", "messages_in_flight", "oldest_age", "time_to_drain", "dlq_depth", "stuck_in_flight",
  "retention_headroom", "visibility_headroom", "near_dlq",
] as const;
export type QueueMeasureId = (typeof QUEUE_MEASURES)[number];

export type Tone = "good" | "warning" | "critical";

/**
  The hosted app's sample data: each queue measure's figure now, per queue, each queue's health, and which sample
  queues' figures climbed to where they are (the rest wobble around their level). Self-hosted Trailbeam has none.
*/
export type SampleFigures = {
  now: Partial<Record<QueueMeasureId, (queue: string) => number>>;
  tone: (queue: string) => Tone;
  climb?: Record<string, string[]>;
};

export const ASKS = ["Is it backing up?", "Are consumers keeping up?", "Is it failing?", "Is it about to fail?", "From event fields"];

const is = (...kinds: Ev["event"][]) => (e: Ev) => kinds.includes(e.event);

const drainNow = (q: QueueLive) => (q.windows[0] ? drainS(q.visible, q.windows[0].sent, q.windows[0].deleted, q.windows[0].periodS) : null);
const nearNow = (q: QueueLive) => (q.maxReceiveCount && q.nearDlq ? (q.nearDlq.inFlight ? (100 * q.nearDlq.count) / q.nearDlq.inFlight : 0) : null);

/* Time to drain at the end of each of n buckets: drainS on the points CloudWatch wrote before then (the visible
   count it last wrote, 0 before its first). The last bucket is the figure now, so the line ends where the number says. */
function drainHistory(q: QueueLive, from: number, to: number, n: number): number[] | null {
  const w = windowFor(q, from);
  if (!w) return null;
  const span = (to - from) / n;
  const byTime = (p: [number, number][]) => [...p].sort((a, b) => a[0] - b[0]);
  const vis = byTime(w.visible), sent = byTime(w.sent), del = byTime(w.deleted);
  let iv = 0, is = 0, id = 0;
  const out = Array.from({ length: n }, (_, i) => {
    const end = from + (i + 1) * span;
    while (iv < vis.length && vis[iv][0] < end) iv++;
    while (is < sent.length && sent[is][0] < end) is++;
    while (id < del.length && del[id][0] < end) id++;
    return drainS(iv ? vis[iv - 1][1] : 0, sent.slice(Math.max(0, is - 5), is), del.slice(Math.max(0, id - 5), id), w.periodS) ?? 0;
  });
  const now = drainNow(q);
  if (now !== null) out[n - 1] = now;
  return out;
}

/* A span-derived gauge in n buckets, ending at its figure now. It is on the day window only, so a range reaching
   back further than that window has none. */
function spanGauge(q: QueueLive, key: "stuck" | "nearDlq", from: number, to: number, n: number, now: number | null): number[] | null {
  const w = windowFor(q, from);
  const pts = w && from >= w.fromMs ? w[key] : undefined;
  if (!pts || now === null) return null;
  const out = bucketize(pts, from, to, n, "gauge");
  out[n - 1] = now;
  return out;
}

/* a CloudWatch gauge in n buckets, from the window that reaches back to from */
function gauge(q: QueueLive, key: "visible" | "inFlight" | "oldestS" | "dlq", from: number, to: number, n: number): number[] | null {
  const w = windowFor(q, from);
  return w ? bucketize(w[key], from, to, n, "gauge") : null;
}

const liveOf = (data: EventData, queue: string) => {
  const q = data.queueLive?.[queue];
  return q && q.status === "ok" ? q : undefined;
};

/* p95 processing time of one queue's events in [from, to), or null without any */
function p95Processing(data: EventData, queue: string, from: number, to: number): number | null {
  const xs = data.events.filter((e) => e.queue === queue && e.ms >= from && e.ms < to && (e.event === "delete" || e.event === "processing_error") && e.durationMs !== undefined).map((e) => e.durationMs!);
  return xs.length ? p95Of(xs) : null;
}

/** Why a queue has no AWS figures, in a sentence, for a widget or card that has none. */
export function whyNone(q: QueueLive | undefined): string {
  if (!q) return "No data. No connected AWS account names this queue. Connect its account in Connections to read its depth and settings.";
  return q.detail ?? "No data from CloudWatch for this queue yet.";
}

/** why, led by "No data." unless it already says so (whyNone's own sentences do, QV-635). */
const noData = (why: string) => (why.startsWith("No data") ? why : `No data. ${why}`);

/** Why a queue measure has no figure on a workspace's own data, in a sentence. */
export function noDepth(data: EventData): string {
  const qs = Object.values(data.queueLive ?? {});
  const failed = qs.find((q) => q.status === "failed");
  if (failed) return noData(whyNone(failed));
  const ok = qs.find((q) => q.status === "ok");
  if (ok) return "No data for this measure from the queues that match. It needs a setting the queue does not have (a redrive policy, for example) or a figure CloudWatch has not reported yet.";
  return noData(whyNone(qs[0]));
}

export { fmt, p95Of, type Unit };

/* A rule's limit, as someone types it, in the measure's own unit. */
export const fmtLimit = (v: number, unit: Unit) => fmt(v, unit);

/* ── filters and variables ────────────────────────────────────────── */

/* The filter a widget really runs: $name becomes the variable's value, and a
   widget that follows the dashboard gets every other variable added. */
export function effectiveFilter(w: Widget, vars: Variable[]): string {
  let f = w.filter ?? "";
  const used = new Set<string>();
  f = f.replace(/(\S*)\$([A-Za-z_][\w.]*)/g, (whole, head: string, name: string) => {
    const v = vars.find((x) => x.name === name);
    if (!v) return whole;
    used.add(name);
    return v.value === "*" ? "" : `${head}${/[\s()]/.test(v.value) ? `"${v.value}"` : v.value}`;
  });
  if (w.follow !== false) for (const v of vars) if (!used.has(v.name) && v.value !== "*") f += ` ${v.field}:${/[\s()]/.test(v.value) ? `"${v.value}"` : v.value}`;
  return f.replace(/\s+/g, " ").trim();
}

const wild = (pattern: string, name: string) => new RegExp(`^${pattern.toLowerCase().split("*").map((s) => s.replace(/[.+?^${}()|[\]\\]/g, "\\$&")).join(".*")}$`).test(name.toLowerCase());

/* The queues a filter leaves. Only its queue terms count for a queue measure. */
export function queuesFor(terms: Term[], names: string[]): string[] {
  const pos = terms.filter((t) => !t.neg && t.field === "queue" && t.value && !t.error);
  const neg = terms.filter((t) => t.neg && t.field === "queue" && t.value && !t.error);
  return names.filter((n) => (!pos.length || pos.some((t) => wild(t.value, n))) && !neg.some((t) => wild(t.value, n)));
}

/* ── series ───────────────────────────────────────────────────────── */

const hash = (s: string) => { let h = 2166136261; for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619); return h >>> 0; };
const rng = (seed: number) => () => { seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };

export type Series = { name: string; points: number[]; now: number };

/* ── alerting ─────────────────────────────────────────────────────── */

export function toneOf(v: number, rule?: Rule): Tone | null {
  if (!rule || (rule.warn === undefined && rule.crit === undefined)) return null;
  const over = (limit?: number) => limit !== undefined && (rule.below ? v < limit : v > limit);
  return over(rule.crit) ? "critical" : over(rule.warn) ? "warning" : "good";
}
export const TONE_WORD: Record<Tone, string> = { good: "OK", warning: "Warning", critical: "Critical" };

export type Alert = { widget: Widget; tone: Tone; value: string; limit: string; queue: string };

/* The most a stored dashboard definition may take, as JSON. Every member who opens the
   dashboards page loads it; the largest starter is a few kilobytes. */
export const MAX_DEF_BYTES = 256 * 1024;

const isObj = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const str = (v: unknown) => typeof v === "string";

const zero = () => 0;

/**
  The measures over one query language: the Event explorer's (queryLanguage(), what the spans recorded) unless the
  host passes its own, and with the host's sample figures, if it has any. Both products make one and pass it to the
  dashboard views (DashboardsEnv).
*/
export function dashboardMeasures(o: { language?: QueryLanguage; sample?: SampleFigures } = {}) {
  const { parse, matches, def, FIELD_DEFS } = o.language ?? queryLanguage();
  const sample = o.sample;
  const now = (id: QueueMeasureId) => sample?.now[id] ?? zero;

  const MEASURES: Measure[] = [
    { id: "messages_visible", label: "Messages visible", col: "Visible", ask: ASKS[0], unit: "count", agg: "sum", now: now("messages_visible"),
      live: (q) => q.visible, history: (q, f, t, n) => gauge(q, "visible", f, t, n) },
    { id: "messages_in_flight", label: "Messages in flight", col: "In flight", ask: ASKS[0], unit: "count", agg: "sum", now: now("messages_in_flight"),
      live: (q) => q.inFlight, history: (q, f, t, n) => gauge(q, "inFlight", f, t, n) },
    { id: "oldest_age", label: "Age of oldest message", col: "Oldest message", ask: ASKS[0], unit: "s", agg: "max", now: now("oldest_age"),
      live: (q) => q.oldestS, history: (q, f, t, n) => gauge(q, "oldestS", f, t, n) },
    /* visible messages over the recent net rate CloudWatch counts: deleted minus sent; its history is the same at the
       end of each bucket, from CloudWatch's points up to then, and the last bucket is the figure now */
    { id: "time_to_drain", label: "Time to drain", ask: ASKS[0], unit: "s", agg: "max", now: now("time_to_drain"),
      live: (q) => drainNow(q), history: (q, f, t, n) => drainHistory(q, f, t, n) },
    { id: "sent", label: "Sent per minute", ask: ASKS[1], unit: "rate", agg: "sum", ev: { when: is("send") } },
    { id: "received", label: "Received per minute", ask: ASKS[1], unit: "rate", agg: "sum", ev: { when: is("receive") } },
    { id: "deleted", label: "Deleted per minute", ask: ASKS[1], unit: "rate", agg: "sum", ev: { when: is("delete") } },
    { id: "processing_time", label: "Handling time, p95", ask: ASKS[1], unit: "ms", agg: "max", ev: { when: is("delete", "processing_error"), take: (e) => e.durationMs, fold: "p95" } },
    { id: "time_in_queue", label: "Time in queue, p95", ask: ASKS[1], unit: "ms", agg: "max", ev: { when: is("receive"), take: (e) => e.waitMs, fold: "p95" } },
    { id: "dlq_depth", label: "Dead-letter queue depth", ask: ASKS[2], unit: "count", agg: "sum", now: now("dlq_depth"),
      live: (q) => (q.dlq ? q.dlqDepth : null), history: (q, f, t, n) => (q.dlq ? gauge(q, "dlq", f, t, n) : null) },
    /* instrumented messages with no final settle, first seen more than 5 minutes ago (the Stuck page) */
    { id: "stuck_in_flight", label: "Stuck in flight", ask: ASKS[2], unit: "count", agg: "sum", now: now("stuck_in_flight"), live: (q) => q.stuck,
      spans: true, history: (q, f, t, n) => spanGauge(q, "stuck", f, t, n, q.stuck) },
    { id: "processing_errors", label: "Handling errors", ask: ASKS[2], unit: "count", agg: "sum", ev: { when: is("processing_error") } },
    { id: "dead_lettered", label: "Messages dead-lettered", ask: ASKS[2], unit: "count", agg: "sum", ev: { when: is("dead_letter") } },
    {
      id: "retention_headroom", label: "Retention headroom", ask: ASKS[3], unit: "pct", agg: "max", now: now("retention_headroom"),
      live: (q) => retentionPct(q.oldestS, q.retentionS),
      history: (q, f, t, n) => (q.retentionS ? gauge(q, "oldestS", f, t, n)?.map((v) => retentionPct(v, q.retentionS)!) ?? null : null),
    },
    {
      id: "visibility_headroom", label: "Visibility timeout headroom", ask: ASKS[3], unit: "pct", agg: "max", now: now("visibility_headroom"),
      /* p95 processing time from the queue's events, over its visibility timeout from GetQueueAttributes */
      live: (q, p) => visibilityPct(p(), q.visibilityTimeoutS),
      history: (q, f, t, n, p) => {
        if (!q.visibilityTimeoutS) return null;
        const span = (t - f) / n;
        return Array.from({ length: n }, (_, i) => visibilityPct(p(f + i * span, f + (i + 1) * span), q.visibilityTimeoutS) ?? 0);
      },
    },
    /* instrumented messages in flight on their last allowed receive (receive count at maxReceiveCount), as a share of
       the instrumented messages in flight */
    { id: "near_dlq", label: "One receive from the dead-letter queue", ask: ASKS[3], unit: "pct", agg: "max", now: now("near_dlq"),
      live: (q) => nearNow(q), spans: true, history: (q, f, t, n) => spanGauge(q, "nearDlq", f, t, n, nearNow(q)) },
    { id: "events", label: "Events matching the filter", ask: ASKS[4], unit: "count", agg: "sum", ev: {} },
    { id: "body_size", label: "Body size, average", ask: ASKS[4], unit: "bytes", agg: "max", ev: { take: (e) => e.sizeBytes, fold: "avg" } },
    { id: "receive_count", label: "Receive count, highest", ask: ASKS[4], unit: "count", agg: "max", ev: { take: (e) => e.receiveCount, fold: "max" } },
  ];
  const measure = (id: string) => MEASURES.find((m) => m.id === id);

  /** A queue measure's figure for one queue now: the sample figure, or the queue's live figure (null: none). */
  function queueValue(m: Measure, queue: string, data: EventData, time?: TimeRange): number | null {
    if (!m.now) return null;
    if (!data.live) return m.now(queue);
    const q = liveOf(data, queue);
    if (!q || !m.live) return null;
    const t = time ?? preset("1h", data.now);
    return m.live(q, () => p95Processing(data, queue, t.from, t.to));
  }

  /* A queue measure on a workspace's own data with no queue that has figures for it. */
  const depthMissing = (m: Measure, data: EventData) => !!m.now && data.live && !data.queues.some((q) => queueValue(m, q, data) !== null);
  /* A queue measure on own data with no history over the range: worked out from the spans as they are now, or from
     the spans' history (24 hours, measures with spans: true) when no queue has it for this range. Without a range, the past hour. */
  function noHistory(m: Measure, data: EventData, time?: TimeRange): boolean {
    if (!m.now || !data.live) return false;
    if (!m.history) return true;
    if (!m.spans) return false;
    const t = time ?? preset("1h", data.now);
    return !Object.values(data.queueLive ?? {}).some((q) => q.status === "ok" && m.history!(q, t.from, t.to, 2, () => null) !== null);
  }

  /* The measures whose history is worked out from the spans, and the queues a dashboard draws them for: every
     queue a widget using one of them can show, whatever its variables are set to. */
  const SPAN_MEASURES = new Set(["stuck_in_flight", "near_dlq"]);
  function spanQueues(d: DashboardDef, names: string[]): string[] {
    const out = new Set<string>();
    const any = d.variables.map((v) => ({ ...v, value: "*" }));
    for (const w of d.widgets) {
      const ids = w.type === "queue_table" ? (w.columns?.length ? w.columns : TABLE_COLUMNS) : (w.queries ?? []).map((q) => q.measure);
      if (!ids.some((id) => SPAN_MEASURES.has(id))) continue;
      for (const q of queuesFor(parse(effectiveFilter(w, any)), names)) out.add(q);
    }
    return [...out];
  }

  /* The filter terms a queue measure can't apply: everything but queue. CloudWatch counts per queue only. */
  const queueOnlyTerms = (filter: string): string[] => parse(filter).filter((t) => t.field !== "queue" && !t.error && t.raw.trim()).map((t) => t.raw.trim());

  /* How a queue figure moved to reach what it is now, on sample data. */
  function shape(m: Measure, queue: string, n: number): number[] {
    const r = rng(hash(`${m.id}/${queue}`));
    const climbs = sample?.climb?.[queue]?.includes(m.id);
    let v = 1;
    return Array.from({ length: n }, (_, i) => {
      v += (1 - v) * 0.3 + (r() - 0.5) * 0.12;
      const t = i / Math.max(1, n - 1);
      return Math.max(0, v * (climbs ? 0.12 + 0.88 * Math.max(0, (t - 0.4) / 0.6) ** 1.3 : 1));
    });
  }

  /* One measure as lines: one per group value, or one for all of it. */
  function seriesFor(m: Measure, filter: string, groupBy: string | undefined, time: TimeRange, n: number, data: EventData): Series[] {
    const terms = parse(filter);
    if (depthMissing(m, data)) return [];
    if (m.now) {
      const qs = queuesFor(terms, data.queues);
      const per = qs.flatMap((q) => {
        const v = queueValue(m, q, data, time);
        if (v === null) return [];
        if (data.live) {
          /* own data: CloudWatch's history; a figure with none is drawn flat at its value now (noHistory says so) */
          const lq = liveOf(data, q)!;
          const h = m.history?.(lq, time.from, time.to, n, (a, b) => p95Processing(data, q, a, b)) ?? null;
          return [{ name: q, now: v, points: h ?? Array<number>(n).fill(Number.isFinite(v) ? v : 0) }];
        }
        const base = Number.isFinite(v) ? v : 0;
        return [{ name: q, now: v, points: shape(m, q, n).map((k) => base * k) }];
      });
      if (groupBy === "queue") return per.sort((a, b) => (Number.isFinite(b.now) ? b.now : 1e12) - (Number.isFinite(a.now) ? a.now : 1e12));
      const fold = (vals: number[]) => (m.agg === "sum" ? vals.reduce((a, b) => a + b, 0) : Math.max(0, ...vals));
      return [{ name: m.label, now: fold(per.map((p) => p.now)), points: Array.from({ length: n }, (_, i) => fold(per.map((p) => p.points[i]))) }];
    }
    const span = (time.to - time.from) / n;
    const rows = data.events.filter((e) => e.ms >= time.from && e.ms < time.to && matches(e, terms) && (!m.ev?.when || m.ev.when(e)));
    const key = groupBy && def(groupBy) ? (e: Ev) => String(def(groupBy)!.get(e) ?? "none") : () => m.label;
    const by = new Map<string, Ev[]>();
    for (const e of rows) by.set(key(e), [...(by.get(key(e)) ?? []), e]);
    if (!by.size) by.set(m.label, []);
    /* minutes: the length of time the events fall in, for a rate */
    const val = (es: Ev[], minutes: number) => {
      if (!m.ev?.take) return m.unit === "rate" ? es.length / Math.max(minutes, 1e-9) : es.length;
      const xs = es.map((e) => m.ev!.take!(e)).filter((x): x is number => x !== undefined);
      if (!xs.length) return 0;
      if (m.ev.fold === "p95") return p95Of(xs);
      if (m.ev.fold === "max") return Math.max(...xs);
      return xs.reduce((a, b) => a + b, 0) / xs.length;
    };
    return [...by].map(([name, es]) => {
      const bucketed: Ev[][] = Array.from({ length: n }, () => []);
      for (const e of es) bucketed[Math.min(n - 1, Math.floor((e.ms - time.from) / span))].push(e);
      return { name, now: val(es, (time.to - time.from) / 60_000), points: bucketed.map((b) => val(b, span / 60_000)) };
    }).sort((a, b) => b.now - a.now);
  }

  /* Events per bucket, ok and failed, as the Event explorer's histogram counts them. */
  function eventBuckets(filter: string, time: TimeRange, n: number, data: EventData) {
    const terms = parse(filter);
    const span = (time.to - time.from) / n;
    const b = Array.from({ length: n }, () => ({ ok: 0, err: 0 }));
    for (const e of data.events) if (e.ms >= time.from && e.ms < time.to && matches(e, terms)) b[Math.min(n - 1, Math.floor((e.ms - time.from) / span))][e.status === "error" ? "err" : "ok"]++;
    return b;
  }
  const eventsFor = (filter: string, time: TimeRange, data: EventData) => { const terms = parse(filter); return data.events.filter((e) => e.ms >= time.from && e.ms < time.to && matches(e, terms)); };

  /* A queue's health, as the queue data gives it. The sidebar's dots show the same thing. */
  function queueTone(q: string, data: EventData): Tone | null {
    if (!data.live) return sample?.tone(q) ?? "good";
    const t = preset("1h", data.now);
    return healthOf(data.queueLive?.[q], p95Processing(data, q, t.from, t.to));
  }

  /* What a widget with a rule says right now: its figure, its status, and the queue behind it. */
  function alertOf(w: Widget, vars: Variable[], data: EventData): Alert | null {
    const m = measure(w.queries?.[0]?.measure ?? "");
    if (!m || !w.rule || (w.type !== "number" && w.type !== "timeseries") || depthMissing(m, data)) return null;
    const filter = effectiveFilter(w, vars);
    const time = preset(w.time ?? "1h", data.now);
    /* a queue rule whose queues have no figure has none, even if a queue it does not match has one (QV-357) */
    if (m.now && data.live && !queuesFor(parse(filter), data.queues).some((q) => queueValue(m, q, data, time) !== null)) return null;
    const all = seriesFor(m, filter, undefined, time, 12, data)[0];
    if (!all) return null;
    const tone = toneOf(all.now, w.rule);
    if (!tone) return null;
    const worst = m.now ? seriesFor(m, filter, "queue", time, 2, data)[0]?.name ?? "" : "";
    const limit = tone === "critical" || w.rule.warn === undefined ? w.rule.crit : w.rule.warn;
    return { widget: w, tone, value: fmt(all.now, m.unit), limit: `${w.rule.below ? "below" : "above"} ${fmtLimit(limit ?? 0, m.unit)}`, queue: worst };
  }

  /* The fields a widget may group by or count by: the Event explorer's own list. */
  // not by system yet (QV-308): the engine's field list (measures.py, contract/fixtures/measures-parity.json) has none
  const GROUP_FIELDS = FIELD_DEFS.filter((f) => f.facet && f.kind === "text" && f.group === "Event" && f.name !== "system").map((f) => ({ id: f.name, label: f.label }));
  const NUMBER_FIELDS = FIELD_DEFS.filter((f) => f.kind !== "text" && f.group === "Event").map((f) => ({ id: f.name, label: f.label }));

  /* A widget definition's mistakes, in plain words, or null. */
  function problem(w: Partial<Widget>): string | null {
    if (!w || typeof w !== "object") return "A widget is a JSON object.";
    if (!w.type || !(WIDGET_TYPES as readonly string[]).includes(w.type)) return `"type" is "${w.type ?? ""}", which is not a widget type.`;
    if (typeof w.title !== "string") return `"title" must be text.`;
    for (const q of w.queries ?? []) {
      if (!measure(q.measure)) return `queries has an unknown measure "${q.measure}".`;
      if (q.group_by && !def(q.group_by)) return `group_by has an unknown field "${q.group_by}".`;
    }
    const bad = parse(w.filter ?? "").find((t) => t.error && !t.raw.includes("$"));
    if (bad) return `filter: ${bad.error}`;
    if (w.type === "event_list") for (const c of w.columns ?? []) if (!["date", "event", "detail", "body"].includes(c) && !def(c)) return `columns has an unknown field "${c}".`;
    if (w.type === "queue_table") for (const c of w.columns ?? []) if (!measure(c)) return `columns has an unknown measure "${c}".`;
    if (w.field && !def(w.field)) return `field is "${w.field}", which is not a field.`;
    const l = w.layout;
    if (!l || [l.x, l.y, l.w, l.h].some((n) => typeof n !== "number")) return `"layout" needs x, y, w and h as numbers.`;
    return null;
  }

  /* A whole dashboard definition, checked before it is stored: its size, then each
     variable, section and widget. Absent variables and sections are empty lists. */
  function checkDef(raw: unknown): { def: DashboardDef } | { error: string } {
    if (!isObj(raw)) return { error: "A dashboard is a JSON object." };
    if (!Array.isArray(raw.widgets)) return { error: "That JSON has no list of widgets." };
    const variables = raw.variables ?? [];
    const sections = raw.sections ?? [];
    if (!Array.isArray(variables)) return { error: `"variables" must be a list.` };
    if (!Array.isArray(sections)) return { error: `"sections" must be a list.` };
    const d = { variables, sections, widgets: raw.widgets } as DashboardDef;
    const bytes = new TextEncoder().encode(JSON.stringify(d)).length;
    if (bytes > MAX_DEF_BYTES) return { error: `That dashboard is ${Math.ceil(bytes / 1024)} KB. A dashboard can be at most ${MAX_DEF_BYTES / 1024} KB.` };
    for (const [i, v] of variables.entries()) {
      if (!isObj(v) || !str(v.name) || !str(v.field) || !str(v.value)) return { error: `Variable ${i + 1} needs "name", "field" and "value" as text.` };
    }
    for (const [i, s] of sections.entries()) {
      if (!isObj(s) || !str(s.id) || !str(s.title)) return { error: `Section ${i + 1} needs "id" and "title" as text.` };
    }
    for (const [i, w] of d.widgets.entries()) {
      const p = problem(w);
      if (p) return { error: `Widget ${i + 1}: ${p}` };
      if (!str(w.id)) return { error: `Widget ${i + 1}: "id" must be text.` };
    }
    return { def: d };
  }

  return {
    MEASURES, measure, queueValue, depthMissing, noHistory, spanQueues, noDepth, queueOnlyTerms, effectiveFilter, queuesFor,
    seriesFor, eventBuckets, eventsFor, queueTone, alertOf, toneOf, GROUP_FIELDS, NUMBER_FIELDS, problem, checkDef, fmt, fmtLimit,
  };
}
export type DashboardMeasures = ReturnType<typeof dashboardMeasures>;
