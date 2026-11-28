/*
  The queue page's rules that are not drawing (QV-360 design): the URL state (§2), the page's step (§4.1), the
  health verdict from spans (§1.4, moved here from the hosted app's queue-live.ts), the SQS console link (§3.1),
  the coverage line (§4.4), and the words the page prints. Pure functions, shared by both products.
*/
import type { Bucket, Point, Tone } from "./types.ts";

const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;

/** The time picker's presets: the hosted app's PRESETS (lib/timerange.ts), id, label, length. */
export const QUEUE_PRESETS = [
  ["5m", "Past 5 minutes", 5 * MIN],
  ["15m", "Past 15 minutes", 15 * MIN],
  ["1h", "Past 1 hour", HOUR],
  ["4h", "Past 4 hours", 4 * HOUR],
  ["1d", "Past 1 day", DAY],
  ["2d", "Past 2 days", 2 * DAY],
  ["7d", "Past 7 days", 7 * DAY],
  ["30d", "Past 30 days", 30 * DAY],
] as const;
export type QueuePreset = (typeof QUEUE_PRESETS)[number][0];
export const DEFAULT_QUEUE_RANGE: QueuePreset = "1h";

/** The steps a page can have, in seconds: 1, 5, 15 and 30 min, 1, 3 and 6 h (the server's queues.STEPS_S). */
export const STEPS_S = [60, 300, 900, 1800, 3600, 10800, 21600] as const;

/** The page's step (§4.1): max(60 s, nice(range / 120)). 1 h gives 60 s, 1 d 15 min, 7 d 3 h. */
export function pageStep(fromMs: number, toMs: number): number {
  const raw = (toMs - fromMs + 1) / 1000 / 120;
  return Math.max(60, STEPS_S.find((s) => s >= raw) ?? STEPS_S[STEPS_S.length - 1]);
}

/* ── the URL ──────────────────────────────────────────────────────────── */

export type QueueRange = { kind: "preset"; preset: QueuePreset } | { kind: "fixed"; fromMs: number; toMs: number };
export type QueueUrl = {
  name: string;
  /** account:region, when two queues share the name */
  at: string | null;
  range: QueueRange;
  /** the selected minutes, [from, to) in ms */
  sel: [number, number] | null;
  split: boolean;
};

const MAX_MS = 10 ** 15;
const msOf = (v: string | null | undefined) => (v && /^\d{1,16}$/.test(v) && Number(v) <= MAX_MS ? Number(v) : null);

/**
 * The page's state from its query (§2): range=<preset> or from=<ms>&to=<ms>, sel=<from>-<to>, split=version,
 * at=<account>:<region>, and name= (self-hosted; the hosted app has the name in its path). What can't be read
 * falls back to the default rather than failing the page; `problems` says what was ignored.
 */
export function readQueueUrl(get: (k: string) => string | null | undefined): QueueUrl & { problems: string[] } {
  const problems: string[] = [];
  const name = (get("name") ?? "").trim();
  const at = (get("at") ?? "").trim() || null;
  let range: QueueRange = { kind: "preset", preset: DEFAULT_QUEUE_RANGE };
  const r = get("range");
  const from = msOf(get("from")), to = msOf(get("to"));
  if (r) {
    if (QUEUE_PRESETS.some(([k]) => k === r)) range = { kind: "preset", preset: r as QueuePreset };
    else problems.push(`range must be one of ${QUEUE_PRESETS.map(([k]) => k).join(", ")}, not “${r}”.`);
  } else if (from !== null && to !== null) {
    if (from < to) range = { kind: "fixed", fromMs: from, toMs: to };
    else problems.push("The range starts after it ends: from is after to.");
  } else if (get("from") || get("to")) problems.push("A fixed range needs both from and to, in ms since 1970.");
  let sel: [number, number] | null = null;
  const s = get("sel");
  if (s) {
    const m = /^(\d{1,16})-(\d{1,16})$/.exec(s);
    if (m && Number(m[1]) < Number(m[2])) sel = [Number(m[1]), Number(m[2])];
    else problems.push("sel must be <from ms>-<to ms>.");
  }
  return { name, at, range, sel, split: get("split") === "version", problems };
}

/** The query that shows this state again: what readQueueUrl reads, defaults left out. */
export function queueQuery(u: Partial<QueueUrl>, { withName = true } = {}): string {
  const p = new URLSearchParams();
  if (withName && u.name) p.set("name", u.name);
  if (u.at) p.set("at", u.at);
  if (u.range?.kind === "preset" && u.range.preset !== DEFAULT_QUEUE_RANGE) p.set("range", u.range.preset);
  if (u.range?.kind === "fixed") { p.set("from", String(u.range.fromMs)); p.set("to", String(u.range.toMs)); }
  if (u.sel) p.set("sel", `${u.sel[0]}-${u.sel[1]}`);
  if (u.split) p.set("split", "version");
  return p.toString();
}

export const presetMs = (p: QueuePreset) => QUEUE_PRESETS.find(([k]) => k === p)![2];
export const presetLabel = (p: QueuePreset) => QUEUE_PRESETS.find(([k]) => k === p)![1];

/** The range at nowMs: a preset ends now. */
export function queueWindow(r: QueueRange, nowMs: number): { fromMs: number; toMs: number; stepS: number; endsNow: boolean } {
  const fromMs = r.kind === "preset" ? nowMs - presetMs(r.preset) : r.fromMs;
  const toMs = r.kind === "preset" ? nowMs : r.toMs;
  return { fromMs, toMs, stepS: pageStep(fromMs, toMs), endsNow: r.kind === "preset" || toMs >= nowMs };
}

/* ── the health verdict from spans (moved from the hosted app's lib/app/queue-live.ts) ─────── */

/** "Failing now": a cause with at least this many messages, last seen this recently */
export const FAILING_NOW = { messages: 10, withinMs: 15 * 60_000 };

/** What spanHealth reads: every figure is from the services' own spans. */
export type SpanFigures = {
  /** receives on the dead-letter queue in the past hour that SQS marked as from this queue */
  seenOnDlq: number;
  /** moves to the dead-letter queue worked out from receive counts against maxReceiveCount */
  moved: number;
  /** messages received and not settled for longer than the Stuck page's limit */
  stuck: number;
  /** a cause on this queue with FAILING_NOW.messages or more, last seen within FAILING_NOW.withinMs */
  failingNow: boolean;
  /** an unfinished message whose latest delivery failed, received twice or more */
  retrying: boolean;
  /** any send or receive on the queue in the range */
  active: boolean;
};

/**
  The queue's health from its spans alone, for a queue AWS wasn't read for. Null when no span reported on the
  queue: without AWS an idle queue and one whose consumers stopped reporting look the same. Spans can't see
  depth, the oldest message's age, retention or messages from services without Trailbeam.
*/
export function spanHealth(f: SpanFigures): { tone: Tone; why: string; basis: "spans" } | null {
  const v = (tone: Tone, why: string) => ({ tone, why, basis: "spans" as const });
  if (f.seenOnDlq > 0) return v("critical", "DLQ moves");
  if (f.failingNow) return v("critical", "Failing now");
  if (f.moved > 0) return v("warning", "Near the DLQ");
  if (f.stuck > 0) return v("warning", "Stuck messages");
  if (f.retrying) return v("warning", "Received again");
  if (f.active) return v("good", "Healthy");
  return null;
}

/** What the health pill's hover says when the verdict is from spans alone. */
export const SPAN_HEALTH_NOTE = "From your services' spans only. Without AWS, Trailbeam can't see queue depth, the oldest message's age, the retention period or messages from services without Trailbeam.";
export const NO_VERDICT = "No verdict: no span reported on this queue in the range";

/** The span figures a page's buckets give: DLQ moves seen in the last hour, moves worked out, and activity. */
export function spanFiguresOf(series: Bucket[], toMs: number, rest: Pick<SpanFigures, "stuck" | "failingNow" | "retrying">): SpanFigures {
  const hour = series.filter((b) => b.t >= toMs - HOUR);
  return {
    seenOnDlq: hour.reduce((n, b) => n + b.dlqSeen, 0),
    moved: series.reduce((n, b) => n + b.dlqInferred, 0),
    active: series.some((b) => b.sends + b.receives > 0),
    ...rest,
  };
}

/* ── links ────────────────────────────────────────────────────────────── */

const AWS_REGION = /^[a-z]{2}(-gov|-iso[a-z]?)?-[a-z]+-\d+$/;

/**
 * The SQS console for the queue (§3.1): the queue itself when region and account are known (or its URL is), the
 * region's queue list when only the region is, else null. Only a link: Trailbeam makes no call.
 */
export function consoleUrl(q: { name: string; account: string | null; region: string | null; url?: string | null }): string | null {
  // an AWS region (eu-west-1, us-gov-west-1, cn-north-1); an emulator's ("elasticmq") has no console
  if (!q.region || !AWS_REGION.test(q.region)) return null;
  const base = `https://console.aws.amazon.com/sqs/v3/home?region=${encodeURIComponent(q.region)}`;
  const url = q.url ?? (q.account ? `https://sqs.${q.region}.amazonaws.com/${q.account}/${q.name}` : null);
  return url ? `${base}#/queues/${encodeURIComponent(url)}` : `${base}#/queues`;
}

/* ── numbers and words ───────────────────────────────────────────────── */

export const fmtNum = (n: number) => n.toLocaleString("en-US");
/** A rate per minute: one decimal under 10. */
export const fmtRate = (n: number) => (n < 10 && n !== 0 && !Number.isInteger(n) ? n.toFixed(1) : fmtNum(Math.round(n)));
/** A bucket's count as a rate per minute, whatever the step (§4.1). */
export const perMin = (count: number, stepS: number) => (count * 60) / stepS;

/** A duration in ms: "840 ms", "2.8 s", "41 s", "6 min 12 s", "3 h 5 min", "2 d 4 h". */
export function fmtMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = ms / 1000;
  if (s < 10) return `${s.toFixed(1)} s`;
  if (s < 60) return `${Math.round(s)} s`;
  const m = Math.floor(s / 60), rs = Math.round(s % 60);
  if (m < 60) return rs ? `${m} min ${rs} s` : `${m} min`;
  const h = Math.floor(m / 60), rm = m % 60;
  if (h < 24) return rm ? `${h} h ${rm} min` : `${h} h`;
  const d = Math.floor(h / 24), rh = h % 24;
  return rh ? `${d} d ${rh} h` : `${d} d`;
}
export const fmtS = (s: number) => fmtMs(s * 1000);

/** A UTC time: "14:31", with the date when `date`, with seconds when `seconds`. */
export function utc(ms: number, { date = false, seconds = false } = {}): string {
  const iso = new Date(ms).toISOString();
  const time = iso.slice(11, seconds ? 19 : 16);
  return date ? `${iso.slice(0, 10)} ${time}` : time;
}
export const spansDays = (fromMs: number, toMs: number) => new Date(fromMs).toISOString().slice(0, 10) !== new Date(toMs - 1).toISOString().slice(0, 10);
/** "14:31–14:32", with dates when the window crosses midnight. */
export const windowText = (fromMs: number, toMs: number) => {
  const d = spansDays(fromMs, toMs);
  return `${utc(fromMs, { date: d })}–${utc(toMs, { date: d })}`;
};

/* ── the coverage line (§4.4) ─────────────────────────────────────────── */

/** CloudWatch's sends against the spans' sends over the range: null when there's nothing honest to say. */
export function coverageText(cloudwatchSent: number | null, spanSends: number): string | null {
  if (cloudwatchSent === null) return null;
  if (cloudwatchSent === 0 && spanSends === 0) return null;
  if (Math.abs(cloudwatchSent - spanSends) <= 0.02 * Math.max(cloudwatchSent, spanSends)) return "CloudWatch's sends match what your services reported.";
  if (cloudwatchSent > spanSends) {
    return `CloudWatch counted ${fmtNum(cloudwatchSent)} sends; spans recorded ${fmtNum(spanSends)}. ${fmtNum(cloudwatchSent - spanSends)} came from services without Trailbeam, or spans that didn't arrive.`;
  }
  return null; // spans above CloudWatch: late CloudWatch points at the range edge
}

/* ── the time grid ────────────────────────────────────────────────────── */

/** Every bucket start from the range's first to its last, a step apart. */
export function grid(fromMs: number, toMs: number, stepS: number): number[] {
  const step = stepS * 1000;
  const out: number[] = [];
  for (let t = Math.floor(fromMs / step) * step; t <= toMs; t += step) out.push(t);
  return out;
}

/** CloudWatch points on the page's grid: each point at the bucket its period starts in (null where there's none). */
export function onGrid(points: Point[], times: number[], stepS: number): (number | null)[] {
  const step = stepS * 1000;
  const by = new Map<number, number>();
  for (const [t, v] of points) {
    const k = Math.floor(t / step) * step;
    by.set(k, Math.max(by.get(k) ?? -Infinity, v));
  }
  return times.map((t) => by.get(t) ?? null);
}

/** Deploy markers that fall within `frac` of the chart's width of each other collapse into one ("3 deploys"), from three. */
export function collapseMarkers<M extends { firstMs: number }>(ms: M[], fromMs: number, toMs: number, frac = 0.08): { t: number; items: M[] }[] {
  const sorted = [...ms].sort((a, b) => a.firstMs - b.firstMs);
  const near = (toMs - fromMs) * frac;
  const groups: { t: number; items: M[] }[] = [];
  for (const m of sorted) {
    const g = groups[groups.length - 1];
    if (g && m.firstMs - g.items[0].firstMs <= near) g.items.push(m);
    else groups.push({ t: m.firstMs, items: [m] });
  }
  // two close together stay two markers: only three or more collapse
  return groups.flatMap((g) => (g.items.length >= 3 ? [g] : g.items.map((m) => ({ t: m.firstMs, items: [m] }))));
}

/** A chart's summary for its aria-label: each line's first and last value and its highest. */
export function chartSummary(title: string, rangeWords: string, lines: { label: string; values: (number | null)[] }[], fmt: (n: number) => string): string {
  const parts = lines.map((l) => {
    const vs = l.values.filter((v): v is number => v !== null);
    if (!vs.length) return `${l.label}: no data`;
    return `${l.label} from ${fmt(vs[0])} to ${fmt(vs[vs.length - 1])}, highest ${fmt(Math.max(...vs))}`;
  });
  return `${title}, ${rangeWords}: ${parts.join("; ")}.`;
}

/** Totals of the buckets in [fromMs, toMs): what "Messages in ‹window›" counts. */
export function totalsIn(series: Bucket[], fromMs: number, toMs: number) {
  const in_ = series.filter((b) => b.t >= fromMs && b.t < toMs);
  const sum = (k: "sends" | "receives" | "failed" | "reReceives" | "handled" | "dlqSeen" | "dlqInferred") => in_.reduce((n, b) => n + b[k], 0);
  return { sends: sum("sends"), receives: sum("receives"), handled: sum("handled"), failedOrRetried: sum("failed") + sum("reReceives"), dlq: sum("dlqSeen") + sum("dlqInferred") };
}

/** p95 of a list (nearest rank). */
export function p95(vs: number[]): number | null {
  if (!vs.length) return null;
  const s = [...vs].sort((a, b) => a - b);
  return s[Math.max(0, Math.ceil(0.95 * s.length) - 1)];
}
