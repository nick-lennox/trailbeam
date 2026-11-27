/*
  Time ranges for the Event explorer, the dashboards and every page built from the explorer's parts: one
  configurable range per page. Times are milliseconds since the epoch, shown in UTC. "Now" is always passed in:
  the host decides it (when the page was loaded, or the end of its sample data). Pure.
*/

export const MIN = 60_000;
export const HOUR = 60 * MIN;
export const DAY = 24 * HOUR;
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

export type TimeRange = { from: number; to: number; label: string; preset?: string };

export const PRESETS: { id: string; label: string; ms: number }[] = [
  { id: "5m", label: "Past 5 minutes", ms: 5 * MIN },
  { id: "15m", label: "Past 15 minutes", ms: 15 * MIN },
  { id: "1h", label: "Past 1 hour", ms: HOUR },
  { id: "4h", label: "Past 4 hours", ms: 4 * HOUR },
  { id: "1d", label: "Past 1 day", ms: DAY },
  { id: "2d", label: "Past 2 days", ms: 2 * DAY },
  { id: "7d", label: "Past 7 days", ms: 7 * DAY },
  { id: "30d", label: "Past 30 days", ms: 30 * DAY },
];
export const preset = (id: string, now: number): TimeRange => {
  const p = PRESETS.find((x) => x.id === id) ?? PRESETS[2];
  return { from: now - p.ms, to: now, label: p.label, preset: p.id };
};
/* A range that ends now, moved up to a later now; a fixed range stays where it is. */
export const follow = (t: TimeRange, now: number): TimeRange => (t.preset && t.to !== now ? { ...t, from: now - (t.to - t.from), to: now } : t);

const p2 = (n: number) => String(n).padStart(2, "0");
const parts = (ms: number) => {
  const d = new Date(ms);
  return { date: `${MONTHS[d.getUTCMonth()]} ${d.getUTCDate()}`, hm: `${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}`, s: p2(d.getUTCSeconds()) };
};
/* 14:05, 14:05:30 or Nov 13, 14:05, depending on what the range needs to tell times apart. */
export function stamp(ms: number, opts: { date?: boolean; seconds?: boolean } = {}) {
  const t = parts(ms);
  return `${opts.date ? `${t.date}, ` : ""}${t.hm}${opts.seconds ? `:${t.s}` : ""}`;
}
export const sameDay = (a: number, b: number) => parts(a).date === parts(b - 1).date;
export function rangeLabel(from: number, to: number) {
  const secs = from % MIN !== 0 || to % MIN !== 0;
  return sameDay(from, to) ? `${stamp(from, { date: true, seconds: secs })} to ${stamp(to, { seconds: secs })}` : `${stamp(from, { date: true })} to ${stamp(to, { date: true })}`;
}
export const custom = (from: number, to: number): TimeRange => ({ from, to, label: rangeLabel(from, to) });
/* "the past 4 hours" or "this time range", for sentences. */
export const phrase = (t: TimeRange) => (t.preset ? `the ${t.label.toLowerCase()}` : "this time range");

/* The value a datetime-local input takes, and back. */
export const toInput = (ms: number) => new Date(ms).toISOString().slice(0, 16);
export const fromInput = (v: string) => (v ? Date.parse(`${v.length === 16 ? `${v}:00` : v}Z`) : NaN);

const UNITS: [RegExp, number, string][] = [
  [/^(m|min|mins|minute|minutes)$/i, MIN, "minute"],
  [/^(h|hr|hrs|hour|hours)$/i, HOUR, "hour"],
  [/^(d|day|days)$/i, DAY, "day"],
  [/^(w|wk|wks|week|weeks)$/i, 7 * DAY, "week"],
];
/* One side of an absolute range: 14:05, 14:05:30, Nov 13 14:05 or 2026-11-13 14:05. A time with no date is on now's day. */
function point(text: string, now: number): number {
  const s = text.trim().replace(",", "");
  const today = new Date(now);
  let m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(s);
  if (m) return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +(m[6] ?? 0));
  m = /^(?:([A-Za-z]{3})[a-z]* (\d{1,2}) )?(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(s);
  if (!m) return NaN;
  const month = m[1] ? MONTHS.findIndex((x) => x.toLowerCase() === m![1].toLowerCase()) : today.getUTCMonth();
  if (month < 0 || +m[3] > 23 || +m[4] > 59) return NaN;
  return Date.UTC(today.getUTCFullYear(), month, m[2] ? +m[2] : today.getUTCDate(), +m[3], +m[4], +(m[5] ?? 0));
}
/* What someone typed, as a range: 45m, past 4 hours, 2d, 14:05 to 14:15, Nov 13 09:00 - Nov 14 09:00. */
export function parseRange(text: string, now: number): TimeRange | null {
  const s = text.trim();
  const rel = /^(?:past\s+|last\s+)?(\d+(?:\.\d+)?)\s*([a-z]+)$/i.exec(s);
  if (rel) {
    const u = UNITS.find(([re]) => re.test(rel[2]));
    const n = +rel[1];
    if (!u || n <= 0) return null;
    const known = PRESETS.find((p) => p.ms === n * u[1]);
    return known ? preset(known.id, now) : { from: now - n * u[1], to: now, label: `Past ${n} ${u[2]}${n === 1 ? "" : "s"}`, preset: `${n}${u[2][0]}` };
  }
  const sides = s.split(/\s+(?:to|until|-|–)\s+/i);
  if (sides.length !== 2) return null;
  const from = point(sides[0], now);
  const to = point(sides[1], now);
  return Number.isFinite(from) && Number.isFinite(to) && from < to ? custom(from, to) : null;
}

/* The bar width for a range: the finest step that keeps the chart to about 70 bars. */
const STEPS: [number, string][] = [[10_000, "10 seconds"], [30_000, "30 seconds"], [MIN, "minute"], [5 * MIN, "5 minutes"], [15 * MIN, "15 minutes"], [30 * MIN, "30 minutes"], [HOUR, "hour"], [4 * HOUR, "4 hours"], [12 * HOUR, "12 hours"], [DAY, "day"]];
export function bucket(t: TimeRange) {
  const [span, label] = STEPS.find(([s]) => (t.to - t.from) / s <= 70) ?? STEPS[STEPS.length - 1];
  return { span, label, n: Math.max(1, Math.ceil((t.to - t.from) / span)) };
}

/* A range in a link (QV-365), so a teammate who opens it sees the same span: ?range=4h for one that ends now, ?from=&to=
   in UTC for a fixed one. pinned() fixes a range that ends now at the times it covers right now, for a link to copy. */
const isoOf = (ms: number) => new Date(ms).toISOString().replace(".000Z", "Z");
const msOf = (v: string | null | undefined) => (!v ? NaN : /^\d{10,}$/.test(v) ? Number(v) : /^\d{4}-\d{2}-\d{2}T/.test(v) ? Date.parse(/Z$|[+-]\d{2}:?\d{2}$/.test(v) ? v : `${v}Z`) : NaN);
export const DEFAULT_RANGE = "1h";
export const pinned = (t: TimeRange): TimeRange => custom(t.from, t.to);
/* named: name the default range too, for a page whose default is something else (Needs attention: any time) */
export function rangeParams(t: TimeRange, named = false): [string, string][] {
  if (t.preset) return t.preset === DEFAULT_RANGE && !named ? [] : [["range", t.preset]];
  return [["from", isoOf(t.from)], ["to", isoOf(t.to)]];
}
export function withRange(href: string, t: TimeRange, named = false): string {
  const [path, query = ""] = href.split("?");
  const s = new URLSearchParams(query);
  for (const k of ["range", "from", "to"]) s.delete(k);
  for (const [k, v] of rangeParams(t, named)) s.set(k, v);
  const out = s.toString();
  return out ? `${path}?${out}` : path;
}
/** The range a link names, or the default; a link that names one that can't be read gets the default too. */
export function rangeFromParams(get: (k: string) => string | null | undefined, now: number, fallback = DEFAULT_RANGE): TimeRange {
  const from = msOf(get("from")), to = msOf(get("to"));
  if (Number.isFinite(from) && Number.isFinite(to) && from < to) return custom(from, to);
  const r = get("range");
  if (r) {
    const known = PRESETS.find((p) => p.id === r);
    if (known) return preset(known.id, now);
    const typed = parseRange(r, now);
    if (typed?.preset) return typed;
  }
  return preset(fallback, now);
}

/* The range a page opens on: the preset `id`, widened to the shortest longer preset that reaches back to
   `oldest`, or from `oldest` itself when none does (QV-620). So a list that draws only what the range holds
   still opens on everything it has, however long ago its oldest point was. */
export function rangeCovering(id: string, oldest: number | null, now: number): TimeRange {
  const r = preset(id, now);
  if (oldest === null || !Number.isFinite(oldest) || oldest >= r.from) return r;
  const wider = PRESETS.find((p) => p.ms > r.to - r.from && now - p.ms <= oldest);
  return wider ? preset(wider.id, now) : custom(Math.floor(oldest / MIN) * MIN, now);
}
