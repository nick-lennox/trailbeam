/*
  The time range on the lists (Recent, Duplicates, Stuck), auto-refresh, and the pinned links "Copy link" makes.
  All of it lives in the page's URL:
    range=1h|24h|7d     a window ending now, worked out again on every load and every refresh
    from_ms, to_ms      a fixed window (custom, or a pinned link), both in ms since the epoch and both included
    refresh=15          ask the API again every 15 s
  The API takes from_ms/to_ms on /api/messages and /api/duplicates (last seen in the window), and only from_ms on
  /api/stuck (first seen at or after it; the upper bound there is always now minus older_than_s).
*/

export const RANGE_PRESETS = [["1h", "Last hour", 3_600], ["24h", "Last 24 hours", 86_400], ["7d", "Last 7 days", 604_800]] as const;
export type RangePreset = (typeof RANGE_PRESETS)[number][0];
export type TimeRange =
  | { kind: "all" }
  | { kind: "preset"; preset: RangePreset }
  | { kind: "custom"; from_ms?: number; to_ms?: number };

/** The refresh=15 value: how often an open list asks again, in seconds. */
export const REFRESH_S = 15;
/** What the lists read from the URL for the range and refresh, and so leave out of their own filters. */
export const RANGE_PARAMS = ["range", "from_ms", "to_ms", "refresh"] as const;

const MAX_MS = 10 ** 15; // the server's bound on from_ms/to_ms (server.py, _int)
const msParam = (v: string | undefined): number | undefined | null => {
  if (v === undefined || v === "") return undefined;
  if (!/^\d+$/.test(v) || Number(v) > MAX_MS) return null;
  return Number(v);
};

/** The URL's range, or what is wrong with it in a sentence the page can show (before asking the API). */
export function readRange(params: Record<string, string>): TimeRange | { error: string } {
  const range = params.range ?? "";
  if (range !== "") {
    if (!RANGE_PRESETS.some(([k]) => k === range)) return { error: `range must be 1h, 24h or 7d, not “${range}”.` };
    return { kind: "preset", preset: range as RangePreset };
  }
  const from_ms = msParam(params.from_ms);
  const to_ms = msParam(params.to_ms);
  if (from_ms === null) return { error: "from_ms must be a time in milliseconds since 1970." };
  if (to_ms === null) return { error: "to_ms must be a time in milliseconds since 1970." };
  if (from_ms === undefined && to_ms === undefined) return { kind: "all" };
  if (from_ms !== undefined && to_ms !== undefined && from_ms > to_ms) return { error: "The range starts after it ends: from_ms is after to_ms." };
  return { kind: "custom", from_ms, to_ms };
}

const presetSeconds = (p: RangePreset) => RANGE_PRESETS.find(([k]) => k === p)![2];

/**
 * The range as absolute times at ``nowMs``: a preset ends now. ``upper: false`` for /api/stuck, which takes no
 * to_ms. "all" gives {} (all history), or with ``pinNow`` an upper bound of now, as a pinned link wants.
 */
export function rangeWindow(r: TimeRange, nowMs: number, { upper = true, pinNow = false } = {}): { from_ms?: number; to_ms?: number } {
  const w: { from_ms?: number; to_ms?: number } =
    r.kind === "preset" ? { from_ms: Math.max(0, nowMs - presetSeconds(r.preset) * 1000), to_ms: nowMs }
      : r.kind === "custom" ? { from_ms: r.from_ms, to_ms: r.to_ms }
        : pinNow ? { to_ms: nowMs } : {};
  if (!upper) delete w.to_ms;
  return Object.fromEntries(Object.entries(w).filter(([, v]) => v !== undefined)) as { from_ms?: number; to_ms?: number };
}

/** The query the API is asked: the page's own filters plus the range's from_ms/to_ms at ``nowMs``. */
export function apiQuery(filters: Record<string, string>, r: TimeRange, nowMs: number, upper = true): Record<string, string> {
  const rest = Object.fromEntries(Object.entries(filters).filter(([k]) => !(RANGE_PARAMS as readonly string[]).includes(k)));
  const w = rangeWindow(r, nowMs, { upper });
  return { ...rest, ...Object.fromEntries(Object.entries(w).map(([k, v]) => [k, String(v)])) };
}

/**
 * The query for a link that shows the same rows later: range=… becomes absolute from_ms/to_ms at ``nowMs``
 * (all history gets to_ms=now), and refresh is dropped, since a fixed window has nothing new to show.
 */
export function pinnedQuery(params: Record<string, string>, r: TimeRange, nowMs: number, upper = true): Record<string, string> {
  const { range: _, from_ms: __, to_ms: ___, refresh: ____, ...rest } = params;
  const w = rangeWindow(r, nowMs, { upper, pinNow: true });
  return { ...rest, ...Object.fromEntries(Object.entries(w).map(([k, v]) => [k, String(v)])) };
}

/** A ms time as a datetime-local input's value, in UTC (the pages' times are all UTC). */
export const toInputUtc = (ms: number) => new Date(ms).toISOString().slice(0, 16);
/** A datetime-local value ("2026-11-23T10:00", read as UTC) as ms, or undefined when blank or unreadable. */
export function fromInputUtc(v: string | undefined): number | undefined {
  const m = /^(\d{4}-\d\d-\d\dT\d\d:\d\d)(:\d\d)?$/.exec(v?.trim() ?? "");
  if (!m) return undefined;
  const ms = Date.parse(`${m[1]}${m[2] ?? ":00"}Z`);
  return Number.isNaN(ms) ? undefined : ms;
}

/**
 * The URL query a list's filter form leads to. The form sends range=""|1h|24h|7d|custom, and for custom the
 * datetime-local fields from/to (UTC): those become from_ms/to_ms (to includes its whole minute). Blank fields go.
 */
export function formQuery(entries: Iterable<[string, string]>): Record<string, string> {
  const f: Record<string, string> = {};
  for (const [k, v] of entries) if (v !== "") f[k] = v;
  const { range, from, to, ...rest } = f;
  if (range === "custom") {
    const from_ms = fromInputUtc(from);
    const to_ms = fromInputUtc(to);
    return { ...rest, ...(from_ms !== undefined && { from_ms: String(from_ms) }), ...(to_ms !== undefined && { to_ms: String(to_ms + 59_999) }) };
  }
  return { ...rest, ...(range && { range }) };
}

/** How the range reads in a sentence: "in the last hour", "from 2026-11-23 10:00 UTC to now". */
export function rangeText(r: TimeRange): string {
  const t = (ms: number) => new Date(ms).toISOString().slice(0, 16).replace("T", " ") + " UTC";
  if (r.kind === "all") return "at any time";
  if (r.kind === "preset") return `in the ${RANGE_PRESETS.find(([k]) => k === r.preset)![1].toLowerCase()}`;
  if (r.from_ms !== undefined && r.to_ms !== undefined) return `between ${t(r.from_ms)} and ${t(r.to_ms)}`;
  return r.from_ms !== undefined ? `since ${t(r.from_ms)}` : `up to ${t(r.to_ms!)}`;
}

export const refreshOn = (params: Record<string, string>) => params.refresh === String(REFRESH_S);
