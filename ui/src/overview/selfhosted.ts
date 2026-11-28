/*
  Self-hosted Trailbeam's Overview at /overview (QV-392 design §2, §6): its URL state and its links. The range is
  the lists' (range=1h|24h|7d, or from_ms/to_ms) without "all time"; flow=, idle=1, refresh=0 and q= are the
  page's own. Links to the queue page and to Stuck carry the range in their own terms.
*/
import { queueQuery, type QueueRange } from "../queue/model.ts";
import { readRange, rangeText, rangeWindow, RANGE_PRESETS, type RangePreset, type TimeRange } from "../messages/range.ts";
import type { OverviewHrefs, QueueRef } from "./FlowDrawing.tsx";
import { custom, rangeParams } from "../model/timerange.ts";

export type OverviewUrl = {
  range: Extract<TimeRange, { kind: "preset" | "custom" }>;
  /** flow= as written: undefined when absent */
  flow: string | undefined;
  idle: boolean;
  refresh: boolean;
  q: string;
};

const DEFAULT: RangePreset = "1h";

/** The URL's state, or what is wrong with its range. No range (or "all time") is the past hour. */
export function readOverviewUrl(params: Record<string, string>): OverviewUrl | { error: string } {
  const r = readRange(params);
  if ("error" in r) return r;
  return {
    range: r.kind === "all" ? { kind: "preset", preset: DEFAULT } : r,
    flow: params.flow,
    idle: params.idle === "1",
    refresh: params.refresh !== "0",
    q: params.q ?? "",
  };
}

/** The URL for a state: defaults left out (range=1h, refresh on, no flow=, no idle, no q). */
export function overviewQuery(u: OverviewUrl): string {
  const p = new URLSearchParams();
  if (u.range.kind === "preset" && u.range.preset !== DEFAULT) p.set("range", u.range.preset);
  if (u.range.kind === "custom") {
    if (u.range.from_ms !== undefined) p.set("from_ms", String(u.range.from_ms));
    if (u.range.to_ms !== undefined) p.set("to_ms", String(u.range.to_ms));
  }
  if (u.flow !== undefined) p.set("flow", u.flow);
  if (u.idle) p.set("idle", "1");
  if (!u.refresh) p.set("refresh", "0");
  if (u.q) p.set("q", u.q);
  return p.toString();
}
export const overviewHref = (u: OverviewUrl) => { const q = overviewQuery(u); return q ? `/overview?${q}` : "/overview"; };

/** The window at `nowMs`, and how it reads in a sentence ("in the last hour"). A custom range without an end ends now. */
export function overviewWindow(r: OverviewUrl["range"], nowMs: number): { fromMs: number; toMs: number; during: string; endsNow: boolean } {
  const w = rangeWindow(r, nowMs);
  const endsNow = r.kind === "preset" || r.to_ms === undefined;
  return { fromMs: w.from_ms ?? 0, toMs: w.to_ms ?? nowMs, during: rangeText(r), endsNow };
}

/** The same rows later: a range ending now pinned to fixed times, refresh dropped. */
export function pinnedOverview(u: OverviewUrl, nowMs: number): string {
  const w = overviewWindow(u.range, nowMs);
  return overviewHref({ ...u, range: { kind: "custom", from_ms: w.fromMs, to_ms: w.toMs }, refresh: true });
}

/** The presets longer than the current one, for "No send or receive on any queue …". */
export function widerPresets(u: OverviewUrl): { label: string; href: string }[] {
  const cur = u.range.kind === "preset" ? RANGE_PRESETS.find(([k]) => k === (u.range as { preset: RangePreset }).preset)![2] : 0;
  return RANGE_PRESETS.filter(([, , s]) => s > cur && u.range.kind === "preset").map(([k, label]) => ({ label, href: overviewHref({ ...u, range: { kind: "preset", preset: k }, flow: undefined }) }));
}

/* the queue page's range for the Overview's: 24h is its 1d; a fixed range stays fixed */
const QUEUE_PRESET = { "1h": "1h", "24h": "1d", "7d": "7d" } as const;
function queueRange(u: OverviewUrl, nowMs: number): QueueRange {
  if (u.range.kind === "preset") return { kind: "preset", preset: QUEUE_PRESET[u.range.preset] };
  const w = overviewWindow(u.range, nowMs);
  return { kind: "fixed", fromMs: w.fromMs, toMs: w.toMs };
}
/** at=<account>:<region>, only when the label had to tell two same-named queues apart */
const atOf = (q: QueueRef) => (q.queue !== q.name && q.account ? `${q.account}:${q.region ?? ""}` : null);

/** The Overview's links in self-hosted Trailbeam: /queue?name=…, /issues?queue=…, both at the page's range. */
export function selfHostedHrefs(u: OverviewUrl, nowMs: number): OverviewHrefs {
  return {
    queue: (q, part) => `/queue?${queueQuery({ name: q.name, at: atOf(q), range: queueRange(u, nowMs) })}${part ? `#${part}` : ""}`,
    // the Issues list's range in its own terms (QV-708): 24h is its 1d, a fixed range is from= and to= in UTC
    stuck: (q) => {
      const p = new URLSearchParams({ queue: q.name });
      if (u.range.kind === "preset") p.set("range", u.range.preset === "24h" ? "1d" : u.range.preset);
      else if (u.range.kind === "custom" && u.range.from_ms !== undefined) {
        for (const [k, v] of rangeParams(custom(u.range.from_ms, u.range.to_ms ?? nowMs))) p.set(k, v);
      }
      return `/issues?${p}`;
    },
    setUp: "/",
  };
}
