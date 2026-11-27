/*
  The Event explorer's chart from the recorded steps (GET /api/events' histogram, operation list-events), for when the
  events the host loaded miss part of the range: every recorded step in the range, counted by the server, with no
  message cap. The list below it still comes from the loaded events, so this chart is used only when the list is cut
  short in the range and the query is empty: the counts are of recorded steps (send, receive, delete, visibility change,
  Lambda outcome), with no status and nothing inferred, so they can't follow a query or split ok from failed.
  The host says where the answer comes from (EventExplorerProps.loadStepChart). Pure.
*/
import type { Ev } from "../model/events.ts";

export type StepChart = { from: number; span: number; per: string; buckets: { ok: number; err: number }[]; total: number };
/** The part of GET /api/events' answer the chart reads. */
export type StepChartDoc = { histogram: { bucket_ms: number; buckets: ({ start_ms: number } & Record<string, number>)[]; total: number } | null };

const KINDS = ["send", "receive", "delete", "change_visibility", "outcome"];
const NAMES: [number, string][] = [[60_000, "minute"], [300_000, "5 minutes"], [900_000, "15 minutes"], [3_600_000, "hour"], [10_800_000, "3 hours"], [21_600_000, "6 hours"], [86_400_000, "day"]];

/** "minute", "hour", "3 days": the width of a bucket, for "Recorded steps per …". */
export function bucketLabel(ms: number): string {
  const named = NAMES.find(([w]) => w === ms);
  if (named) return named[1];
  return ms % 86_400_000 === 0 ? `${ms / 86_400_000} days` : `${Math.round(ms / 60_000)} minutes`;
}

/** The answer's histogram as the explorer's bars: one per bucket, from the first bucket's start (an edge at or before the range). */
export function stepChart(doc: StepChartDoc): StepChart | null {
  const h = doc.histogram;
  if (!h || !h.buckets.length) return null;
  return {
    from: h.buckets[0].start_ms,
    span: h.bucket_ms,
    per: bucketLabel(h.bucket_ms),
    buckets: h.buckets.map((b) => ({ ok: KINDS.reduce((s, k) => s + (b[k] ?? 0), 0), err: 0 })),
    total: h.total,
  };
}

/** GET /api/events' query for [from, to): its window includes both ends, and one row is the least it returns. */
export function stepChartQuery(from: number, to: number): Record<string, string> {
  return { from_ms: String(Math.floor(from)), to_ms: String(Math.ceil(to) - 1), limit: "1" };
}

/** Whether the loaded events miss part of [from, to): the loader was capped and its oldest event is after from. */
export function cutShort(events: Pick<Ev, "ms">[], capped: boolean, from: number): boolean {
  if (!capped) return false;
  let oldest = Infinity;
  for (const e of events) if (e.ms < oldest) oldest = e.ms;
  return oldest > from;
}
