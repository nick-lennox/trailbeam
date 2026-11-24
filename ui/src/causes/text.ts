/*
  How a cause group is said: its trend, when it was seen, its dead-letter split, its versions and tenants.
  The same functions as the hosted app's lib/app/causes.ts, which can import them from here.
*/
import type { CauseGroup, CauseOutcome, VersionSeen } from "./types.ts";

/** The trend's window: the past 24 hours. */
export const TREND_MS = 86_400_000;
/** The trend's number of bars. */
export const TREND_BARS = 9;

/** A group's evidence times in n equal buckets over [fromMs, toMs]: its trend. Messages, not deliveries. */
export function trendOf(times: number[], fromMs: number, toMs: number, n = TREND_BARS): number[] {
  const out = Array.from({ length: n }, () => 0);
  const span = Math.max(1, toMs - fromMs);
  for (const t of times) {
    if (t < fromMs || t > toMs) continue;
    out[Math.min(n - 1, Math.floor(((t - fromMs) / span) * n))]++;
  }
  return out;
}

/** "2 in the DLQ, 3 retrying", or "" when none is in a dead-letter queue. */
export function dlqSplit(g: Pick<CauseGroup, "counts" | "dlqInferred">): string {
  const d = g.counts.dead_lettered;
  if (!d) return "";
  const note = g.dlqInferred ? (g.dlqInferred === d ? " (inferred from maxReceiveCount)" : `, ${g.dlqInferred} inferred from maxReceiveCount`) : "";
  return `${d} in the DLQ${note}${g.counts.stuck ? `, ${g.counts.stuck} retrying` : ""}`;
}

const pad = (n: number) => String(n).padStart(2, "0");
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** When a cause was first seen: "14:05 UTC" within a day, else "3 Nov". */
export function whenFirst(ms: number, nowMs: number): string {
  if (!Number.isFinite(ms)) return "unknown";
  const d = new Date(ms);
  if (nowMs - ms < 86_400_000) return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())} UTC`;
  return `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]}`;
}

/** When a cause was last seen: "now", "5m ago", "3h ago", "2d ago". */
export function whenLast(ms: number, nowMs: number): string {
  if (!Number.isFinite(ms)) return "unknown";
  const s = Math.max(0, (nowMs - ms) / 1000);
  if (s < 60) return "now";
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86_400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86_400)}d ago`;
}

/** "seen in 1.4.1, 1.4.2", the newest `max`, or "" with none. */
export function versionsText(vs: Pick<VersionSeen, "version">[], max = 3): string {
  if (!vs.length) return "";
  const shown = vs.slice(-max).map((v) => v.version);
  return `seen in ${vs.length > max ? `${shown.join(", ")} and ${vs.length - max} earlier` : shown.join(", ")}`;
}

/** "acme", "acme +2", or "" with none. */
export const tenantsText = (ts: string[]) => (ts.length ? (ts.length === 1 ? ts[0] : `${ts[0]} +${ts.length - 1}`) : "");

/** A cause's worst outcome, in words. */
export const OUTCOME_LABELS: Record<CauseOutcome, string> = {
  dead_lettered: "Dead-lettered",
  stuck: "Stuck",
  filtered: "Filtered by trigger",
};

/** The note above the cause rows. `aws`: the host reads CloudTrail and Lambda's log too (the hosted app). */
export function causesNote({ aws = false, capped = false, readLimit = 5000 }: { aws?: boolean; capped?: boolean; readLimit?: number } = {}): string {
  const n = readLimit.toLocaleString("en-US");
  return "One row per cause: what went wrong on each message's latest failed delivery, for its queue and consumer. "
    + `First and last seen are when Trailbeam saw the cause, on the clock of the host that reported it${aws ? " (or AWS's, for CloudTrail and Lambda's log)" : ""}. `
    + "A redriven message that fails again comes back under a new message ID, so it counts again here."
    + (capped ? ` At least ${n} messages are unfinished. Causes are from the ${n} ${aws ? "most recently active" : "oldest"}.` : "");
}
