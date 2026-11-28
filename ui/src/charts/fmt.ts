import { fmtBytes, fmtNum } from "../model/format.ts";

/* How the charts and the dashboard measures write a figure in its unit. Pure, so the measures (../dashboards/measures.ts)
   can use it outside a client module. */

export type Unit = "count" | "rate" | "s" | "ms" | "pct" | "bytes";

/* A length of time: 4s, 2m, 5m 12s, 3h 41m. */
function dur(sec: number): string {
  if (sec < 60) return `${sec < 10 && sec % 1 ? sec.toFixed(1) : Math.round(sec)}s`;
  const s = Math.round(sec), m = Math.floor(s / 60) % 60, h = Math.floor(s / 3600);
  if (h) return m ? `${h}h ${m}m` : `${h}h`;
  return s % 60 ? `${m}m ${s % 60}s` : `${m}m`;
}
/* A figure in its unit, as the chart's scale and its hover note write it. */
export function fmt(v: number, unit: Unit): string {
  if (!Number.isFinite(v)) return "Growing";
  if (unit === "pct") return `${v > 0 && v < 10 ? v.toFixed(1) : Math.round(v)}%`;
  if (unit === "s") return dur(v);
  if (unit === "ms") return v < 1000 ? `${Math.round(v)}ms` : dur(v / 1000);
  if (unit === "bytes") return fmtBytes(v);
  if (v >= 10_000) return `${(v / 1000).toFixed(v >= 100_000 ? 0 : 1).replace(/\.0$/, "")}k`;
  return fmtNum(Math.round(v));
}
