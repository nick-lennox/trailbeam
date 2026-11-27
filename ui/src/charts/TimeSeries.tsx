"use client";

import { createContext, useContext, useId, useLayoutEffect, useRef, useState } from "react";
import { DAY, HOUR, MIN, stamp } from "../model/timerange.ts";
import { fmtBytes, fmtNum } from "../model/format.ts";

/*
  The charts over a time range, moved from the hosted dashboards (QV-583) for the Issues list, and for the dashboards
  when they come to open source. Drawn the way the rest of the app draws: flat fills, faint solid gridlines, mono
  figures, and hatching for messages that are not visible. Every time series shares one crosshair through HoverCtx,
  so the same minute is marked on all of them.
*/

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

/* The width and height of an element, kept current. */
export function useSize<T extends HTMLElement>() {
  const ref = useRef<T>(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const read = () => setSize({ w: el.clientWidth, h: el.clientHeight });
    read();
    const ro = new ResizeObserver(read);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, size] as const;
}
export const CAT = ["var(--color-series-1)", "var(--color-series-2)", "var(--color-series-3)", "var(--cat-4)", "var(--cat-5)"];
export const OTHER = "var(--color-ink-3)";

/* The moment under the pointer, as a share of the time range, on any chart. */
export const HoverCtx = createContext<{ at: number | null; set: (f: number | null) => void }>({ at: null, set: () => {} });

export type Line = { name: string; color: string; points: number[]; hatch?: boolean };

/* The top of the scale: a round number whose quarters are round too. */
function niceMax(n: number) {
  if (!(n > 0)) return 4;
  const p = 10 ** Math.floor(Math.log10(n));
  return [1, 2, 4, 6, 8, 10].map((m) => m * p).find((v) => v >= n)!;
}
/* Time labels on round times: every 10 minutes for an hour, every 6 hours for two days, and so on. */
const STEPS = [MIN, 2 * MIN, 5 * MIN, 10 * MIN, 15 * MIN, 30 * MIN, HOUR, 2 * HOUR, 4 * HOUR, 6 * HOUR, 12 * HOUR, DAY, 2 * DAY, 7 * DAY];
function ticksX(from: number, to: number, n = 6) {
  const step = STEPS.find((s) => (to - from) / s <= n) ?? STEPS[STEPS.length - 1];
  const out: { f: number; label: string }[] = [];
  for (let t = Math.ceil(from / step) * step; t < to; t += step) {
    const f = (t - from) / (to - from);
    if (f > 0.03 && f < 0.97) out.push({ f, label: stamp(t, { date: step >= DAY }).replace(/, 00:00$/, "") });
  }
  return out;
}
const GRID = [0, 1, 2, 3, 4];

export function TimeSeries({ lines, from, to, unit, kind = "line", marker, legend = true, onZoom, onPick }: {
  lines: Line[];
  from: number;
  to: number;
  unit: Unit;
  kind?: "line" | "stacked" | "bars";
  marker?: { value: number; label: string };
  legend?: boolean;
  onZoom?: (from: number, to: number) => void;
  onPick?: (at: number) => void;
}) {
  const [ref, { w, h }] = useSize<HTMLDivElement>();
  const [outer, { w: ow }] = useSize<HTMLDivElement>();
  /* with room to spare, the legend is a table beside the chart, as on a metrics dashboard */
  const side = legend && lines.length > 1 && ow >= 900;
  /* a count per interval is read as a total; anything else as its latest value */
  const tail = (l: Line) => (kind === "bars" ? l.points.reduce((a, b) => a + b, 0) : l.points[l.points.length - 1]);
  const hover = useContext(HoverCtx);
  const [drag, setDrag] = useState<{ a: number; b: number } | null>(null);
  const dragRef = useRef(drag);
  dragRef.current = drag;
  const hid = useId();
  const L = 40, R = 10, T = 10, B = 18;
  const pw = Math.max(1, w - L - R), ph = Math.max(1, h - T - B);
  const n = lines[0]?.points.length ?? 0;
  const stackTop = Array.from({ length: n }, (_, i) => lines.reduce((a, l) => a + l.points[i], 0));
  /* a figure without end (Time to drain while the backlog grows) is drawn along the top */
  const peak = Math.max(marker?.value ?? 0, ...(kind === "line" ? lines.flatMap((l) => l.points) : stackTop).filter(Number.isFinite));
  const top = niceMax(peak * 1.05);
  const X = (i: number) => L + (n > 1 ? (pw * i) / (n - 1) : pw / 2);
  const Y = (v: number) => T + ph * (1 - Math.min(v, top) / top);
  const frac = (clientX: number, el: Element) => Math.max(0, Math.min(1, (clientX - el.getBoundingClientRect().left - L) / pw));
  const at = hover.at;
  const hi = at === null ? null : Math.round(at * (n - 1));
  const path = (pts: number[]) => pts.map((v, i) => `${i ? "L" : "M"}${X(i).toFixed(1)},${Y(v).toFixed(1)}`).join(" ");

  return (
    <div ref={outer} className={`flex min-h-0 flex-1 ${side ? "gap-4" : "flex-col"}`}>
      <div ref={ref} className="relative min-h-0 min-w-0 flex-1 select-none">
        {w > 0 && n > 0 && (
          <svg
            width={w}
            height={h}
            className={onZoom ? "cursor-crosshair" : ""}
            role="img"
            aria-label={`${lines.map((l) => l.name).join(", ")} from ${stamp(from)} to ${stamp(to)}`}
            onPointerMove={(e) => { const f = frac(e.clientX, e.currentTarget); hover.set(f); if (dragRef.current) setDrag({ ...dragRef.current, b: f }); }}
            onPointerLeave={() => { hover.set(null); }}
            onPointerDown={(e) => { if (!onZoom) return; e.currentTarget.setPointerCapture(e.pointerId); const f = frac(e.clientX, e.currentTarget); setDrag({ a: f, b: f }); }}
            onPointerUp={(e) => {
              const d = dragRef.current;
              setDrag(null);
              if (!d) return;
              const a = Math.min(d.a, d.b), b = Math.max(d.a, d.b);
              if (b - a > 0.02) onZoom?.(Math.round(from + a * (to - from)), Math.round(from + b * (to - from)));
              else onPick?.(Math.round(from + frac(e.clientX, e.currentTarget) * (to - from)));
            }}
          >
            <defs>
              <pattern id={hid} width="3" height="3" patternUnits="userSpaceOnUse"><rect width="3" height="1" fill="var(--color-surface)" /></pattern>
            </defs>
            {GRID.map((k) => (
              <g key={k}>
                <line x1={L} x2={w - R} y1={Y((top * k) / 4)} y2={Y((top * k) / 4)} stroke="var(--color-line)" strokeOpacity={k ? 0.55 : 1} />
                <text x={L - 6} y={Y((top * k) / 4)} dy="0.32em" textAnchor="end" className="fill-ink-3 font-mono text-[10.5px]">{k % 2 === 0 || (h > 150 && (unit !== "count" || Number.isInteger(top / 4))) ? fmt((top * k) / 4, unit) : ""}</text>
              </g>
            ))}
            {ticksX(from, to, w < 420 ? 3 : 6).map((t) => (
              <text key={t.f} x={L + pw * t.f} y={h - 4} textAnchor="middle" className="fill-ink-3 font-mono text-[10.5px]">{t.label}</text>
            ))}
            {kind === "bars" && lines.map((l, k) => l.points.map((v, i) => {
              if (v <= 0) return null;
              const below = lines.slice(0, k).reduce((a, x) => a + x.points[i], 0);
              const bw = pw / n;
              return <rect key={`${k}-${i}`} x={L + bw * i + 1} y={Y(below + v)} width={Math.max(1, bw - 2)} height={Math.max(1, Y(below) - Y(below + v) - 1)} rx={1} fill={l.color} />;
            }))}
            {kind === "stacked" && lines.map((l, k) => {
              const base = Array.from({ length: n }, (_, i) => lines.slice(0, k).reduce((a, x) => a + x.points[i], 0));
              const up = path(base.map((b, i) => b + l.points[i]));
              const down = base.map((b, i) => `L${X(i).toFixed(1)},${(Y(b) - (k ? 1.5 : 0)).toFixed(1)}`).reverse().join(" ");
              return (
                <g key={l.name}>
                  <path d={`${up} ${down} Z`} fill={l.color} />
                  {l.hatch && <path d={`${up} ${down} Z`} fill={`url(#${hid})`} />}
                </g>
              );
            })}
            {kind === "line" && lines.map((l) => (
              <g key={l.name}>
                <path d={path(l.points)} fill="none" stroke={l.color} strokeWidth={1.75} strokeLinejoin="round" strokeLinecap="round" />
                {hi === null && !drag && (
                  <>
                    <circle cx={X(n - 1)} cy={Y(l.points[n - 1])} r={6} fill={l.color} fillOpacity={0.22} />
                    <circle cx={X(n - 1)} cy={Y(l.points[n - 1])} r={2.75} fill={l.color} />
                  </>
                )}
              </g>
            ))}
            {marker && (
              <g>
                <line x1={L} x2={w - R} y1={Y(marker.value)} y2={Y(marker.value)} stroke="var(--color-critical)" strokeWidth={1.25} />
                <text x={L + 4} y={Y(marker.value) - 5} className="fill-critical-text font-sans text-[11px]">{marker.label}</text>
              </g>
            )}
            {drag && Math.abs(drag.b - drag.a) > 0.005 && (() => {
              const a = L + pw * Math.min(drag.a, drag.b), b = L + pw * Math.max(drag.a, drag.b);
              return (
                <g>
                  <rect x={L} y={T} width={a - L} height={ph} fill="var(--color-surface)" fillOpacity={0.72} />
                  <rect x={b} y={T} width={w - R - b} height={ph} fill="var(--color-surface)" fillOpacity={0.72} />
                  {[a, b].map((x) => <g key={x}><line x1={x} x2={x} y1={T} y2={T + ph} stroke="var(--color-ink)" /><rect x={x - 2.5} y={T + ph / 2 - 9} width={5} height={18} rx={2.5} fill="var(--color-ink)" /></g>)}
                </g>
              );
            })()}
            {hi !== null && !drag && (
              <g>
                <line x1={X(hi)} x2={X(hi)} y1={T} y2={T + ph} stroke="var(--color-ink-3)" />
                {kind === "line" && lines.map((l) => <circle key={l.name} cx={X(hi)} cy={Y(l.points[hi])} r={3.5} fill={l.color} stroke="var(--color-surface)" strokeWidth={2} />)}
              </g>
            )}
          </svg>
        )}
        {hi !== null && !drag && w > 0 && (
          <div className="pointer-events-none absolute top-2 z-10 min-w-[180px] rounded-control border border-line-strong bg-surface px-2.5 py-2 text-xs text-ink-2 shadow-raised" style={X(hi) < w * 0.55 ? { left: X(hi) + 12 } : { right: w - X(hi) + 12 }}>
            <div className="mb-1 font-mono text-ink-3">{stamp(from + (hi / Math.max(1, n - 1)) * (to - from), { date: true })}</div>
            {lines.map((l) => (
              <div key={l.name} className="flex items-center gap-1.5">
                <span className="size-2 shrink-0 rounded-[2px]" style={{ background: l.color }} />
                <span className="truncate">{l.name}</span>
                <b className="ml-auto pl-3 font-mono font-medium text-ink">{fmt(l.points[hi], unit)}</b>
              </div>
            ))}
          </div>
        )}
      </div>
      {side && (
        <table className="w-[38%] max-w-[460px] shrink-0 self-start border-collapse text-xs">
          <thead>
            <tr className="text-ink-3">
              <th className="border-b border-line pb-1.5 text-left font-medium">Series</th>
              {["Avg", "Max", "Now"].map((c) => <th key={c} className="border-b border-line pb-1.5 pl-3 text-right font-medium">{c}</th>)}
            </tr>
          </thead>
          <tbody>
            {lines.map((l) => {
              const at = hi ?? l.points.length - 1;
              return (
                <tr key={l.name}>
                  <td className="max-w-0 border-b border-line py-1.5 text-ink-2">
                    <span className="flex items-center gap-1.5">
                      <span className="size-2 shrink-0 rounded-[2px]" style={{ backgroundColor: l.color, backgroundImage: l.hatch ? "repeating-linear-gradient(0deg, var(--color-surface) 0 1px, transparent 1px 3px)" : undefined }} />
                      <span className="truncate">{l.name}</span>
                    </span>
                  </td>
                  <td className="border-b border-line py-1.5 pl-3 text-right font-mono text-ink-2">{fmt(l.points.reduce((a, b) => a + b, 0) / l.points.length, unit)}</td>
                  <td className="border-b border-line py-1.5 pl-3 text-right font-mono text-ink-2">{fmt(Math.max(...l.points), unit)}</td>
                  <td className="border-b border-line py-1.5 pl-3 text-right font-mono font-medium text-ink">{fmt(l.points[at], unit)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      {!side && legend && lines.length > 1 && (
        <div className="mt-1.5 flex flex-wrap gap-x-3.5 gap-y-1 text-xs text-ink-2">
          {lines.map((l) => (
            <span key={l.name} className="inline-flex items-center gap-1.5">
              <span className="size-2 shrink-0 rounded-[2px]" style={{ backgroundColor: l.color, backgroundImage: l.hatch ? "repeating-linear-gradient(0deg, var(--color-surface) 0 1px, transparent 1px 3px)" : undefined }} />
              {l.name}
              <b className="font-mono font-medium text-ink">{fmt(tail(l), unit)}</b>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
