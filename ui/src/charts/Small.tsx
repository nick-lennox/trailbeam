"use client";

import { fmt } from "./fmt.ts";
import { GRID, niceMax, ticksX, useSize } from "./TimeSeries.tsx";

/* The dashboards' small charts, moved from the hosted app with the dashboards (QV-591): a figure's recent shape
   (Spark), a timing's spread over time (Heatmap) and a histogram of one field (Bars). */

/* A plain line along the bottom of a number. */
/* The same figure over the time range, under a number: a line that ends in a dot at now. */
export function Spark({ points, color, className = "" }: { points: number[]; color: string; className?: string }) {
  const finite = points.filter(Number.isFinite);
  const max = Math.max(0, ...finite) || 1, min = Math.min(max, ...finite);
  const span = max - min || max || 1;
  const y = (v: number) => 6 + 88 * (1 - (Math.min(v, max) - min) / span);
  const d = points.map((v, i) => `${i ? "L" : "M"}${((300 * i) / (points.length - 1)).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  return (
    <div className={`relative shrink-0 ${className}`}>
      <svg viewBox="0 0 300 100" preserveAspectRatio="none" aria-hidden className="block size-full">
        <path d={d} fill="none" stroke={color} strokeWidth={1.75} vectorEffect="non-scaling-stroke" strokeLinejoin="round" />
      </svg>
      <span className="absolute right-0 size-1.5 translate-x-1/2 -translate-y-1/2 rounded-full" style={{ top: `${y(points[points.length - 1])}%`, background: color, boxShadow: `0 0 0 3px color-mix(in srgb, ${color} 25%, transparent)` }} />
    </div>
  );
}

/* A timing's distribution over time. rows run from the longest duration to the shortest. */
export function Heatmap({ cells, labels, from, to, line }: { cells: number[][]; labels: string[]; from: number; to: number; line?: { row: number; label: string } }) {
  const [ref, { w, h }] = useSize<HTMLDivElement>();
  const L = 46, R = 10, T = 6, B = 18;
  const cols = cells[0]?.length ?? 0, rows = cells.length;
  const cw = (w - L - R) / Math.max(1, cols), ch = (h - T - B) / Math.max(1, rows);
  const max = Math.max(1, ...cells.flat());
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div ref={ref} className="min-h-0 flex-1">
        {w > 0 && (
          <svg width={w} height={h} role="img" aria-label="Distribution over time">
            {labels.map((lb, j) => <text key={lb} x={L - 6} y={T + ch * j + ch / 2} dy="0.32em" textAnchor="end" className="fill-ink-3 font-mono text-[10.5px]">{lb}</text>)}
            {cells.map((row, j) => row.map((c, i) => c > 0 && (
              <rect key={`${i}-${j}`} x={L + cw * i + 0.75} y={T + ch * j + 0.75} width={Math.max(1, cw - 1.5)} height={Math.max(1, ch - 1.5)} rx={1.5} fill="var(--color-series-3)" fillOpacity={0.14 + 0.86 * (c / max)}>
                <title>{`${c} receives, ${labels[j]}`}</title>
              </rect>
            )))}
            {line && (
              <g>
                <line x1={L} x2={w - R} y1={T + ch * line.row} y2={T + ch * line.row} stroke="var(--color-critical)" />
                <text x={L + 4} y={line.row === 0 ? T + 13 : T + ch * line.row - 4} className="fill-ink font-sans text-[11px]">{line.label}</text>
              </g>
            )}
            {ticksX(from, to, w < 420 ? 3 : 6).map((t) => <text key={t.f} x={L + (w - L - R) * t.f} y={h - 4} textAnchor="middle" className="fill-ink-3 font-mono text-[10.5px]">{t.label}</text>)}
          </svg>
        )}
      </div>
    </div>
  );
}

/* A histogram of one field over the whole range. */
export function Bars({ bars, caption, onPick }: { bars: { label: string; value: number; critical?: boolean }[]; caption: string; onPick?: (label: string) => void }) {
  const [ref, { w, h }] = useSize<HTMLDivElement>();
  const L = 34, R = 8, T = 18, B = 30;
  const top = niceMax(Math.max(...bars.map((b) => b.value)));
  const pw = w - L - R, ph = h - T - B, bw = pw / Math.max(1, bars.length), cw = Math.min(14, Math.max(4, bw * 0.4));
  return (
    <div ref={ref} className="min-h-0 flex-1">
      {w > 0 && (
        <svg width={w} height={h} role="img" aria-label={caption}>
          {GRID.map((k) => (
            <g key={k}>
              <line x1={L} x2={w - R} y1={T + ph * (1 - k / 4)} y2={T + ph * (1 - k / 4)} stroke="var(--color-line)" strokeOpacity={k ? 0.55 : 1} />
              <text x={L - 6} y={T + ph * (1 - k / 4)} dy="0.32em" textAnchor="end" className="fill-ink-3 font-mono text-[10.5px]">{k % 2 === 0 ? fmt((top * k) / 4, "count") : ""}</text>
            </g>
          ))}
          {bars.map((b, i) => {
            const bh = b.value > 0 ? Math.max(cw, (ph * b.value) / top) : 0;
            const x = L + bw * i + bw / 2 - cw / 2;
            return (
              <g key={b.label} className={onPick ? "cursor-pointer" : ""} onClick={() => onPick?.(b.label)}>
                {bh > 0 && <rect x={x} y={T + ph - bh} width={cw} height={bh} rx={cw / 2} fill={b.critical ? "var(--color-critical)" : "var(--color-series-1)"} />}
                <text x={x + cw / 2} y={T + ph - bh - 6} textAnchor="middle" className="fill-ink font-mono text-[10.5px]">{b.value.toLocaleString("en-US")}</text>
                <text x={x + cw / 2} y={T + ph + 13} textAnchor="middle" className="fill-ink-3 font-mono text-[10.5px]">{b.label}</text>
              </g>
            );
          })}
          <text x={L + pw / 2} y={h - 3} textAnchor="middle" className="fill-ink-3 font-sans text-[11px]">{caption}</text>
        </svg>
      )}
    </div>
  );
}
