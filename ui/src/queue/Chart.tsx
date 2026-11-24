/*
  The queue page's chart (QV-360 design §4): lines over one shared time grid, in SVG with no chart library, so
  the self-hosted shell ships nothing extra. Every chart on a page gets the same `times`, so a bucket index means
  the same minute in each, and the page passes one hover index to all of them: the crosshair moves across every
  chart at once. A missing value is a gap, never a zero. The last, still-open bucket is drawn at half opacity
  (not dashed: dashes mean inferred). Colours come from role tokens (theme.css), so the host page's theme applies.
*/
import { useRef, useState, type KeyboardEvent, type PointerEvent, type ReactNode } from "react";
import { utc, spansDays } from "./model.ts";

export type ChartLine = {
  key: string;
  label: string;
  /** a CSS colour, normally var(--color-flow-…) */
  color: string;
  values: (number | null)[];
  opacity?: number;
  /** points drawn as dots and not joined (a version with too few spans for a p95) */
  dots?: boolean[];
  /** a label at the line's right end (By version) */
  endLabel?: string;
  /** left out of the tooltip and the summary (an older version's line) */
  quiet?: boolean;
};
/** A DLQ move along the bottom edge: seen (filled diamond, recorded) or worked out (hollow dashed, inferred). */
export type ChartMark = { i: number; kind: "seen" | "inferred"; n: number };
/** A horizontal rule at a value, e.g. the visibility timeout. */
export type ChartRule = { value: number; label: string };
/** A vertical hairline at a time, e.g. a deploy. */
export type ChartMarker = { t: number; label: string; title: string };
export type LegendItem = { label: string; color: string; shape?: "square" | "diamond" | "diamond-dashed" | "rule" | "marker" };

export type ChartProps = {
  title: string;
  /** the sentence under the legend */
  caption?: ReactNode;
  /** the source tag at the caption's end: spans, CloudWatch */
  source: string;
  times: number[];
  stepS: number;
  lines: ChartLine[];
  /** fill between two lines where the first is above the second (sends above handled: the backlog building) */
  band?: { upper: string; lower: string; color: string };
  marks?: ChartMark[];
  rules?: ChartRule[];
  markers?: ChartMarker[];
  /** a value as the axis and tooltip show it */
  fmt: (n: number) => string;
  /** more tooltip lines for a bucket */
  tip?: (i: number) => string[];
  legend: LegendItem[];
  /** "past hour", for the aria-label */
  summary: string;
  hover: number | null;
  onHover: (i: number | null) => void;
  sel: [number, number] | null;
  /** clicking or dragging selects buckets; leave out for a chart that can't */
  onSelect?: (sel: [number, number] | null) => void;
  /** the last bucket is still open */
  inProgress?: boolean;
};

const W = 560;
const H = 176;

function niceCeil(v: number): number {
  if (!(v > 0)) return 1;
  const pow = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]) if (m * pow >= v - 1e-9) return m * pow;
  return 10 * pow;
}

const diamond = (x: number, y: number, r: number) => `${x},${y - r} ${x + r},${y} ${x},${y + r} ${x - r},${y}`;

export function Key({ item }: { item: LegendItem }) {
  const s = item.shape ?? "square";
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden className="shrink-0">
      {s === "square" && <rect x="1" y="1" width="10" height="10" rx="2" style={{ fill: item.color }} />}
      {s === "diamond" && <polygon points={diamond(6, 6, 5)} style={{ fill: item.color }} />}
      {s === "diamond-dashed" && <polygon points={diamond(6, 6, 4.5)} fill="none" strokeDasharray="2 1.5" strokeWidth="1.2" style={{ stroke: item.color }} />}
      {s === "rule" && <line x1="0" y1="6" x2="12" y2="6" strokeWidth="1.5" style={{ stroke: item.color }} />}
      {s === "marker" && <line x1="6" y1="0" x2="6" y2="12" strokeWidth="1" style={{ stroke: item.color }} />}
    </svg>
  );
}

export function Legend({ items }: { items: LegendItem[] }) {
  return (
    <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-caption text-ink-2" aria-label="Legend">
      {items.map((it) => <li key={it.label} className="inline-flex items-center gap-1.5"><Key item={it} />{it.label}</li>)}
    </ul>
  );
}

/** A source tag: recorded (solid border) or inferred (dashed). */
export const Tag = ({ children, inferred = false }: { children: ReactNode; inferred?: boolean }) => (
  <span className={`inline-block rounded-full border px-1.5 font-mono text-micro text-ink-3 ${inferred ? "border-dashed border-ink-3" : "border-line"}`}>{children}</span>
);

/** A rule more than this many times the data's highest value stays off the scale (named at the top instead). */
const RULE_REACH = 3;

export function Chart(p: ChartProps) {
  const n = p.times.length;
  const ref = useRef<SVGSVGElement>(null);
  const [drag, setDrag] = useState<{ a: number; b: number } | null>(null);
  const labelled = p.lines.some((l) => l.endLabel);
  const stepMs = p.stepS * 1000;

  const all = p.lines.flatMap((l) => l.values.filter((v): v is number => v !== null));
  // a rule far above the data (a 4-day retention over ages of seconds) would flatten the lines: it stays off the
  // scale and is named at the top instead
  const dataMax = Math.max(0, ...all);
  const rules = p.rules ?? [];
  const onScale = rules.filter((r) => dataMax === 0 || r.value <= dataMax * RULE_REACH);
  const above = rules.filter((r) => !onScale.includes(r));
  const top = niceCeil(Math.max(dataMax, ...onScale.map((r) => r.value * 1.08)));
  // the y axis: 0, half and the top, each once (a small top can round two of them to the same label)
  const yTicks = [0, top / 2, top].filter((v, i, vs) => vs.findIndex((w) => p.fmt(w) === p.fmt(v)) === i);
  // room on the left for the longest of their labels ("1 min 30 s")
  const pad = { l: Math.max(44, 10 + 6.2 * Math.max(...yTicks.map((v) => p.fmt(v).length))), r: labelled ? 60 : 12, t: 16, b: 22 };
  const pw = W - pad.l - pad.r;
  const ph = H - pad.t - pad.b;
  const x = (i: number) => pad.l + ((i + 0.5) / Math.max(1, n)) * pw;
  const xEdge = (i: number) => pad.l + (i / Math.max(1, n)) * pw;
  const xt = (t: number) => (n ? pad.l + ((t - p.times[0]) / (n * stepMs)) * pw : pad.l);
  const y = (v: number) => pad.t + (1 - v / top) * ph;
  const last = n - 1;

  const indexAt = (clientX: number) => {
    const r = ref.current?.getBoundingClientRect();
    if (!r || !n) return null;
    const px = ((clientX - r.left) / r.width) * W;
    return Math.max(0, Math.min(last, Math.floor(((px - pad.l) / pw) * n)));
  };
  const select = (a: number, b: number) => p.onSelect?.([p.times[Math.min(a, b)], p.times[Math.max(a, b)] + stepMs]);

  const onMove = (e: PointerEvent) => {
    const i = indexAt(e.clientX);
    p.onHover(i);
    if (drag && i !== null) setDrag({ ...drag, b: i });
  };
  const onDown = (e: PointerEvent) => {
    if (!p.onSelect) return;
    const i = indexAt(e.clientX);
    if (i === null) return;
    ref.current?.setPointerCapture?.(e.pointerId);
    setDrag({ a: i, b: i });
  };
  const onUp = () => {
    if (drag) select(drag.a, drag.b);
    setDrag(null);
  };
  const onKey = (e: KeyboardEvent) => {
    if (!n) return;
    if (e.key === "ArrowLeft" || e.key === "ArrowRight") {
      e.preventDefault();
      const h = p.hover ?? (e.key === "ArrowLeft" ? n : -1);
      p.onHover(Math.max(0, Math.min(last, h + (e.key === "ArrowLeft" ? -1 : 1))));
    } else if (e.key === "Enter" && p.hover !== null && p.onSelect) {
      e.preventDefault();
      select(p.hover, p.hover);
    } else if (e.key === "Escape") {
      p.onHover(null);
      p.onSelect?.(null);
    }
  };

  // the path of a line: runs of consecutive values; the open last bucket and dots are drawn on their own
  const paths = (l: ChartLine) => {
    const runs: string[] = [];
    let cur: string[] = [];
    const vs = l.values.map((v, i) => (l.dots?.[i] ? null : v));
    const end = p.inProgress ? last - 1 : last;
    for (let i = 0; i <= end; i++) {
      const v = vs[i];
      if (v === null || v === undefined) { if (cur.length) runs.push(cur.join(" ")); cur = []; continue; }
      cur.push(`${cur.length ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`);
    }
    if (cur.length) runs.push(cur.join(" "));
    return { runs, vs };
  };

  const bandPath = () => {
    if (!p.band) return [];
    const u = p.lines.find((l) => l.key === p.band!.upper)?.values ?? [];
    const lo = p.lines.find((l) => l.key === p.band!.lower)?.values ?? [];
    const out: string[] = [];
    let run: number[] = [];
    const flush = () => {
      if (run.length > 1 && run.some((i) => (u[i] as number) > (lo[i] as number))) {
        const upper = run.map((i) => `${x(i).toFixed(1)},${y(Math.max(u[i] as number, lo[i] as number)).toFixed(1)}`);
        const lower = [...run].reverse().map((i) => `${x(i).toFixed(1)},${y(lo[i] as number).toFixed(1)}`);
        out.push(`M${upper.join(" L")} L${lower.join(" L")} Z`);
      }
      run = [];
    };
    for (let i = 0; i < n; i++) {
      if (u[i] === null || u[i] === undefined || lo[i] === null || lo[i] === undefined) flush();
      else run.push(i);
    }
    flush();
    return out;
  };

  const selIdx = (() => {
    if (drag) return [Math.min(drag.a, drag.b), Math.max(drag.a, drag.b)] as const;
    if (!p.sel) return null;
    const ins = p.times.map((t, i) => (t >= p.sel![0] && t < p.sel![1] ? i : -1)).filter((i) => i >= 0);
    return ins.length ? ([ins[0], ins[ins.length - 1]] as const) : null;
  })();

  const multiDay = n > 0 && spansDays(p.times[0], p.times[last] + stepMs);
  const ticks = n ? [...new Set([0, Math.round(n / 4), Math.round(n / 2), Math.round((3 * n) / 4)])].filter((i) => i < n) : [];
  const shown = p.lines.filter((l) => !l.quiet);
  const label = `${p.title}, ${p.summary}`;
  const hv = p.hover !== null && p.hover >= 0 && p.hover < n ? p.hover : null;

  return (
    <figure className="min-w-0 rounded-[12px] border border-line bg-surface/40 p-4">
      <figcaption className="text-[15px] text-ink">{p.title}</figcaption>
      <div className="relative mt-2">
        <div tabIndex={0} role="img" aria-label={label} onKeyDown={onKey} className="rounded-[6px] outline-offset-2">
          <svg ref={ref} viewBox={`0 0 ${W} ${H}`} className="block h-auto w-full touch-none select-none" aria-hidden
            onPointerMove={onMove} onPointerLeave={() => !drag && p.onHover(null)} onPointerDown={onDown} onPointerUp={onUp}>
            {/* the y axis: 0, half and the top, as hairlines */}
            {yTicks.map((v) => (
              <g key={v}>
                <line x1={pad.l} x2={W - pad.r} y1={y(v)} y2={y(v)} strokeWidth="1" style={{ stroke: "var(--color-line)" }} />
                <text x={pad.l - 6} y={y(v) + 3.5} textAnchor="end" fontSize="10" className="font-mono" style={{ fill: "var(--color-ink-3)" }}>{p.fmt(v)}</text>
              </g>
            ))}
            {ticks.map((i) => (
              <text key={i} x={xEdge(i)} y={H - 6} fontSize="10" className="font-mono" style={{ fill: "var(--color-ink-3)" }}>{utc(p.times[i], { date: multiDay && i === 0 })}</text>
            ))}
            {selIdx && (
              <rect x={xEdge(selIdx[0])} y={pad.t} width={xEdge(selIdx[1] + 1) - xEdge(selIdx[0])} height={ph} style={{ fill: "var(--color-ink)", fillOpacity: 0.06 }} />
            )}
            {bandPath().map((d, i) => <path key={i} d={d} style={{ fill: p.band!.color, fillOpacity: 0.12 }} />)}
            {onScale.map((r) => (
              <g key={r.label}>
                <line x1={pad.l} x2={W - pad.r} y1={y(r.value)} y2={y(r.value)} strokeWidth="1.2" style={{ stroke: "var(--color-line-strong)" }} />
                <text x={W - pad.r} y={y(r.value) - 4} textAnchor="end" fontSize="10" className="font-mono" style={{ fill: "var(--color-ink-3)" }}>{r.label}</text>
              </g>
            ))}
            {(p.markers ?? []).map((m) => (
              <g key={`${m.t}-${m.label}`}>
                <title>{m.title}</title>
                <line x1={xt(m.t)} x2={xt(m.t)} y1={pad.t - 4} y2={pad.t + ph} strokeWidth="1" style={{ stroke: "var(--color-ink-3)" }} />
                {/* the label reads away from the right edge, so a late deploy's name isn't cut off */}
                <text x={xt(m.t) + (xt(m.t) > W * 0.6 ? -3 : 3)} y={pad.t - 6} textAnchor={xt(m.t) > W * 0.6 ? "end" : "start"} fontSize="9.5" className="font-mono" style={{ fill: "var(--color-ink-3)" }}>{m.label}</text>
              </g>
            ))}
            {p.lines.map((l) => {
              const { runs, vs } = paths(l);
              const o = l.opacity ?? 1;
              const lastV = l.values[last];
              const prevV = l.values[last - 1];
              const lastI = l.values.map((v, i) => (v !== null ? i : -1)).filter((i) => i >= 0).pop();
              return (
                <g key={l.key} style={{ stroke: l.color, fill: l.color }}>
                  {runs.map((d, i) => <path key={i} d={d} fill="none" strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" opacity={o} />)}
                  {/* a value with no neighbour: a dot, so it shows */}
                  {vs.map((v, i) => (v !== null && (i === 0 || vs[i - 1] === null) && (i === last || vs[i + 1] === null) && !(p.inProgress && i === last)
                    ? <circle key={i} cx={x(i)} cy={y(v)} r="2.2" stroke="none" opacity={o} /> : null))}
                  {l.dots?.map((d, i) => (d && l.values[i] !== null ? <circle key={`d${i}`} cx={x(i)} cy={y(l.values[i]!)} r="2.2" stroke="none" opacity={o} /> : null))}
                  {p.inProgress && lastV !== null && !l.dots?.[last] && (prevV !== null && prevV !== undefined && !l.dots?.[last - 1]
                    ? <path d={`M${x(last - 1)},${y(prevV)} L${x(last)},${y(lastV)}`} fill="none" strokeWidth="2" strokeLinecap="round" opacity={o * 0.5} />
                    : <circle cx={x(last)} cy={y(lastV)} r="2.2" stroke="none" opacity={o * 0.5} />)}
                  {l.endLabel && lastI !== undefined && (
                    <text x={x(lastI) + 5} y={y(l.values[lastI]!) + 3.5} fontSize="9.5" stroke="none" className="font-mono" opacity={Math.max(o, 0.7)} style={{ fill: "var(--color-ink-2)" }}>{l.endLabel}</text>
                  )}
                </g>
              );
            })}
            {(p.marks ?? []).map((m) => {
              const cy = pad.t + ph - 6;
              const cx = x(m.i) + (m.kind === "inferred" ? 3 : -3);
              return m.kind === "seen"
                ? <polygon key={`s${m.i}`} points={diamond(cx, cy, 4.5)} style={{ fill: "var(--color-flow-lost)" }} />
                : <polygon key={`i${m.i}`} points={diamond(cx, cy, 4)} fill="none" strokeDasharray="2 1.5" strokeWidth="1.2" style={{ stroke: "var(--color-flow-lost)" }} />;
            })}
            {hv !== null && <line x1={x(hv)} x2={x(hv)} y1={pad.t} y2={pad.t + ph} strokeWidth="1" style={{ stroke: "var(--color-ink-3)" }} />}
            {hv !== null && shown.map((l) => (l.values[hv] !== null ? <circle key={l.key} cx={x(hv)} cy={y(l.values[hv]!)} r="3" style={{ fill: l.color }} /> : null))}
          </svg>
        </div>
        {above.length > 0 && <p className="mt-1 text-right font-mono text-micro text-ink-3">{above.map((r) => `${r.label}: above this scale ↑`).join(" · ")}</p>}
        {hv !== null && (
          <div role="presentation" className="pointer-events-none absolute top-0 z-10 min-w-44 whitespace-nowrap rounded-[8px] border border-line bg-surface px-3 py-2 text-caption text-ink-2 shadow-sm"
            style={{ left: `${(x(hv) / W) * 100}%`, transform: x(hv) > W * 0.6 ? "translateX(calc(-100% - 10px))" : "translateX(10px)" }}>
            <p className="font-mono text-ink">{utc(p.times[hv], { date: multiDay })}–{utc(p.times[hv] + stepMs)} UTC</p>
            {p.inProgress && hv === last && <p className="text-ink-3">Minute in progress</p>}
            {shown.map((l) => (
              <p key={l.key} className="flex items-center gap-1.5">
                <Key item={{ label: l.label, color: l.color }} />{l.label}
                <span className="ml-auto pl-3 font-mono tabular-nums text-ink">{l.values[hv] === null ? "no data" : p.fmt(l.values[hv]!)}</span>
              </p>
            ))}
            {p.tip?.(hv).map((t) => <p key={t} className="text-ink-3">{t}</p>)}
          </div>
        )}
      </div>
      <Legend items={p.legend} />
      {p.caption !== undefined || p.source ? (
        <p className="mt-2 text-caption leading-[1.5] text-ink-3">{p.caption} <Tag>{p.source}</Tag></p>
      ) : null}
    </figure>
  );
}

/** A chart's frame with no data yet: the axes' baseline, no spinner (§11, Loading). */
export function ChartSkeleton({ title }: { title: string }) {
  return (
    <figure className="min-w-0 rounded-[12px] border border-line bg-surface/40 p-4" aria-busy="true">
      <figcaption className="text-[15px] text-ink">{title}</figcaption>
      <svg viewBox={`0 0 ${W} ${H}`} className="mt-2 block h-auto w-full" aria-hidden>
        <line x1={44} x2={W - 12} y1={H - 22} y2={H - 22} strokeWidth="1" style={{ stroke: "var(--color-line)" }} />
      </svg>
    </figure>
  );
}
