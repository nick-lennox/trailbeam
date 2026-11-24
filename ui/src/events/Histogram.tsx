/*
  The Event explorer's chart: recorded steps per bucket over the whole range, one series. Hovering a bar shows its
  steps by kind; clicking a bar, or dragging across several, narrows the list to that span (the chart keeps the range).
*/
import { useEffect, useRef, useState } from "react";
import { kindLabel, type Bar } from "./model.ts";
import { STEP_KINDS } from "./types.ts";

const H = 64;

/** The top of the scale: the next round number at or above the tallest bar. */
export function niceMax(n: number): number {
  if (n <= 4) return 4;
  const p = 10 ** Math.floor(Math.log10(n));
  return [1, 1.5, 2, 3, 4, 5, 6, 8, 10].map((m) => m * p).find((v) => v >= n)!;
}

const utc = (ms: number, withDate: boolean, seconds = false) => {
  const iso = new Date(ms).toISOString();
  const time = iso.slice(11, seconds ? 19 : 16);
  return withDate ? `${iso.slice(5, 10)} ${time}` : time;
};

export type HistogramProps = {
  bars: Bar[];
  per: string;
  /** the picked bars, as indexes into `bars` (both included), or null */
  picked: [number, number] | null;
  onPick: (span: [number, number] | null) => void;
};

export function StepHistogram({ bars, per, picked, onPick }: HistogramProps) {
  const [hover, setHover] = useState<number | null>(null);
  const [drag, setDrag] = useState<[number, number] | null>(null);
  const dragRef = useRef(drag);
  dragRef.current = drag;
  const n = bars.length;
  const top = niceMax(Math.max(0, ...bars.map((b) => b.total)));
  const dated = n > 0 && new Date(bars[0].start).toISOString().slice(0, 10) !== new Date(bars[n - 1].end - 1).toISOString().slice(0, 10);
  const fine = n > 0 && bars[0].end - bars[0].start < 60_000;
  const within = (i: number, s: [number, number] | null) => !!s && i >= Math.min(s[0], s[1]) && i <= Math.max(s[0], s[1]);

  useEffect(() => {
    const up = () => {
      const d = dragRef.current;
      if (!d) return;
      setDrag(null);
      const span: [number, number] = [Math.min(d[0], d[1]), Math.max(d[0], d[1])];
      onPick(picked && picked[0] === span[0] && picked[1] === span[1] ? null : span);
    };
    window.addEventListener("mouseup", up);
    return () => window.removeEventListener("mouseup", up);
  }, [picked, onPick]);

  const every = Math.max(1, Math.round(n / 4));
  const ticks = n ? Array.from({ length: Math.floor((n - 1) / every) + 1 }, (_, k) => k * every) : [];
  const tip = hover === null ? null : bars[hover];

  return (
    <div data-histogram>
      <p className="mb-1.5 text-xs text-ink-3">Recorded steps per {per}</p>
      <div className="grid grid-cols-[32px_minmax(0,1fr)] gap-x-2">
        <div className="relative font-mono text-micro text-ink-3" style={{ height: H }} aria-hidden>
          <span className="absolute -top-1.5 right-0">{top.toLocaleString("en-US")}</span>
          <span className="absolute right-0 -bottom-1.5">0</span>
        </div>
        <div className="relative border-b border-line-strong" style={{ height: H }} onMouseLeave={() => setHover(null)}>
          <i className="pointer-events-none absolute inset-x-0 top-0 border-t border-dashed border-line" />
          <div className="absolute inset-0 flex items-stretch select-none" role="group" aria-label={`Recorded steps per ${per}. Select a bar to list its steps.`}>
            {bars.map((b, i) => {
              const on = within(i, picked) || within(i, drag);
              const dim = (picked || drag) && !on;
              return (
                <div key={b.start} role="button" tabIndex={b.total ? 0 : -1} aria-pressed={within(i, picked)}
                  aria-label={`${utc(b.start, dated, fine)}, ${b.total} recorded steps`}
                  className={`group/bar relative min-w-0 flex-1 outline-none ${b.total ? "cursor-pointer" : "cursor-default"}`}
                  onMouseEnter={() => { setHover(i); if (dragRef.current) setDrag([dragRef.current[0], i]); }}
                  onMouseDown={(e) => { e.preventDefault(); setDrag([i, i]); }}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onPick(picked && picked[0] === i && picked[1] === i ? null : [i, i]); } }}>
                  <span className={`absolute inset-x-0 -top-1 bottom-0 rounded-t-[3px] ${on ? "bg-brand/15" : "group-hover/bar:bg-ink/10 group-focus-visible/bar:bg-ink/15"}`} />
                  {b.total > 0 && (
                    <i className={`absolute inset-x-px bottom-0 block rounded-t-[2px] bg-series-1 ${dim ? "opacity-40" : ""}`} style={{ height: Math.max(2, (b.total / top) * H) }} />
                  )}
                </div>
              );
            })}
          </div>
          {tip && !drag && (
            <div role="tooltip" className="pointer-events-none absolute top-full z-30 mt-7 w-max rounded-lg border border-line-strong bg-surface px-3 py-2 text-xs text-ink-2 shadow-lg"
              style={hover! < n * 0.2 ? { left: `${(hover! / n) * 100}%` } : hover! > n * 0.8 ? { right: `${(1 - (hover! + 1) / n) * 100}%` } : { left: `${((hover! + 0.5) / n) * 100}%`, transform: "translateX(-50%)" }}>
              <p className="font-mono text-micro text-ink-3">{utc(tip.start, dated, fine)} to {utc(tip.end, dated, fine)} UTC</p>
              <p className="mt-1 font-medium text-ink"><span className="font-mono">{tip.total.toLocaleString("en-US")}</span> {tip.total === 1 ? "recorded step" : "recorded steps"}</p>
              {tip.total > 0 && (
                <ul className="mt-1 grid grid-cols-[auto_auto] gap-x-3">
                  {STEP_KINDS.filter((k) => tip.counts[k]).map((k) => (
                    <li key={k} className="contents"><span>{kindLabel(k)}</span><span className="text-right font-mono tabular-nums">{tip.counts[k].toLocaleString("en-US")}</span></li>
                  ))}
                </ul>
              )}
              {tip.total > 0 && <p className="mt-1.5 border-t border-line pt-1.5 text-ink-3">Click to list these steps. Drag to pick a span.</p>}
            </div>
          )}
        </div>
        <span />
        <div className="relative h-5 font-mono text-micro text-ink-3" aria-hidden>
          {ticks.map((i, k) => (
            <span key={i} className="absolute top-0 flex flex-col" style={k === 0 ? { left: 0 } : i / n > 0.9 ? { right: 0, alignItems: "flex-end" } : { left: `${(i / n) * 100}%`, transform: "translateX(-50%)", alignItems: "center" }}>
              <i className="block h-1 border-l border-line-strong" />
              {utc(bars[i].start, dated, fine)}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}
