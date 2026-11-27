"use client";

import { useEffect, useRef, useState } from "react";
import { X } from "lucide-react";
import { stamp, sameDay, DAY, MIN } from "../model/timerange.ts";

/* The Event explorer's chart: events over time, as bars on an axis. Hovering a bar shows what is in it. Clicking a bar
   shows only that bar's events; clicking its red or violet part also filters to failed or ok events; dragging selects
   a span of time. */

export type Seg = "ok" | "err";
type Bucket = { ok: number; err: number };

const H = 64;

/* The top of the scale: the next round number at or above the tallest bar. */
function niceMax(n: number) {
  if (n <= 4) return 4;
  const p = 10 ** Math.floor(Math.log10(n));
  return [1, 1.5, 2, 3, 4, 5, 6, 8, 10].map((m) => m * p).find((v) => v >= n)!;
}

/* steps: the bars count recorded steps (lib/step-chart.ts) with no status, so there is one colour and no ok or failed to pick. */
export function EventChart({ buckets, from, to, span, per, win, onPick, steps = false }: {
  buckets: Bucket[];
  from: number;
  to: number;
  span: number;
  per: string;
  win: [number, number] | null;
  onPick: (win: [number, number] | null, seg?: Seg) => void;
  steps?: boolean;
}) {
  const [hover, setHover] = useState<{ i: number; seg?: Seg } | null>(null);
  const [drag, setDrag] = useState<{ a: number; b: number; seg?: Seg } | null>(null);
  const dragRef = useRef(drag);
  dragRef.current = drag;
  const n = buckets.length;
  const top = niceMax(Math.max(...buckets.map((b) => b.ok + b.err)));
  const inWin = (i: number) => !!win && from + i * span >= win[0] && from + (i + 1) * span <= win[1];
  const inDrag = (i: number) => !!drag && i >= Math.min(drag.a, drag.b) && i <= Math.max(drag.a, drag.b);

  useEffect(() => {
    const up = () => {
      const d = dragRef.current;
      if (!d) return;
      setDrag(null);
      const lo = Math.min(d.a, d.b);
      const hi = Math.max(d.a, d.b);
      const next: [number, number] = [from + lo * span, from + (hi + 1) * span];
      const same = !!win && win[0] === next[0] && win[1] === next[1];
      if (same && !d.seg) onPick(null);
      else onPick(next, lo === hi ? d.seg : undefined);
    };
    window.addEventListener("mouseup", up);
    return () => window.removeEventListener("mouseup", up);
  }, [from, span, win, onPick]);

  /* Times carry the date when the range crosses midnight, and seconds when the bars are finer than a minute. */
  const dated = !sameDay(from, to);
  const clock = (ms: number) => stamp(ms, { date: dated, seconds: span < MIN });
  const tick = (ms: number) => (to - from > 2 * DAY ? stamp(ms, { date: true }).split(",")[0] : stamp(ms, { date: dated, seconds: span < MIN }));
  const tip = hover ? buckets[hover.i] : null;
  /* About five ticks, each on the edge of a bar. */
  const every = Math.max(1, Math.round(n / 4));
  const ticks = Array.from({ length: Math.floor((n - 1) / every) + 1 }, (_, k) => (k * every) / n);

  return (
    <div>
      <div className="mb-1.5 flex h-5 items-center justify-between gap-3 text-xs text-ink-3">
        <span>{steps ? "Recorded steps" : "Events"} per {per}</span>
        {win && (
          <span className="inline-flex items-center gap-1.5 rounded-md bg-accent/12 py-0.5 pr-1 pl-2 font-mono text-micro text-accent">
            {clock(win[0])} to {clock(win[1])}
            <button onClick={() => onPick(null)} aria-label="Show the whole range again" className="rounded p-0.5 hover:bg-accent/20"><X size={11} /></button>
          </span>
        )}
      </div>
      <div className="grid grid-cols-[26px_minmax(0,1fr)] gap-x-2">
        <div className="relative font-mono text-micro text-ink-3" style={{ height: H }} aria-hidden>
          <span className="absolute -top-1.5 right-0">{top}</span>
          <span className="absolute top-1/2 right-0 -translate-y-1/2">{top / 2}</span>
          <span className="absolute right-0 -bottom-1.5">0</span>
        </div>
        <div className="relative border-b border-line-strong" style={{ height: H }} onMouseLeave={() => setHover(null)}>
          <i className="pointer-events-none absolute inset-x-0 top-0 border-t border-dashed border-line" />
          <i className="pointer-events-none absolute inset-x-0 top-1/2 border-t border-dashed border-line" />
          <div className="absolute inset-0 flex items-stretch select-none" role="group" aria-label={`${steps ? "Recorded steps" : "Events"} per ${per}. Select a bar to show its events.`}>
            {buckets.map((b, i) => {
              const total = b.ok + b.err;
              const picked = inWin(i) || inDrag(i);
              const dim = (hover && hover.i !== i && !drag) || ((win || drag) && !picked);
              const start = from + i * span;
              return (
                <div
                  key={i}
                  role="button"
                  tabIndex={total ? 0 : -1}
                  aria-label={steps ? `${clock(start)}, ${total} recorded steps` : `${clock(start)}, ${total} events, ${b.err} failed`}
                  aria-pressed={inWin(i)}
                  className={`group/bar relative min-w-0 flex-1 cursor-pointer outline-none ${total ? "" : "cursor-default"}`}
                  onMouseEnter={() => { setHover({ i }); if (dragRef.current) setDrag({ ...dragRef.current, b: i }); }}
                  onMouseOver={(e) => setHover({ i, seg: ((e.target as HTMLElement).dataset.seg as Seg) || undefined })}
                  onMouseDown={(e) => { e.preventDefault(); setDrag({ a: i, b: i, seg: ((e.target as HTMLElement).dataset.seg as Seg) || undefined }); }}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onPick(inWin(i) && win![1] - win![0] === span ? null : [start, start + span]); } }}
                >
                  <span className={`absolute inset-x-0 -top-1 bottom-0 rounded-t-[3px] transition-colors duration-100 ${picked ? "bg-accent/12" : "group-hover/bar:bg-ink/6 group-focus-visible/bar:bg-ink/10"}`} />
                  <span className={`absolute inset-x-[1.5px] bottom-0 flex flex-col-reverse gap-px transition-opacity duration-100 ${dim ? "opacity-40" : ""}`}>
                    {b.ok > 0 && <i data-seg={steps ? undefined : "ok"} className="hist-seg block rounded-[1.5px] bg-brand/70 transition-[filter] duration-100 hover:brightness-125" style={{ height: Math.max(2, (b.ok / top) * H), animationDelay: `${i * 6}ms` }} />}
                    {b.err > 0 && <i data-seg="err" className="hist-seg block rounded-[1.5px] bg-critical transition-[filter] duration-100 hover:brightness-125" style={{ height: Math.max(2, (b.err / top) * H), animationDelay: `${i * 6}ms` }} />}
                  </span>
                </div>
              );
            })}
          </div>
          {hover && tip && !drag && (
            <div
              role="tooltip"
              /* Below the axis, over the list: there is no room above the chart at the top of the page. */
              className="pointer-events-none absolute top-full z-30 mt-7 w-max rounded-lg border border-line-strong bg-surface px-3 py-2 text-xs shadow-panel"
              style={hover.i < n * 0.15 ? { left: `${(hover.i / n) * 100}%` } : hover.i > n * 0.85 ? { right: `${(1 - (hover.i + 1) / n) * 100}%` } : { left: `${((hover.i + 0.5) / n) * 100}%`, transform: "translateX(-50%)" }}
            >
              <p className="font-mono text-micro text-ink-3">{clock(from + hover.i * span)} to {clock(from + (hover.i + 1) * span)}</p>
              <p className="mt-1 font-medium text-ink"><span className="font-mono">{tip.ok + tip.err}</span> {steps ? (tip.ok + tip.err === 1 ? "recorded step" : "recorded steps") : tip.ok + tip.err === 1 ? "event" : "events"}</p>
              {!steps && <><p className={`mt-1 flex items-center gap-1.5 ${hover.seg === "ok" ? "text-ink" : "text-ink-2"}`}><i className="block size-2 rounded-[2px] bg-brand/70" /><span className="font-mono">{tip.ok}</span> ok</p>
              <p className={`flex items-center gap-1.5 ${hover.seg === "err" ? "text-ink" : "text-ink-2"}`}><i className="block size-2 rounded-[2px] bg-critical" /><span className="font-mono">{tip.err}</span> failed</p></>}
              {tip.ok + tip.err > 0 && (
                <p className="mt-1.5 border-t border-line pt-1.5 text-ink-3">
                  {hover.seg === "err" ? "Click to show the failed events." : hover.seg === "ok" ? "Click to show the ok events." : "Click to show these events."} Drag to select a span.
                </p>
              )}
            </div>
          )}
        </div>
        <span />
        <div className="relative h-5 font-mono text-micro text-ink-3" aria-hidden>
          {ticks.map((p, i) => (
            <span key={p} className="absolute top-0 flex flex-col" style={i === 0 ? { left: 0 } : { left: `${p * 100}%`, transform: "translateX(-50%)", alignItems: "center" }}>
              <i className="block h-1 border-l border-line-strong" />
              {tick(from + Math.round(p * n) * span)}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}
