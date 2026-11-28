"use client";

import { useEffect, useRef, useState } from "react";
import { Tooltip } from "../design/Tooltip.tsx";
import { stamp, sameDay, DAY, MIN } from "../model/timerange.ts";

/* The Event explorer's chart: events over time, as bars on an axis. Hovering a bar shows what is in it. Clicking a bar
   makes the page's time range that bar's; clicking its red or violet part also filters to failed or ok events; dragging
   makes the page's time range the span dragged over. The page's one time range is the time picker's: the chart sets it
   through onPick and keeps no range of its own. */

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
export function EventChart({ buckets, from, to, span, per, onPick, steps = false }: {
  buckets: Bucket[];
  from: number;
  to: number;
  span: number;
  per: string;
  /** the span of time picked, [from, to) in ms, and the part of a single bar clicked */
  onPick: (win: [number, number], seg?: Seg) => void;
  steps?: boolean;
}) {
  const [hover, setHover] = useState<{ i: number; seg?: Seg } | null>(null);
  const [drag, setDrag] = useState<{ a: number; b: number; seg?: Seg } | null>(null);
  const dragRef = useRef(drag);
  dragRef.current = drag;
  const n = buckets.length;
  const top = niceMax(Math.max(...buckets.map((b) => b.ok + b.err)));
  const inDrag = (i: number) => !!drag && i >= Math.min(drag.a, drag.b) && i <= Math.max(drag.a, drag.b);

  useEffect(() => {
    const up = () => {
      const d = dragRef.current;
      if (!d) return;
      setDrag(null);
      const lo = Math.min(d.a, d.b);
      const hi = Math.max(d.a, d.b);
      onPick([from + lo * span, Math.min(to, from + (hi + 1) * span)], lo === hi ? d.seg : undefined);
    };
    window.addEventListener("mouseup", up);
    return () => window.removeEventListener("mouseup", up);
  }, [from, to, span, onPick]);

  /* Times carry the date when the range crosses midnight, and seconds when the bars are finer than a minute. */
  const dated = !sameDay(from, to);
  const clock = (ms: number) => stamp(ms, { date: dated, seconds: span < MIN });
  const tick = (ms: number) => (to - from > 2 * DAY ? stamp(ms, { date: true }).split(",")[0] : stamp(ms, { date: dated, seconds: span < MIN }));
  const plural = (n: number) => (steps ? (n === 1 ? "recorded step" : "recorded steps") : n === 1 ? "event" : "events");
  /* The hover note, drawn by the design system's Tooltip on the bar under the pointer. */
  const note = (i: number) => {
    const b = buckets[i];
    const seg = hover?.i === i ? hover.seg : undefined;
    return (
      <span className="block text-left">
        <span className="block text-micro text-ink-3"><span className="font-mono">{clock(from + i * span)}</span> to <span className="font-mono">{clock(Math.min(to, from + (i + 1) * span))}</span></span>
        <span className="mt-1 block font-medium text-ink"><span className="font-mono">{b.ok + b.err}</span> {plural(b.ok + b.err)}</span>
        {!steps && <>
          <span className={`mt-1 flex items-center gap-1.5 ${seg === "ok" ? "text-ink" : "text-ink-2"}`}><i className="block size-2 rounded-[2px] bg-brand/70" /><span className="font-mono">{b.ok}</span> ok</span>
          <span className={`flex items-center gap-1.5 ${seg === "err" ? "text-ink" : "text-ink-2"}`}><i className="block size-2 rounded-[2px] bg-critical" /><span className="font-mono">{b.err}</span> failed</span>
        </>}
        {b.ok + b.err > 0 && (
          <span className="mt-1.5 block border-t border-line pt-1.5 text-ink-3">
            {seg === "err" ? "Click to show the failed events." : seg === "ok" ? "Click to show the ok events." : "Click to show these events."} Drag to set the time range.
          </span>
        )}
      </span>
    );
  };
  /* About five ticks, each on the edge of a bar. */
  const every = Math.max(1, Math.round(n / 4));
  const ticks = Array.from({ length: Math.floor((n - 1) / every) + 1 }, (_, k) => (k * every) / n);

  return (
    <div>
      <div className="mb-1.5 flex h-5 items-center text-xs text-ink-3">
        <span>{steps ? "Recorded steps" : "Events"} per {per}</span>
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
          <div className="absolute inset-0 flex items-stretch select-none" role="group" aria-label={`${steps ? "Recorded steps" : "Events"} per ${per}. Select a bar to show its time range.`}>
            {buckets.map((b, i) => {
              const total = b.ok + b.err;
              const picked = inDrag(i);
              const dim = (hover && hover.i !== i && !drag) || (drag && !picked);
              const start = from + i * span;
              return (
                <div
                  key={i}
                  role="button"
                  tabIndex={total ? 0 : -1}
                  aria-label={steps ? `${clock(start)}, ${total} recorded steps` : `${clock(start)}, ${total} events, ${b.err} failed`}
                  className={`group/bar relative min-w-0 flex-1 cursor-pointer outline-none ${total ? "" : "cursor-default"}`}
                  onMouseEnter={() => { setHover({ i }); if (dragRef.current) setDrag({ ...dragRef.current, b: i }); }}
                  onMouseOver={(e) => setHover({ i, seg: ((e.target as HTMLElement).dataset.seg as Seg) || undefined })}
                  onMouseDown={(e) => { e.preventDefault(); setDrag({ a: i, b: i, seg: ((e.target as HTMLElement).dataset.seg as Seg) || undefined }); }}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onPick([start, Math.min(to, start + span)]); } }}
                >
                  <span className={`absolute inset-x-0 -top-1 bottom-0 rounded-t-[3px] transition-colors duration-100 ${picked ? "bg-accent/12" : "group-hover/bar:bg-ink/6 group-focus-visible/bar:bg-ink/10"}`} />
                  <Tooltip content={!drag && total > 0 && note(i)} className="absolute inset-0">
                  <span className={`absolute inset-x-[1.5px] bottom-0 flex flex-col-reverse gap-px transition-opacity duration-100 ${dim ? "opacity-40" : ""}`}>
                    {b.ok > 0 && <i data-seg={steps ? undefined : "ok"} className="hist-seg block rounded-[1.5px] bg-brand/70 transition-[filter] duration-100 hover:brightness-125" style={{ height: Math.max(2, (b.ok / top) * H), animationDelay: `${i * 6}ms` }} />}
                    {b.err > 0 && <i data-seg="err" className="hist-seg block rounded-[1.5px] bg-critical transition-[filter] duration-100 hover:brightness-125" style={{ height: Math.max(2, (b.err / top) * H), animationDelay: `${i * 6}ms` }} />}
                  </span>
                  </Tooltip>
                </div>
              );
            })}
          </div>
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
