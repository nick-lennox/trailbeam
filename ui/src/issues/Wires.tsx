/*
  Curved, dashed wires across one gap of a flow drawing, for IssueFlow. Moved from the legacy Overview's
  FlowDrawing when that was removed (QV-710); the Overview's own wires are OverviewFlows'.
*/
import { useLayoutEffect, useRef, useState } from "react";

/* An element's content width or height, kept up to date as it changes (0 until it is measured). */
function useSize<T extends Element>(side: "width" | "height") {
  const ref = useRef<T>(null);
  const [n, setN] = useState(0);
  useLayoutEffect(() => {
    if (!ref.current || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(([e]) => setN(e.contentRect[side]));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, [side]);
  return [ref, n] as const;
}

/* Curved, dashed connectors across one gap of the drawing. Dashes move the way messages do (not under
   prefers-reduced-motion). A wire's pace sits beside it where it leaves or meets a node, on the side away from
   its bend, so the wire never crosses its own label. */
export type Line = { key: string; y1: number; y2: number; color: string; rate?: { n: string; bytes?: string; at: "start" | "end" } };
export function Wires({ lines, height }: { lines: Line[]; height: number }) {
  const [ref, w] = useSize<HTMLDivElement>("width");
  const halo = { paintOrder: "stroke" as const, stroke: "var(--color-canvas)", strokeWidth: 4, strokeLinejoin: "round" as const };
  return (
    <div ref={ref} style={{ height }}>
      {w > 0 && (
        <svg width={w} height={height} className="block overflow-visible" aria-hidden>
          {lines.map((l) => {
            const end = w - 7;
            const d = `M0 ${l.y1} C${end * 0.5} ${l.y1} ${end * 0.5} ${l.y2} ${end} ${l.y2}`;
            const r = l.rate;
            const x = r?.at === "end" ? w - 12 : 8;
            const yy = r?.at === "end" ? l.y2 : l.y1;
            const other = r?.at === "end" ? l.y1 : l.y2;
            const below = Math.abs(other - yy) > 1 && other < yy;
            const top = below ? yy + 15 : yy - 21;
            return (
              <g key={l.key}>
                <path d={d} fill="none" stroke={l.color} strokeWidth={1.5} strokeDasharray="5 9" className="motion-safe:animate-flow-dash" />
                <path d={`M${w - 8} ${l.y2 - 4} L${w - 1} ${l.y2} L${w - 8} ${l.y2 + 4} Z`} fill={l.color} />
                {r && (
                  <text x={x} y={top} textAnchor={r.at === "end" ? "end" : "start"} className="text-[11px]" style={halo}>
                    <tspan className="fill-ink font-mono tabular-nums">{r.n}</tspan><tspan className="fill-ink-3"> messages/min</tspan>
                    {r.bytes && <><tspan x={x} dy={13} className="fill-ink-2 font-mono tabular-nums">{r.bytes}</tspan><tspan className="fill-ink-3">/min</tspan></>}
                  </text>
                )}
              </g>
            );
          })}
        </svg>
      )}
    </div>
  );
}
