/*
  The Overview's open row (QV-392 design §4): one flow drawn from its Flow alone. Senders on the left, the queue
  card in the middle, receivers and the dead-letter queue on the right, joined by dashed wires that carry the
  pace (messages a minute, and bytes a minute when the sends reported a body size). Moved from the hosted app's
  OverviewFlows (Wires and the node faces). Nodes are links to the queue page, not detail toggles.
*/
import { useLayoutEffect, useRef, useState, type ComponentType, type CSSProperties, type ReactNode } from "react";
import { drawingLabel, fmtBytes, fmtNum, fmtRate, NO_RECEIVE, NO_SEND, perMinute, type Flow } from "./derive.ts";

export type LinkProps = { href: string; className?: string; children: ReactNode; title?: string; style?: CSSProperties };
export const A = ({ href, className, children, title, style }: LinkProps) => <a href={href} className={className} title={title} style={style}>{children}</a>;

/** Where a queue is: enough for a host to build its page's link (the label, and the place when the label had to be told apart). */
export type QueueRef = Pick<Flow, "queue" | "name" | "account" | "region">;
export type OverviewHrefs = {
  /** the queue's page; `part` is the section a node opens at */
  queue: (q: QueueRef, part?: "senders" | "consumers") => string;
  /** Needs attention (self-hosted: Stuck) filtered to the queue */
  stuck: (q: QueueRef) => string;
  /** the empty store's "Set up a service" (self-hosted: /) */
  setUp?: string;
};

const SEND = "var(--color-flow-send)";
const RECEIVE = "var(--color-flow-wait)";
const QUIET = "var(--color-ink-3)";
const BAD = "var(--color-critical)";

/* the small square that says which colour a word goes with */
export const Key = ({ color }: { color: string }) => <span aria-hidden className="mr-1.5 inline-block size-2 rounded-[2px] align-baseline" style={{ background: color }} />;

/* Service marks: AWS Lambda and Amazon SQS from the Simple Icons set (CC0), drawn in the current colour. */
const LAMBDA = "M4.9855 0c-.2941.0031-.5335.2466-.534.5482L4.446 5.456c0 .1451.06.2835.159.3891a.5322.5322 0 0 0 .3806.1562h3.4282l8.197 17.6805a.5365.5365 0 0 0 .4885.3181h5.811c.2969 0 .5426-.2448.5426-.5482V18.544c0-.3035-.2392-.5482-.5425-.5482h-2.0138L12.7394.3153C12.647.124 12.4564 0 12.2452 0h-7.254Zm.5397 1.0907h6.3678l8.16 17.6804a.5365.5365 0 0 0 .4885.3181h1.8178v3.8173H17.437L9.2402 5.226a.536.536 0 0 0-.4885-.318H5.5223Zm2.0137 8.2366c-.2098.0011-.3937.1193-.4857.3096L.6002 23.2133a.5506.5506 0 0 0 .0313.5282.5334.5334 0 0 0 .4544.25h6.169a.5468.5468 0 0 0 .497-.3096l3.38-7.166a.5405.5405 0 0 0-.0029-.4686L8.036 9.637a.5468.5468 0 0 0-.4942-.3096Zm.0057 1.8036 2.488 5.1522-3.1214 6.6206H1.9465Z";
const Icon = ({ lambda, color }: { lambda: boolean; color: string }) =>
  lambda ? (
    <svg width={18} height={18} viewBox="0 0 24 24" aria-hidden className="shrink-0" style={{ color }}><path fill="currentColor" d={LAMBDA} /></svg>
  ) : (
    <svg viewBox="0 0 20 20" width={18} height={18} fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" aria-hidden className="shrink-0" style={{ color }}>
      <rect x="3" y="3.5" width="14" height="5" rx="1.5" /><rect x="3" y="11.5" width="14" height="5" rx="1.5" /><path d="M6 6h.01M6 14h.01" />
    </svg>
  );

function useWidth<T extends Element>() {
  const ref = useRef<T>(null);
  const [w, setW] = useState(0);
  useLayoutEffect(() => {
    if (!ref.current || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(([e]) => setW(e.contentRect.width));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return [ref, w] as const;
}

/* Curved, dashed connectors across one gap of the drawing. Dashes move the way messages do (not under
   prefers-reduced-motion). A wire's pace sits beside it where it leaves or meets a node, on the side away from
   its bend, so the wire never crosses its own label. */
export type Line = { key: string; y1: number; y2: number; color: string; rate?: { n: string; bytes?: string; at: "start" | "end" } };
export function Wires({ lines, height }: { lines: Line[]; height: number }) {
  const [ref, w] = useWidth<HTMLDivElement>();
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

export type FlowDrawingProps = {
  flow: Flow;
  /** every flow, to find the dead-letter queue's own place */
  flows: Flow[];
  range: { fromMs: number; toMs: number };
  href: OverviewHrefs;
  Link?: ComponentType<LinkProps>;
  /** the host's queue card slot, under Sent and Received (hosted: depth, age and settings from AWS) */
  queueCard?: ReactNode;
  /** the host says the dead-letter queue is growing: its wire takes the critical colour */
  dlqBad?: boolean;
  /** what the host's nav calls Needs attention: "Stuck" in self-hosted Trailbeam */
  stuckWord: string;
  /** the id the open row's chevron points at */
  id?: string;
};

const PITCH = 76;
const node = "absolute inset-x-0 block h-14 -translate-y-1/2 rounded-lg border border-line-strong bg-surface px-3 text-left hover:border-ink-3";

export function FlowDrawing({ flow: f, flows, range, href, Link = A, queueCard, dlqBad = false, stuckWord, id }: FlowDrawingProps) {
  const rate = (n: number) => perMinute(n, range.fromMs, range.toMs);
  const pace = (n: number) => ({ n: fmtRate(rate(n)), bytes: f.avgBytes === null ? undefined : `${fmtBytes(rate(n) * f.avgBytes)}` });
  const ins = f.producers.filter((p) => p.n > 0);
  const outs = f.consumers.filter((c) => c.n > 0);
  const dlqRef: QueueRef | null = f.dlq ? flows.find((x) => x.queue === f.dlq!.name) ?? { queue: f.dlq.name, name: f.dlq.name, account: null, region: null } : null;
  const rights = outs.length + (f.dlq ? 1 : 0);
  const H = Math.max(200, Math.max(ins.length, rights) * PITCH + 36);
  const y = (i: number, n: number) => H / 2 + (i - (n - 1) / 2) * PITCH;
  const port = (i: number, n: number) => H / 2 + (i - (n - 1) / 2) * 22;
  const place = [f.account, f.region].filter(Boolean).join(" / ");
  const facts = [f.visibilityChanges > 0 ? `Visibility changes ${fmtNum(f.visibilityChanges)}` : null, f.avgBytes !== null ? `Average body ${fmtBytes(f.avgBytes)}` : null].filter(Boolean);
  const face = (lambda: boolean, role: string, name: string, color: string, extra?: ReactNode) => (
    <span className="flex h-full items-center gap-2.5" title={lambda ? "AWS Lambda" : undefined}>
      <span className="grid size-8 shrink-0 place-items-center rounded-md border border-line bg-canvas"><Icon lambda={lambda} color={color} /></span>
      <span className="min-w-0">
        <span className="block truncate text-xs text-ink-3"><Key color={color} />{role}</span>
        <span className="block truncate text-sm font-medium text-ink" title={name}>{name}</span>
        {extra}
      </span>
    </span>
  );
  const link = "text-brand hover:underline";

  return (
    <div id={id}>
      {/* the drawing, from 768 px */}
      <div role="img" aria-label={drawingLabel(f, range.fromMs, range.toMs)} className="hidden overflow-x-auto md:block">
        <div className="grid min-w-[760px] grid-cols-[minmax(150px,200px)_minmax(70px,1fr)_minmax(240px,320px)_minmax(70px,1fr)_minmax(150px,210px)] px-5 py-4">
          <div className="relative" style={{ height: H }}>
            {ins.length === 0 && <p className="absolute inset-x-0 top-1/2 -translate-y-1/2 text-sm text-ink-3">{NO_SEND}</p>}
            {ins.map((p, i) => (
              <Link key={p.name} href={href.queue(f, "senders")} className={node} style={{ top: y(i, ins.length) }}>{face(p.lambda, "Sender", p.name, SEND)}</Link>
            ))}
          </div>
          <Wires height={H} lines={ins.map((p, i) => ({ key: p.name, y1: y(i, ins.length), y2: port(i, ins.length), color: SEND, rate: { ...pace(p.n), at: "start" as const } }))} />
          <div className="flex items-center" style={{ height: H }}>
            <div className="w-full rounded-lg border border-line-strong bg-surface">
              <div className="flex items-baseline gap-2.5 px-4 pt-3">
                <Link href={href.queue(f)} className="min-w-0 truncate font-mono text-[15px] text-ink hover:underline">{f.queue}</Link>
                {place && <span className="ml-auto shrink-0 font-mono text-micro text-ink-3">{place}</span>}
              </div>
              <div className="grid grid-cols-2 gap-x-4 px-4 pt-2.5 pb-3">
                <div><p className="text-xs text-ink-3"><Key color={SEND} />Sent</p><p className="mt-0.5 font-mono text-[17px] text-ink tabular-nums">{fmtNum(f.sent)}</p></div>
                <div><p className="text-xs text-ink-3"><Key color={RECEIVE} />Received</p><p className="mt-0.5 font-mono text-[17px] text-ink tabular-nums">{fmtNum(f.received)}</p></div>
              </div>
              {queueCard && <div className="border-t border-line px-4 py-3">{queueCard}</div>}
            </div>
          </div>
          <Wires height={H} lines={[
            ...outs.map((c, i) => ({ key: `c:${c.name}`, y1: port(i, rights), y2: y(i, rights), color: RECEIVE, rate: { n: fmtRate(rate(c.n)), at: "end" as const } })),
            ...(f.dlq ? [{ key: "dlq", y1: port(rights - 1, rights), y2: y(rights - 1, rights), color: dlqBad ? BAD : QUIET }] : []),
          ]} />
          <div className="relative" style={{ height: H }}>
            {rights === 0 && <p className="absolute inset-x-0 top-1/2 -translate-y-1/2 text-sm text-ink-3">{NO_RECEIVE}</p>}
            {outs.map((c, i) => (
              <Link key={c.name} href={href.queue(f, "consumers")} className={node} style={{ top: y(i, rights) }}>{face(c.lambda, c.lambda ? "Receiver (Lambda)" : "Receiver", c.name, RECEIVE)}</Link>
            ))}
            {f.dlq && dlqRef && (
              <Link href={href.queue(dlqRef)} className={`${node} h-16`} style={{ top: y(rights - 1, rights) }}>
                <span className="flex h-full flex-col justify-center">
                  <span className="block truncate text-xs text-ink-3"><Key color={dlqBad ? BAD : QUIET} />Dead-letter queue</span>
                  <span className="block truncate font-mono text-sm text-ink" title={f.dlq.name}>{f.dlq.name}</span>
                  <span className="block truncate text-xs text-ink-3"><span className="font-mono tabular-nums text-ink-2">{fmtNum(f.dlq.received)}</span> received from it</span>
                </span>
              </Link>
            )}
          </div>
        </div>
      </div>

      {/* under 768 px, a list with the same figures */}
      <dl className="grid gap-1.5 px-4 py-3 text-sm md:hidden">
        <div><dt className="inline text-ink-3">Senders: </dt><dd className="inline text-ink-2">{ins.length ? ins.map((p) => `${p.name} (${fmtRate(rate(p.n))}/min)`).join(", ") : NO_SEND}</dd></div>
        <div><dt className="inline text-ink-3">Receivers: </dt><dd className="inline text-ink-2">{outs.length ? outs.map((c) => `${c.name}${c.lambda ? " (Lambda)" : ""} (${fmtRate(rate(c.n))}/min)`).join(", ") : NO_RECEIVE}</dd></div>
        {f.dlq && dlqRef && <div><dt className="inline text-ink-3">Dead-letter queue: </dt><dd className="inline"><Link href={href.queue(dlqRef)} className={link}>{f.dlq.name}</Link> <span className="text-ink-3">· {fmtNum(f.dlq.received)} received from it</span></dd></div>}
        {queueCard && <div className="pt-1">{queueCard}</div>}
      </dl>

      <div className="flex flex-wrap items-baseline gap-x-5 gap-y-1 border-t border-line px-5 py-2.5 text-sm">
        {facts.length > 0 && <span className="font-mono text-caption text-ink-3">{facts.join(" · ")}</span>}
        <span className="ml-auto flex gap-5">
          <Link href={href.queue(f)} className={link}>Queue page →</Link>
          <Link href={href.stuck(f)} className={link}>{stuckWord} on {f.queue} →</Link>
        </span>
      </div>
    </div>
  );
}
