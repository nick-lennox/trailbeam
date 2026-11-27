"use client";

import { Wires, type Line } from "../overview/FlowDrawing.tsx";
import { PlatformIcon } from "../design/icons.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { STAGES, severity, type Issue } from "../model/issues.ts";
import { TONE } from "./parts.tsx";

/*
  An issue on its flow, drawn the way the Overview draws a queue: what sends to the queue,
  the queue, what receives from it, and the dead-letter queue. The part where the issue
  stops messages carries the mark, and the wire into trouble is red.
*/

/* the hosted Overview's wire tones, as the shared Wires' colours */
const WIRE = { send: "var(--color-series-2)", receive: "var(--color-series-1)", warn: "var(--color-serious)", bad: "var(--color-critical)" } as const;

const H = 196;
const SEND = "var(--color-series-2)", RECEIVE = "var(--color-series-1)";
const Key = ({ color }: { color: string }) => <span aria-hidden className="mr-1.5 inline-block size-2 rounded-[2px] align-baseline" style={{ background: color }} />;

export function IssueFlow({ issue }: { issue: Pick<Issue, "stage" | "queue" | "producers" | "consumers" | "dlq" | "platforms" | "count" | "inDlq" | "notFinished" | "maxReceives" | "maxReceiveCount" | "visibilityTimeoutS"> }) {
  const tone = TONE[severity(issue)];
  const stage = STAGES.find((s) => s.id === issue.stage)!;
  const atQueue = issue.stage === "queue" || issue.stage === "receive";
  const ins = issue.producers.slice(0, 3);
  const y = (i: number, n: number) => (H * (i + 1)) / (n + 1);
  const port = (i: number, n: number) => H / 2 + (i - (n - 1) / 2) * 14;
  const hasDlq = issue.dlq !== null;
  const yConsumer = hasDlq ? H * 0.3 : H / 2, yDlq = H * 0.76;
  const consumer = issue.consumers.join(", ");

  const node = "absolute inset-x-0 -translate-y-1/2 rounded-lg border bg-surface px-3 py-2.5";
  const face = (platform: string, role: string, name: string, color: string) => (
    <span className="flex items-center gap-2.5">
      <span className="grid size-8 shrink-0 place-items-center rounded-md border border-line bg-canvas"><PlatformIcon platform={platform} size={18} color={color} /></span>
      <span className="min-w-0">
        <span className="block truncate text-xs text-ink-3"><Key color={color} />{role}</span>
        <Tooltip content={name} className="block truncate text-sm font-medium text-ink">{name}</Tooltip>
      </span>
    </span>
  );
  const stops = (
    <span className="mt-2 flex items-center gap-1.5 border-t border-line pt-2 text-xs font-medium" style={{ color: tone }}>
      <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden><path d="M1.5 1.5 L8.5 8.5 M8.5 1.5 L1.5 8.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" /></svg>
      Stops here: {stage.label}
    </span>
  );
  const out: Line[] = [
    ...(consumer ? [{ key: "c", y1: hasDlq ? H / 2 - 8 : H / 2, y2: yConsumer, color: WIRE[atQueue ? "receive" : severity(issue) === "critical" ? "bad" : "warn"] }] : []),
    ...(hasDlq ? [{ key: "d", y1: H / 2 + 8, y2: yDlq, color: WIRE.bad }] : []),
  ];

  return (
    <section
      aria-label={`The flow through ${issue.queue}`}
      className="overflow-x-auto rounded-card border border-line bg-canvas/60"
      style={{ backgroundImage: "radial-gradient(color-mix(in srgb, var(--color-ink-3) 26%, transparent) 1px, transparent 1px)", backgroundSize: "16px 16px", backgroundPosition: "8px 8px" }}
    >
      <div className="grid min-w-[880px] grid-cols-[minmax(160px,210px)_minmax(90px,1fr)_minmax(300px,360px)_minmax(90px,1fr)_minmax(230px,280px)] px-6 py-5">
        <div className="relative" style={{ height: H }}>
          {ins.length === 0 && <p className="absolute inset-x-0 top-1/2 -translate-y-1/2 text-sm leading-relaxed text-ink-3">No data. No service reported a send for these messages.</p>}
          {ins.map((p, i) => (
            <div key={p} className={`${node} border-line-strong`} style={{ top: y(i, ins.length) }}>{face(issue.platforms?.[p] ?? "", "Producer", p, SEND)}</div>
          ))}
        </div>
        <Wires height={H} lines={ins.map((p, i) => ({ key: p, y1: y(i, ins.length), y2: port(i, ins.length), color: WIRE.send }))} />

        <div className="flex items-center" style={{ height: H }}>
          <div className={`w-full rounded-card border bg-surface shadow-raised ${atQueue ? "" : "border-line-strong"}`} style={atQueue ? { borderColor: tone } : undefined}>
            <div className="flex items-center gap-2.5 px-4 pt-3">
              <PlatformIcon platform="SQS" size={18} />
              <h3 className="min-w-0 truncate text-[15px] font-medium text-ink">{issue.queue}</h3>
            </div>
            <dl className="mt-2.5 grid grid-cols-2 gap-x-5 gap-y-1.5 px-4 pb-3 text-xs whitespace-nowrap">
              {([
                ["Messages in this issue", String(issue.count)],
                ["Highest receive count", String(issue.maxReceives)],
                ["Visibility timeout", issue.visibilityTimeoutS === null ? null : `${issue.visibilityTimeoutS} s`],
                ["maxReceiveCount", issue.maxReceiveCount === null ? null : String(issue.maxReceiveCount)],
              ] as [string, string | null][]).map(([k, v]) => (
                <div key={k} className="flex items-baseline justify-between gap-3">
                  <dt className="text-ink-3">{k}</dt>
                  <dd className="shrink-0 font-mono text-[12.5px] text-ink tabular-nums">{v ?? <span className="font-sans text-ink-3">No data</span>}</dd>
                </div>
              ))}
            </dl>
            {atQueue && <div className="px-4 pb-3">{stops}</div>}
          </div>
        </div>
        <Wires height={H} lines={out} />

        <div className="relative" style={{ height: H }}>
          {consumer ? (
            <div className={`${node} ${atQueue ? "border-line-strong" : ""}`} style={{ top: yConsumer, ...(atQueue ? {} : { borderColor: tone }) }}>
              {face(issue.platforms?.[issue.consumers[0]] ?? "", "Consumer", consumer, RECEIVE)}
              {!atQueue && stops}
            </div>
          ) : (
            <p className="absolute inset-x-0 -translate-y-1/2 text-sm leading-relaxed text-ink-3" style={{ top: yConsumer }}>No data. No service reported a receive for these messages.</p>
          )}
          {hasDlq && (
            <div className={`${node} border-critical/60`} style={{ top: yDlq }}>
              <span className="flex items-center gap-2.5">
                <span className="grid size-8 shrink-0 place-items-center rounded-md border border-line bg-canvas"><PlatformIcon platform="SQS" size={18} /></span>
                <span className="min-w-0">
                  <span className="block truncate text-xs text-ink-3"><span className="font-mono font-medium text-critical-text tabular-nums">{issue.inDlq}</span> moved to this dead-letter queue</span>
                  <Tooltip content={issue.dlq} className="block truncate text-sm font-medium text-ink">{issue.dlq}</Tooltip>
                </span>
              </span>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
