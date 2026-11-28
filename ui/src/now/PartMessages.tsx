"use client";

import { Fragment, useState } from "react";
import { fmtBytes, fmtMs, fmtNum } from "../model/format.ts";
import { fmtT } from "../model/events.ts";
import type { PartMessage } from "../model/part-messages.ts";
import { UiLink } from "../design/nav.tsx";
import { MessageSequence, type QueueFacts, type Sequence } from "./MessageSequence.tsx";

/* The messages behind a selected producer, consumer or dead-letter queue on the Overview: the same table as the
   queue's own list ("In ‹queue› now"), one row per message, and a row opens to that message's sequence
   diagram. The host passes the rows in, newest first: the hosted app's sample, or a backend's messages from spans. */

export type { PartMessage };
export type PartMessagesProps = {
  queue: string; part: "consumer" | "dlq" | "producer"; producer?: string; rows: readonly PartMessage[]; max: number | null;
  /* the Event explorer with a query typed in, for "Open in Event explorer" */
  eventsHref: (q: string) => string;
  facts?: QueueFacts;
  /* the diagram for a row, when the host draws its own events (the hosted app's sample) */
  sequenceFor?: (m: PartMessage) => Sequence;
};

const PAGE = 10;
const th = "border-b border-line px-2.5 py-[7px] text-left font-sans text-xs font-medium whitespace-nowrap text-ink-3";
const td = "max-w-[320px] truncate border-b border-line px-2.5 py-1.5 text-ink-2";
const none = <span className="font-sans text-ink-3">none</span>;
const OUTCOME = { visible: "Visible", in_flight: "In flight", deleted: "Deleted", failed: "Processing failed", dead_lettered: "Dead-lettered" };

export function PartMessages({ queue, part, producer = "", rows, max, eventsHref, facts, sequenceFor }: PartMessagesProps) {
  const [open, setOpen] = useState<string | null>(null);
  const [shown, setShown] = useState(PAGE);
  const page = rows.slice(0, shown);
  const dlq = part === "dlq";
  const from = part === "producer";
  const cols = dlq ? ["Dead-lettered", "Message ID", "Producer", "Tenant", "Receive count", "Last error"] : from ? ["Sent", "Message ID", "Tenant", "Size", "State", "Last error"] : ["Received", "Message ID", "Producer", "Tenant", "Outcome", "Processing time", "Last error"];

  if (!rows.length) return null;
  return (
    <div className="border-t border-line">
      <div className="min-w-0 overflow-auto [container-type:inline-size]">
        <table className="w-full border-collapse font-mono text-caption">
          <thead><tr>{cols.map((c) => <th key={c} className={`${th} first:pl-5`}>{c}</th>)}</tr></thead>
          <tbody>
            {page.map((m) => {
              const sel = open === m.id, cell = `${td} ${sel ? "bg-brand-soft" : "group-hover:bg-sunken"}`;
              const receive = m.events.find((e) => e.event === "receive"), end = m.events[m.events.length - 1];
              return (
                <Fragment key={m.id}>
                  <tr className="group cursor-pointer" tabIndex={0} aria-expanded={sel} onClick={() => setOpen(sel ? null : m.id)}
                    onKeyDown={(k) => { if (k.key === "Enter" || k.key === " ") { k.preventDefault(); setOpen(sel ? null : m.id); } }}>
                    <td className={`${cell} pl-5 tabular-nums ${sel ? "text-accent" : "text-ink-3"}`}>{fmtT(dlq ? m.lastMs : from ? m.sentMs : receive?.ms ?? m.sentMs).slice(0, 8)}</td>
                    <td className={`${cell} text-ink`}>{m.id.slice(0, 8)}</td>
                    {!from && <td className={cell}>{m.producer}</td>}
                    <td className={cell}>{m.events[0]?.tenant ?? none}</td>
                    {from && <td className={`${cell} tabular-nums`}>{m.events[0]?.sizeBytes !== undefined ? fmtBytes(m.events[0].sizeBytes) : none}</td>}
                    {!dlq && <td className={cell}><span className={`font-sans text-[13px] font-medium ${m.outcome === "failed" ? "text-critical-text" : "text-ink"}`}>{OUTCOME[m.outcome]}</span></td>}
                    {dlq
                      ? <td className={`${cell} tabular-nums !text-critical-text`}>{m.receiveCount}{max !== null && ` of ${max}`}</td>
                      : !from && <td className={`${cell} tabular-nums`}>{end?.durationMs !== undefined ? fmtMs(end.durationMs) : none}</td>}
                    <td className={cell}>{m.lastError ?? none}</td>
                  </tr>
                  {sel && end && (
                    <tr>
                      <td colSpan={cols.length} className="border-b border-line bg-canvas p-0">
                        <div className="@container sticky left-0 w-[100cqw] px-5 py-4">
                          <MessageSequence event={end} history={m.events} sequence={sequenceFor?.(m)} payload={m.payload} stacked queue={facts} />
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 px-5 py-2 text-[13px] text-ink-3">
        <span>
          {rows.length > page.length ? <>Showing <b className="font-mono font-medium text-ink">{page.length}</b> of </> : null}
          <b className="font-mono font-medium tabular-nums text-ink">{fmtNum(rows.length)}</b> {dlq ? "dead-lettered" : from ? "most recently sent" : "most recent"}
        </span>
        {rows.length > shown && <button type="button" onClick={() => setShown(shown + PAGE)} className="font-medium text-brand hover:underline">Show {Math.min(PAGE, rows.length - shown)} more</button>}
        <UiLink href={eventsHref(dlq ? `queue:${queue} event:dead_letter` : from ? `queue:${queue} producer:${producer} event:send` : `queue:${queue} event:receive`)} className="ml-auto inline-flex items-center gap-1 font-medium text-brand hover:underline">Open in Event explorer</UiLink>
      </div>
    </div>
  );
}
