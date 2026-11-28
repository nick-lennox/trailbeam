"use client";

import { Fragment, useEffect, useMemo, useState } from "react";
import { fmtNum } from "../model/format.ts";
import { fmtT } from "../model/events.ts";
import { heldAt, type HeldMessage, type InQueueView } from "../model/in-queue.ts";
import type { QueueLive } from "../model/queue-live.ts";
import { UiLink } from "../design/nav.tsx";
import { MessageSequence, type QueueFacts, type SequenceTrigger } from "./MessageSequence.tsx";
import { Watching } from "./Watching.tsx";
import { QueueLane, type LaneMessage } from "./QueueLane.tsx";

/* "In ‹queue› now" on recorded data (model/in-queue.ts): the instrumented messages in the queue, each visible or in flight,
   under the lane drawn to SQS's own totals when the host read them, and to what the spans account for when it did not. The
   host reloads the view every few seconds; between loads each visibility timeout counts down here, and a message whose
   timeout runs out becomes visible again (or, on its last allowed receive, is inferred to move to the dead-letter queue on
   the next receive attempt: "not seen on the DLQ yet", contract 9b, never counted as dead-lettered). Dead-lettered counts
   only the moves a receive on the dead-letter queue reported. Links go through the host's hrefs. */

const PAGE = 10;
const age = (s: number) => (s < 60 ? `${s} s` : `${Math.floor(s / 60)} min ${s % 60} s`);
const flip = <T,>(list: T[], x: T) => (list.includes(x) ? list.filter((y) => y !== x) : [...list, x]);
const th = "border-b border-line px-2.5 py-[7px] text-left font-sans text-xs font-medium whitespace-nowrap text-ink-3";
const td = "max-w-[260px] truncate border-b border-line px-2.5 py-1.5 text-ink-2";
const none = <span className="text-ink-3">none</span>;
const STATE = { visible: "Visible", in_flight: "In flight" };

/** a held message as the lane draws it */
const asLane = (m: HeldMessage, since = 0): LaneMessage => ({ id: m.id, receiveCount: m.receiveCount, state: m.state, sentMs: m.sentMs, leftS: m.leftS, used: m.used, since });

export type QueueNowHrefs = {
  /** a message's own page */
  message: (id: string) => string;
  /** the Event explorer with a query typed in */
  events: (query: string) => string;
};

export type QueueNowLiveProps = {
  queue: string;
  view: InQueueView | null;
  /** the queue's figures the host read (AWS's, in the hosted app); null when none were */
  live: QueueLive | null;
  /** messages in the queue the backend worked out, before it cut the list it sent */
  heldTotal: number;
  consumer: string;
  fifo: boolean;
  triggers?: SequenceTrigger[] | null;
  hrefs: QueueNowHrefs;
};

export function QueueNowLive({ queue, view, live, heldTotal, consumer, fifo, triggers = null, hrefs }: QueueNowLiveProps) {
  // the clock, offset so the server's "now" and the browser's agree
  const [skew] = useState(() => (view ? Date.now() - view.nowMs : 0));
  const [now, setNow] = useState(() => view?.nowMs ?? 0);
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now() - skew), 1000);
    return () => clearInterval(id);
  }, [skew]);

  const [counts, setCounts] = useState<number[]>([]);
  const [states, setStates] = useState<HeldMessage["state"][]>([]);
  const [from, setFrom] = useState<string[]>([]);
  const [open, setOpen] = useState<string | null>(null);
  const [shown, setShown] = useState(PAGE);
  const change = (f: () => void) => { f(); setOpen(null); setShown(PAGE); };

  const max = view?.config.maxReceiveCount ?? null;
  const defaultTimeout = !!view && !view.config.visibilityKnown;
  const all = useMemo(() => (view?.held ?? []).map((m) => heldAt(m, now, max, defaultTimeout)), [view, now, max, defaultTimeout]);
  const held = all.filter((m) => !m.left);
  /* inferred to move on the next receive attempt: here, and those whose timeout ran out before the page loaded */
  const notSeen = all.filter((m) => m.left).length + (view?.notSeenOnDlq.length ?? 0);

  if (!view) {
    return (
      <section className="overflow-hidden rounded-card border border-line bg-surface shadow-card" aria-label={`In ${queue} now`}>
        <h2 className="border-b border-line px-5 py-3.5 text-sm font-medium text-ink">In {queue} now</h2>
        <Watching className="px-6 pt-8 pb-10" title={`No messages to show in ${queue}`}>Trailbeam is watching this queue.</Watching>
      </section>
    );
  }

  const cfg = view.config;
  const vis = held.filter((m) => m.state === "visible").length, fly = held.length - vis;
  const extra = heldTotal - view.held.length;
  /* SQS's totals when the host read them; otherwise what the spans account for */
  const totals = view.totals;
  const visible = totals ? totals.visible : vis + Math.max(0, extra);
  const inFlight = totals ? totals.inFlight : fly;
  const un = view.uninstrumented;
  const total = visible + inFlight;

  const pass = (m: HeldMessage) => (!counts.length || counts.includes(m.receiveCount)) && (!states.length || states.includes(m.state)) && (!from.length || from.includes(m.producer || "No send observed"));
  const rows = held.filter(pass).sort((a, b) => b.receiveCount - a.receiveCount || (a.state === b.state ? (a.state === "in_flight" ? a.leftS - b.leftS : a.sentMs - b.sentMs) : a.state === "in_flight" ? -1 : 1));
  const page = rows.slice(0, shown);
  /* with no filter on, the messages the backend worked out but did not send are counted too */
  const listed = rows.length + (states.length || counts.length || from.length ? 0 : Math.max(0, extra));
  const top = Math.max(max ?? 0, ...held.map((m) => m.receiveCount));
  const rcs = Array.from({ length: top + 1 }, (_, rc) => ({ rc, n: held.filter((m) => m.receiveCount === rc).length })).reverse().filter((c) => c.rc === 0 || c.rc === max || c.n > 0);
  const query = [`queue:${queue}`, ...counts.map((c) => `receive_count:${c}`)].join(" ");
  const facets = [
    { title: "Receive count", values: rcs.map((c) => ({ key: String(c.rc), label: max !== null ? `${c.rc} of ${max}` : String(c.rc), n: c.n, mono: true, on: counts.includes(c.rc), toggle: () => setCounts(flip(counts, c.rc)) })) },
    { title: "State", values: ([["in_flight", "In flight", fly], ["visible", "Visible", vis]] as [HeldMessage["state"], string, number][]).map(([k, label, n]) => ({ key: k, label, n, mono: false, on: states.includes(k), toggle: () => setStates(flip(states, k)) })) },
    { title: "Producer", values: view.producers.map((p) => ({ key: p.name, label: p.name, n: p.n, mono: true, on: from.includes(p.name), toggle: () => setFrom(flip(from, p.name)) })) },
  ];

  /* the lane: the totals, the instrumented messages as marks, deletes in the past minute, and the moves to the
     dead-letter queue reported in the past hour (receives on it naming this queue). Those inferred from
     maxReceiveCount are not moves seen, so they are not counted or drawn there: the caption says how many */
  const t = Math.floor(now / 1000);
  const gone: LaneMessage[] = view.deleted.filter((d) => d.id).map((d) => ({
    id: d.id!, receiveCount: 0, state: "visible", sentMs: d.atMs, leftS: 0, used: 0, since: Math.floor(d.atMs / 1000), gone: "deleted",
  }));
  const sel = open ? held.find((m) => m.id === open) : undefined;
  /* what the opened message's panel may say about the queue: only what the host read */
  const facts: QueueFacts = {
    read: live?.status === "ok", fifo: live?.fifo ?? null, visibilityTimeoutS: cfg.visibilityTimeoutS, retentionS: cfg.retentionS,
    delayS: live?.delayS ?? null, maxReceiveCount: cfg.maxReceiveCount, dlq: cfg.dlq, triggers,
  };
  const b = (n: number) => <b className="font-mono font-medium text-ink-2">{fmtNum(n)}</b>;

  return (
    <section className="overflow-hidden rounded-card border border-line bg-surface shadow-card" aria-label={`In ${queue} now`}>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 px-4 pt-3.5 pb-3">
        <h2 className="text-sm font-medium text-ink">In {queue} now</h2>
        <p className="mr-auto text-[13px] text-ink-3">
          {cfg.visibilityKnown ? <>Visibility timeout <span className="font-mono text-caption text-ink-2">{cfg.visibilityTimeoutS} s</span></> : <>Visibility timeout not read, so SQS&apos;s default <span className="font-mono text-caption text-ink-2">30 s</span> is used</>}
          {max !== null ? <>, maxReceiveCount <span className="font-mono text-caption text-ink-2">{max}</span>{cfg.dlq && <>, dead-letter queue <span className="font-mono text-caption text-ink-2">{cfg.dlq}</span></>}</> : cfg.visibilityKnown ? ", no redrive policy" : ""}
        </p>
        <span className="inline-flex items-center gap-2 text-[13px] text-ink-3">
          <i className="block size-1.5 rounded-full bg-good" aria-hidden />
          Live <span className="font-mono text-caption tabular-nums text-ink-2">{fmtT(now).slice(0, 8)}</span>
        </span>
      </div>

      <div className="border-y border-line bg-canvas/50 px-4 py-3">
        <QueueLane
          live={held.map((m) => asLane(m))} gone={gone} consumer={consumer || "the consumer"} dlq={cfg.dlq} max={max} fifo={fifo}
          visible={visible} inFlight={inFlight} flow={view.deleted.length} windowS={60} exact t={t} dead={view.seenOnDlq} deadNote="Seen on the dead-letter queue in the past hour."
          selected={open} onSelect={(id) => setOpen(open === id ? null : id)} onGroup={(st) => change(() => setStates(states.length === 1 && states[0] === st ? [] : [st]))}
        />
        <p className="mt-2 text-xs text-ink-3">
          {totals && un
            ? <>SQS holds {b(totals.visible)} visible and {b(totals.inFlight)} in flight{totals.delayed ? <>, and {b(totals.delayed)} delayed</> : null}. Your instrumented services account for {b(vis)} visible and {b(fly)} in flight, listed below. The other {b(un.visible)} visible and {b(un.inFlight)} in flight were sent or received by services without Trailbeam.{un.over && " Your spans list more messages than SQS holds: some were deleted by a service without Trailbeam, or their delete spans were lost."}</>
            : <>{live?.detail ?? "SQS's own totals were not read."} The counts are the messages your instrumented services sent or received.</>}
          {notSeen > 0 && <> {b(notSeen)} {notSeen === 1 ? "message has" : "messages have"} reached maxReceiveCount and {notSeen === 1 ? "is" : "are"} not seen on the DLQ yet (inferred): SQS moves such a message on its next receive, and no receive from the dead-letter queue has reported {notSeen === 1 ? "it" : "them"}, so {notSeen === 1 ? "it is" : "they are"} not counted as dead-lettered.</>}
          {view.capped && <> Only the oldest {fmtNum(view.held.length)} unfinished messages were read.</>}
        </p>
      </div>

      <div className="grid md:grid-cols-[212px_minmax(0,1fr)]">
        <aside className="grid content-start gap-3 border-b border-line px-3 py-3 text-[13px] md:border-r md:border-b-0">
          {facets.map((f) => (
            <fieldset key={f.title}>
              <legend className="px-0 py-1 text-xs font-medium text-ink-2">{f.title}</legend>
              {f.values.map((v) => (
                <label key={v.key} className="-mx-1.5 flex cursor-pointer items-center gap-2 rounded-[5px] px-1.5 py-[3px] hover:bg-sunken">
                  <input type="checkbox" checked={v.on} onChange={() => change(v.toggle)} className="size-3.5 shrink-0 accent-brand" />
                  <span className={`min-w-0 flex-1 truncate text-ink ${v.mono ? "font-mono text-caption" : "font-sans text-[13px]"}`}>{v.label}</span>
                  <span className="font-mono text-micro tabular-nums text-ink-3">{fmtNum(v.n)}</span>
                </label>
              ))}
            </fieldset>
          ))}
        </aside>

        <div className="min-w-0 overflow-auto [container-type:inline-size]">
          <table className="w-full border-collapse font-mono text-caption">
            <thead className="bg-surface">
              <tr>{["Sent", "Message ID", "Producer", "Consumer", "State", "Receive count", "Visibility timeout", "Last error"].map((c) => <th key={c} className={th}>{c}</th>)}</tr>
            </thead>
            <tbody>
              {page.map((m) => {
                const isOpen = open === m.id;
                const flying = m.state === "in_flight";
                const last = max !== null && m.receiveCount >= max;
                const cell = `${td} ${isOpen ? "bg-brand-soft" : "group-hover:bg-sunken"}`;
                return (
                  <Fragment key={m.id}>
                    <tr className="group cursor-pointer" tabIndex={0} aria-expanded={isOpen} onClick={() => setOpen(isOpen ? null : m.id)}
                      onKeyDown={(k) => { if (k.key === "Enter" || k.key === " ") { k.preventDefault(); setOpen(isOpen ? null : m.id); } }}>
                      <td className={`${cell} tabular-nums ${isOpen ? "text-accent" : "text-ink-3"}`}>{fmtT(m.sentMs).slice(0, 8)}</td>
                      <td className={`${cell} text-ink`}>{m.id.slice(0, 8)}</td>
                      <td className={cell}>{m.producer || none}</td>
                      <td className={cell}>{m.consumer || none}</td>
                      <td className={cell}><span className="font-sans text-[13px] font-medium text-ink">{STATE[m.state]}</span></td>
                      <td className={`${cell} tabular-nums ${last ? "!text-critical-text" : ""}`}>{m.receiveCount}{max !== null && ` of ${max}`}</td>
                      <td className={`${cell} tabular-nums ${last && flying ? "!text-critical-text" : ""}`}>
                        {flying ? (
                          <span className="inline-flex items-center gap-2">
                            <i className="block h-1 w-10 shrink-0 rounded-[2px]" style={{ background: `linear-gradient(to right, ${last ? "var(--color-critical)" : "var(--color-ink-2)"} ${(1 - m.used) * 100}%, var(--color-line-strong) 0)` }} aria-hidden />
                            {age(m.leftS)} left
                          </span>
                        ) : none}
                      </td>
                      <td className={cell}>{m.lastError ?? none}</td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={8} className="border-b border-line bg-canvas p-0">
                          <div className="@container sticky left-0 w-[100cqw] px-5 py-4">
                            {m.events.length ? <MessageSequence event={m.events[m.events.length - 1]} history={m.events} stacked queue={facts} /> : null}
                            <UiLink href={hrefs.message(m.id)} className="mt-3 inline-flex items-center gap-1 font-sans text-xs font-medium text-brand hover:underline">Open its timeline</UiLink>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
              {page.length === 0 && (
                <tr><td colSpan={8} className="px-6 py-10 text-center font-sans text-sm text-ink-2">
                  {held.length === 0
                    ? <Watching title={total ? `No message from your instrumented services in ${queue} right now` : `No messages in ${queue} right now`}>{total ? "SQS holds messages that services without Trailbeam sent or received." : "The queue is empty. Trailbeam is watching for the next message."}</Watching>
                    : "No message in the queue matches these filters."}
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 border-t border-line px-4 py-2 text-[13px] text-ink-3">
        <span className="flex flex-wrap items-center gap-x-4">
          <span>
            {rows.length > page.length ? <>Showing <b className="font-mono font-medium text-ink">{page.length}</b> of </> : null}
            <b className="font-mono font-medium tabular-nums text-ink">{fmtNum(listed)}</b> {listed === 1 ? "message" : "messages"}
          </span>
          {rows.length > shown && <button type="button" onClick={() => setShown(shown + PAGE)} className="font-medium text-brand hover:underline">Show {Math.min(PAGE, rows.length - shown)} more</button>}
          <UiLink href={hrefs.events(query)} className="inline-flex items-center gap-1 font-medium text-brand hover:underline">Open in Event explorer</UiLink>
        </span>
        <span>{sel ? "Select the row again to close it" : max !== null ? "Closest to the dead-letter queue first" : "Highest receive count first"}</span>
      </div>
    </section>
  );
}
