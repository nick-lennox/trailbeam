"use client";

import type { ReactNode } from "react";
import { UiLink } from "../design/nav.tsx";
import { SourceTag } from "../design/pills.tsx";
import { SystemBadge } from "../design/SystemBadge.tsx";
import { shownId } from "../systems.ts";
import { fmtMs } from "../model/format.ts";
import { fmtDate, eventName, type Ev } from "../model/events.ts";

/*
  An opened row of the Event explorer: what the event's span said, and the way to the message's whole timeline. Only
  what the event carries is listed: a fact no span gave is left out rather than said to be missing. The hosted
  sample's opened row draws queue settings and platforms that spans do not carry, and the hosted app passes it in
  (EventExplorerProps.detail).
*/
export function EventDetail({ event: e, messageHref }: { event: Ev; messageHref: (id: string) => string }) {
  const a = e.attrs ?? {};
  const mono = (k: string, v: ReactNode): [string, ReactNode] => [k, <span key={k} className="font-mono">{v}</span>];
  const facts: [string, ReactNode][] = [
    mono("Time", `${fmtDate(e.ms)} UTC`),
    mono("Queue", e.queue),
    ...(a["queue.account"] ? [mono("Account", a["queue.account"])] : []),
    ...(a["queue.region"] ? [mono("Region", a["queue.region"])] : []),
    [e.event === "send" ? "Producer" : e.event === "visibility_expired" || e.event === "dead_letter" ? "Consumer before it" : "Consumer", e.service || "Not reported"],
    ...(e.tenant ? [mono("Tenant", e.tenant)] : []),
    ...(a["message.MessageGroupId"] ? [mono("Message group", a["message.MessageGroupId"])] : []),
    ...(a["message.SequenceNumber"] ? [mono("Sequence number", a["message.SequenceNumber"])] : []),
    ...(e.sizeBytes !== undefined ? [["Body size", <><span key="n" className="font-mono">{e.sizeBytes.toLocaleString("en-US")}</span> bytes</>] as [string, ReactNode]] : []),
    ...(e.receiveCount !== undefined ? [mono("Receive count", e.receiveCount)] : []),
    ...(e.waitMs !== undefined ? [mono("Time in queue", fmtMs(Math.round(e.waitMs)))] : []),
    ...(e.durationMs !== undefined ? [mono("Processing time", fmtMs(Math.round(e.durationMs)))] : []),
    ["Recorded by", <SourceTag key="s" source={e.source} />],
  ];
  return (
    <div className="grid max-w-3xl gap-3 font-sans text-[13px]" data-event-detail>
      <p className="text-ink">
        <b className="font-medium">{eventName(e)}</b>
        {e.system && <> <SystemBadge system={e.system} /></>}
        {e.detail ? <span className="text-ink-2">. {e.detail}</span> : null}
      </p>
      <dl className="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-6 gap-y-1">
        {facts.map(([k, v]) => (
          <div key={k} className="contents">
            <dt className="text-ink-3">{k}</dt>
            <dd className="min-w-0 truncate text-ink-2">{v}</dd>
          </div>
        ))}
      </dl>
      <UiLink href={messageHref(e.messageId)} className="inline-flex items-center gap-1 text-brand hover:underline">
        Open message <span className="font-mono text-caption">{shownId(e.messageId)}</span>
      </UiLink>
    </div>
  );
}
