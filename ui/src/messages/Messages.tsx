/*
  A list of messages (GET /api/messages, /api/duplicates, /api/stuck: contract/openapi.yaml, MessageSummary) and
  the empty state that stands where a list would be. Role tokens (theme.css), like the Tenants table, so they
  take the host page's theme. Links come from href().
*/
import type { ReactNode } from "react";

export type MessageSummary = {
  message_id: string;
  state: string;
  source_queue: string;
  current_queue: string;
  in_dead_letter_queue: boolean;
  dead_letter_queue_move: "observed" | "not_seen_on_dlq_yet" | null;
  deliveries: number;
  successful_deliveries: number;
  consumers: { service_name: string | null; client_id: string | null }[];
  first_seen: string | null;
  first_seen_clock: "host" | "sqs";
  last_seen: string | null;
  /** /api/duplicates */
  reasons?: string[];
  /** /api/stuck */
  age_s?: number | null;
  age_across_clocks?: boolean;
};

export const STATE_LABELS: Record<string, string> = {
  deleted: "deleted",
  deleted_inferred: "deleted by Lambda (inferred)",
  processing: "processing",
  not_deleted: "not deleted",
};

/** The Reason filter's options on Duplicates. */
export const DUPLICATE_REASONS: Record<string, string> = {
  received_more_than_once: "received more than once",
  succeeded_more_than_once: "handled more than once",
};

/** The Why cell on Duplicates: each reason with its count from the row (receives, or successful handlings). */
export function duplicateWhy(m: MessageSummary): ReactNode {
  const parts = (m.reasons ?? []).map((r) => {
    const n = r === "received_more_than_once" ? m.deliveries : r === "succeeded_more_than_once" ? m.successful_deliveries : null;
    if (n === null) return <span key={r}>{DUPLICATE_REASONS[r] ?? r}</span>;
    const verb = r === "received_more_than_once" ? "received" : "handled";
    return <span key={r}>{verb} <span className="font-mono tabular-nums">{n}</span> times</span>;
  });
  return parts.flatMap((p, i) => (i ? [", ", p] : [p]));
}

/** An API timestamp as the list shows it: UTC, to the millisecond, no "T" or "Z". */
export const fmtTime = (t: string | null) => (t ? t.replace("T", " ").replace(/(\.\d{3})\d*Z$/, "$1").replace(/Z$/, "") : null);

/** A stuck message's age, as the server-rendered page says it. */
export function fmtAge(s: number | null | undefined): string {
  if (s === null || s === undefined) return "?";
  if (s < 120) return `${Math.round(s)} s`;
  if (s < 7200) return `${Math.round(s / 60)} min`;
  if (s < 172_800) return `${(s / 3600).toFixed(1)} h`;
  return `${(s / 86_400).toFixed(1)} days`;
}

/** A threshold in seconds, in words when it is a whole number of days, hours or minutes: 300 → "5 min". */
export function fmtThreshold(s: number): string {
  const unit = (n: number, one: string, many = one) => `${n} ${n === 1 ? one : many}`;
  if (s > 0 && s % 86_400 === 0) return unit(s / 86_400, "day", "days");
  if (s > 0 && s % 3600 === 0) return unit(s / 3600, "h");
  if (s > 0 && s % 60 === 0) return unit(s / 60, "min");
  return `${s} s`;
}

const Muted = ({ children }: { children: ReactNode }) => <span className="text-ink-3">{children}</span>;

function Queue({ m }: { m: MessageSummary }) {
  return (
    <>
      <span className="font-mono text-caption text-ink">{m.current_queue || m.source_queue || "?"}</span>
      {m.in_dead_letter_queue && <> <Muted>(DLQ, from {m.source_queue || "?"})</Muted></>}
      {!m.in_dead_letter_queue && m.dead_letter_queue_move === "not_seen_on_dlq_yet" && <> <Muted>(maxReceiveCount reached; not seen on the DLQ yet)</Muted></>}
    </>
  );
}

const Time = ({ t }: { t: string | null }) => (t ? <time dateTime={t} className="font-mono text-caption tabular-nums text-ink-2">{fmtTime(t)}</time> : <Muted>unknown</Muted>);

export type Column = { title: string; cell: (m: MessageSummary) => ReactNode; align?: "right" };

export type MessagesTableProps = {
  messages: MessageSummary[];
  /** where a message links to: its timeline */
  href: (messageId: string) => string;
  /** one more column at the end (Why, Age) */
  extra?: Column;
};

/** One row per message, in the API's order. Times are UTC. */
export function MessagesTable({ messages, href, extra }: MessagesTableProps) {
  const heads = ["Message ID", "State", "Queue", "Deliveries", "Consumers", "First seen", "Last seen", ...(extra ? [extra.title] : [])];
  return (
    <table className="w-full text-[15px]">
      <thead>
        <tr className="border-b border-line text-left font-mono text-caption text-ink-3">
          {heads.map((h, i) => <th key={h} className={`py-2 font-medium ${i === 0 ? "pl-5 pr-3" : i === heads.length - 1 ? "pl-3 pr-5" : "px-3"}`}>{h}</th>)}
        </tr>
      </thead>
      <tbody>
        {messages.map((m) => (
          <tr key={m.message_id} className="border-b border-line/60 align-top last:border-0">
            <td className="py-3 pl-5 pr-3"><a href={href(m.message_id)} className="break-all font-mono text-caption text-ink hover:text-brand">{m.message_id}</a></td>
            <td className="px-3 py-3 text-ink-2">{STATE_LABELS[m.state] ?? m.state}</td>
            <td className="px-3 py-3"><Queue m={m} /></td>
            <td className="px-3 py-3 font-mono text-caption tabular-nums text-ink-2">{m.deliveries} <Muted>({m.successful_deliveries} ok)</Muted></td>
            <td className="px-3 py-3 text-ink-2">
              {m.consumers.length === 0 ? <Muted>none yet</Muted> : m.consumers.map((c, i) => (
                <span key={i}>{i > 0 && ", "}{c.service_name ?? "?"}</span>
              ))}
            </td>
            <td className="px-3 py-3"><Time t={m.first_seen} />{m.first_seen_clock === "sqs" && <> <Muted>SQS&rsquo;s clock</Muted></>}</td>
            <td className={`py-3 ${extra ? "px-3" : "pl-3 pr-5"}`}><Time t={m.last_seen} /></td>
            {extra && <td className={`py-3 pl-3 pr-5 text-ink-2 ${extra.align === "right" ? "text-right" : ""}`}>{extra.cell(m)}</td>}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export type EmptyStateProps = { title: ReactNode; detail?: ReactNode; link?: { href: string; label: string } };

/** Where a list would be: a sentence, at most one line of explanation, at most one link. No panel. */
export function EmptyState({ title, detail, link }: EmptyStateProps) {
  return (
    <div className="py-2 text-[15px] leading-[1.5]" data-empty>
      <p className="text-ink">{title}</p>
      {detail && <p className="mt-1 max-w-[72ch] text-ink-2">{detail}</p>}
      {link && <p className="mt-1"><a href={link.href} className="text-brand hover:underline">{link.label}</a></p>}
    </div>
  );
}

/** The link every page's empty-store state ends with. */
export const SET_UP = { href: "/", label: "Set up your first service" };
