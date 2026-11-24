/*
  A list of messages (GET /api/messages, /api/duplicates, /api/stuck: contract/openapi.yaml, MessageSummary) and
  the empty state that stands where a list would be. Role tokens (theme.css), like the Tenants table, so they
  take the host page's theme. Links come from href().
*/
import type { ReactNode } from "react";
import type { Purge } from "../timeline/types.ts";

export type MessageSummary = {
  message_id: string;
  state: string;
  source_queue: string;
  current_queue: string;
  in_dead_letter_queue: boolean;
  dead_letter_queue_move: "observed" | "not_seen_on_dlq_yet" | null;
  deliveries: number;
  successful_deliveries: number;
  consumers: { service_name: string | null; client_id: string | null; service_version?: string | null }[];
  first_seen: string | null;
  first_seen_clock: "host" | "sqs";
  last_seen: string | null;
  /** contract 3.3: the recorded PurgeQueue call that removed the message, or may have */
  purge?: Purge;
  /** /api/duplicates */
  reasons?: string[];
  /** /api/stuck */
  age_s?: number | null;
  age_across_clocks?: boolean;
};

export const STATE_LABELS: Record<string, string> = {
  deleted: "deleted",
  deleted_inferred: "deleted by Lambda (inferred)",
  processing: "being handled (inferred)",
  not_deleted: "no delete seen",
  purged: "purged",
};

/** A message's state as the lists say it, with what a recorded PurgeQueue call says about it (contract 3.3). */
export function stateText(m: Pick<MessageSummary, "state" | "purge">): string {
  const label = STATE_LABELS[m.state] ?? m.state;
  const p = m.purge;
  if (!p) return label;
  const at = `PurgeQueue at ${fmtTime(p.start)}`;
  return p.status === "purged" ? `${label} (observed: ${at})` : `${label}, possibly purged (${at})`;
}

/** The tooltip on "possibly purged". */
export const POSSIBLY_PURGED_TITLE = "The last observed step ended while a purge of the queue was running, or within 60 s after it. "
  + "SQS may or may not have deleted this message.";

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
      {!m.in_dead_letter_queue && m.dead_letter_queue_move === "not_seen_on_dlq_yet" && <> <Muted>(maxReceiveCount reached; DLQ move inferred, not seen yet)</Muted></>}
    </>
  );
}

const Time = ({ t }: { t: string | null }) => (t ? <time dateTime={t} className="font-mono text-caption tabular-nums text-ink-2">{fmtTime(t)}</time> : <Muted>unknown</Muted>);

export type Column = {
  title: string;
  cell: (m: MessageSummary) => ReactNode;
  align?: "right";
  /** the same fact as plain text, for a row's second line below `md` (the reason, the age); `cell` when absent */
  brief?: (m: MessageSummary) => string;
};

/** A message ID on one line: the first 8 and last 5 characters around an ellipsis, when it is longer than that. */
export const shortId = (id: string) => (id.length > 16 ? `${id.slice(0, 8)}\u2026${id.slice(-5)}` : id);

const plural = (n: number, one: string, many: string) => `${n} ${n === 1 ? one : many}`;

/** A row's second line below `md`: state · queue · receives · handled, then the extra column's fact. */
export function rowFacts(m: MessageSummary): string[] {
  const queue = m.current_queue || m.source_queue || "?";
  return [
    stateText(m),
    m.in_dead_letter_queue ? `${queue} (DLQ)` : queue,
    plural(m.deliveries, "receive", "receives"),
    `${m.successful_deliveries} handled`,
  ];
}

export type MessagesTableProps = {
  messages: MessageSummary[];
  /** where a message links to: its timeline */
  href: (messageId: string) => string;
  /** one more column at the end (Why, Age) */
  extra?: Column;
};

/** The Receives head's tooltip: what the count is, and what it is not (SQS's own receive count). */
export const RECEIVES_TITLE = "Receives Trailbeam observed, on every queue, the receive from a dead-letter queue included. "
  + "SQS's own receive count can be higher: it counts receives by consumers Trailbeam doesn't see. "
  + "Handled: a receive that ended in a delete or a successful Lambda record.";

/** The tooltip on a column head, where the word alone needs explaining. */
const HEAD_TITLES: Record<string, string> = {
  Receives: RECEIVES_TITLE,
};

/** One row per message, in the API's order. Times are UTC. A message received more than once shows its count
 *  in warning-text (Ember on the sky, the timeline's colour for a repeat attempt). Below `md`, three lines per
 *  row instead of the table. */
export function MessagesTable({ messages, href, extra }: MessagesTableProps) {
  const heads = ["Message ID", "State", "Queue", "Receives", "Consumers", "First seen", "Last seen", ...(extra ? [extra.title] : [])];
  return (
    <>
      <ul className="md:hidden" data-phone-list>
        {messages.map((m) => {
          const facts = rowFacts(m);
          const more = extra && (extra.brief ? extra.brief(m) : extra.cell(m));
          return (
            <li key={m.message_id} className="border-b border-line/60 py-3 leading-[1.45] last:border-0">
              <a href={href(m.message_id)} title={m.message_id} className="block truncate whitespace-nowrap font-mono text-caption text-ink hover:text-brand">{shortId(m.message_id)}</a>
              <p className="truncate text-[15px] text-ink-2" title={[...facts, ...(typeof more === "string" ? [more] : [])].join(" · ")}>
                {facts.join(" · ")}{more !== undefined && more !== "" && <> · {more}</>}
              </p>
              <p className="truncate font-mono text-caption tabular-nums text-ink-3">
                {m.first_seen ? <time dateTime={m.first_seen}>{fmtTime(m.first_seen)}</time> : "first seen unknown"}{m.first_seen_clock === "sqs" && " SQS\u2019s clock"}
              </p>
            </li>
          );
        })}
      </ul>
    <table className="hidden w-full text-[15px] md:table">
      <thead>
        <tr className="border-b border-line text-left font-mono text-caption text-ink-3">
          {heads.map((h, i) => <th key={h} title={HEAD_TITLES[h]} className={`py-2 font-medium ${i === 0 ? "pl-5 pr-3" : i === heads.length - 1 ? "pl-3 pr-5" : "px-3"}`}>{h}</th>)}
        </tr>
      </thead>
      <tbody>
        {messages.map((m) => (
          <tr key={m.message_id} className="border-b border-line/60 align-top last:border-0">
            <td className="py-3 pl-5 pr-3"><a href={href(m.message_id)} className="break-all font-mono text-caption text-ink hover:text-brand">{m.message_id}</a></td>
            <td className="px-3 py-3 text-ink-2" data-purge={m.purge?.status} title={m.purge?.status === "possibly_purged" ? POSSIBLY_PURGED_TITLE : undefined}>{stateText(m)}</td>
            <td className="px-3 py-3"><Queue m={m} /></td>
            <td className="px-3 py-3 font-mono text-caption tabular-nums text-ink-2"><span className={m.deliveries > 1 ? "text-warning-text" : undefined}>{m.deliveries}</span> <Muted>· {m.successful_deliveries} handled</Muted></td>
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
    </>
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
