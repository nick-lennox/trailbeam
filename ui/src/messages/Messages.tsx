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

/* each of a row's reasons: its verb and count from the row (receives, or successful handlings), or its name */
const whyParts = (m: MessageSummary) => (m.reasons ?? []).map((r) => {
  const n = r === "received_more_than_once" ? m.deliveries : r === "succeeded_more_than_once" ? m.successful_deliveries : null;
  return { r, verb: r === "received_more_than_once" ? "received" : "handled", n };
});

/** The Why cell on Duplicates: each reason with its count from the row (receives, or successful handlings). */
export function duplicateWhy(m: MessageSummary): ReactNode {
  const parts = whyParts(m).map(({ r, verb, n }) => n === null
    ? <span key={r}>{DUPLICATE_REASONS[r] ?? r}</span>
    : <span key={r}>{verb} <span className="font-mono tabular-nums">{n}</span> times</span>);
  return parts.flatMap((p, i) => (i ? [", ", p] : [p]));
}

/** The Why cell as text, for an export: "received 3 times, handled 2 times". */
export const duplicateWhyText = (m: MessageSummary): string =>
  whyParts(m).map(({ r, verb, n }) => (n === null ? DUPLICATE_REASONS[r] ?? r : `${verb} ${n} times`)).join(", ");

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

/** A message ID on one line: the first 8 and last 5 characters around an ellipsis, when it is longer than that. */
export const shortId = (id: string) => (id.length > 16 ? `${id.slice(0, 8)}\u2026${id.slice(-5)}` : id);

/** The Receives head's tooltip: what the count is, and what it is not (SQS's own receive count). */
export const RECEIVES_TITLE = "Receives Trailbeam observed, on every queue, the receive from a dead-letter queue included. "
  + "SQS's own receive count can be higher: it counts receives by consumers Trailbeam doesn't see.";

/** The Handled head's tooltip. */
export const HANDLED_TITLE = "Receives that ended in a delete, or in a Lambda record that succeeded.";

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
