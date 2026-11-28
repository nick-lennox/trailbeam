/*
  The Messages list's columns: what each is called, how its cell is drawn in the explorer's event-list style, and its
  text for an export. A column no row fills (In queue and End to end, on a list API that doesn't measure them) is left
  out of the table and the column list, as the Event explorer leaves out its empty columns. A host adds its own
  columns at the end (Why on Duplicates, Age on Stuck).
*/
import { SystemBadge } from "../design/SystemBadge.tsx";
import { isRabbitMq, shownId } from "../systems.ts";
import type { ReactNode } from "react";
import { StatePill, purgeText, stateLook } from "../design/pills.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { fmtMs } from "../model/format.ts";
import { HANDLED_TITLE, RECEIVES_TITLE, duplicateWhy, duplicateWhyText, fmtTime, shortId, type MessageSummary } from "./Messages.tsx";

/** A row of the list: a list API's message, with the times the hosted list measures (lib/app/message-times.ts). */
export type MessageRow = MessageSummary & {
  /** from the send to the end of the first receive; with waiting, the time so far of a message nobody received yet */
  in_queue_ms?: number | null;
  waiting?: boolean;
  /** from first seen to the delete that finished it; null while not finished */
  end_to_end_ms?: number | null;
  /** "sqs": the send wasn't observed, so the time in queue starts at SQS's SentTimestamp */
  clock?: "host" | "sqs";
};

export type MessageColumn = {
  id: string;
  label: string;
  cell: (m: MessageRow) => ReactNode;
  /** the cell in full, as an export writes it */
  text: (m: MessageRow) => string;
  align?: "right";
  /** the head's note, where the word alone needs explaining */
  tip?: string;
  /** whether a row has a value here; a column no row fills is left out */
  fills?: (m: MessageRow) => boolean;
};

const Muted = ({ children }: { children: ReactNode }) => <span className="font-sans text-ink-3">{children}</span>;

const queueOf = (m: MessageRow) => m.current_queue || m.source_queue || "?";
/** What the queue cell says after the name: in a dead-letter queue, or a move SQS should have made by now. */
function queueNote(m: MessageRow): string {
  if (m.in_dead_letter_queue) return `dead-letter queue of ${m.source_queue || "?"}`;
  if (m.dead_letter_queue_move === "not_seen_on_dlq_yet") return isRabbitMq(m)
    ? "passed x-delivery-limit; the move to a dead-letter queue isn't seen yet"
    : "reached maxReceiveCount; the move to the dead-letter queue isn't seen yet";
  return "";
}

const consumerNames = (m: MessageRow) => [...new Set(m.consumers.map((c) => c.service_name ?? "unnamed service"))];

const SQS_CLOCK = "SQS's SentTimestamp, because the send wasn't observed";

function inQueue(m: MessageRow): ReactNode {
  if (m.in_queue_ms === null || m.in_queue_ms === undefined) return <Muted>none</Muted>;
  const v = <>{fmtMs(Math.round(m.in_queue_ms))}{m.waiting && <Muted> so far</Muted>}</>;
  const tip = m.waiting ? "Not received yet: the time since it was sent" : m.clock === "sqs" ? `From ${SQS_CLOCK}` : undefined;
  return tip ? <Tooltip content={tip} tabIndex={0}>{v}</Tooltip> : v;
}

const time = (t: string | null) => (t ? <time dateTime={t} className="tabular-nums">{fmtTime(t)}</time> : <Muted>unknown</Muted>);

/** Every column the list has, in the order they are offered. */
export const MESSAGE_COLUMNS: MessageColumn[] = [
  {
    id: "message_id", label: "Message ID",
    // a RabbitMQ message: its ID as the application set it, and the system's badge (QV-308)
    cell: (m) => {
      const id = shownId(m.message_id);
      return <><Tooltip content={id.length > 16 ? id : undefined} className="text-ink">{shortId(id)}</Tooltip>{isRabbitMq(m) && <SystemBadge system="rabbitmq" className="ml-2" />}</>;
    },
    text: (m) => shownId(m.message_id),
  },
  { id: "state", label: "State", cell: (m) => <StatePill m={m} />, text: (m) => [stateLook(m).label, purgeText(m)].filter(Boolean).join(". ") },
  {
    id: "queue", label: "Queue",
    cell: (m) => { const note = queueNote(m); return <>{queueOf(m)}{note && <Muted> ({note})</Muted>}</>; },
    text: (m) => { const note = queueNote(m); return note ? `${queueOf(m)} (${note})` : queueOf(m); },
  },
  {
    id: "consumers", label: "Consumers",
    cell: (m) => { const c = consumerNames(m); return c.length ? <span className="font-sans text-ink-2">{c.join(", ")}</span> : <Muted>None yet</Muted>; },
    text: (m) => consumerNames(m).join(", "),
  },
  {
    id: "receives", label: "Receives", align: "right", tip: RECEIVES_TITLE,
    cell: (m) => <span className={`tabular-nums ${m.deliveries > 1 ? "font-medium text-warning-text" : ""}`}>{m.deliveries}</span>,
    text: (m) => String(m.deliveries),
  },
  { id: "handled", label: "Handled", align: "right", tip: HANDLED_TITLE, cell: (m) => <span className="tabular-nums">{m.successful_deliveries}</span>, text: (m) => String(m.successful_deliveries) },
  {
    id: "in_queue", label: "In queue", align: "right", cell: inQueue, fills: (m) => m.in_queue_ms !== undefined,
    text: (m) => (m.in_queue_ms === null || m.in_queue_ms === undefined ? "" : `${fmtMs(Math.round(m.in_queue_ms))}${m.waiting ? " so far" : ""}`),
  },
  {
    id: "end_to_end", label: "End to end", align: "right", fills: (m) => m.end_to_end_ms !== undefined,
    cell: (m) => (m.end_to_end_ms === null || m.end_to_end_ms === undefined ? <Muted>Not finished</Muted> : fmtMs(Math.round(m.end_to_end_ms))),
    text: (m) => (m.end_to_end_ms === null || m.end_to_end_ms === undefined ? "" : fmtMs(Math.round(m.end_to_end_ms))),
  },
  {
    id: "first_seen", label: "First seen",
    cell: (m) => <>{time(m.first_seen)}{m.first_seen_clock === "sqs" && <> <Tooltip content={SQS_CLOCK} tabIndex={0} className="font-sans text-ink-3">SQS clock</Tooltip></>}</>,
    text: (m) => `${fmtTime(m.first_seen) ?? ""}${m.first_seen_clock === "sqs" ? " (SQS clock)" : ""}`,
  },
  { id: "last_seen", label: "Last seen", cell: (m) => time(m.last_seen), text: (m) => fmtTime(m.last_seen) ?? "" },
];

export const DEFAULT_MESSAGE_COLUMNS = MESSAGE_COLUMNS.map((c) => c.id);

/** Duplicates' own column: why each message is listed, with its count ("received 3 times, handled 2 times"). */
export const WHY_COLUMN: MessageColumn = {
  id: "why", label: "Why", cell: (m) => <span className="font-sans text-ink-2">{duplicateWhy(m)}</span>, text: duplicateWhyText,
  fills: (m) => !!m.reasons?.length,
};

/* A row's level, on the bar at its left edge as in the Event explorer: red in a dead-letter queue, amber when received more
   than once or due on the DLQ. */
export function messageLevel(m: MessageRow): { level: "info" | "warn" | "error"; word: string } {
  if (m.in_dead_letter_queue) return { level: "error", word: "In a dead-letter queue" };
  if (m.dead_letter_queue_move === "not_seen_on_dlq_yet") return { level: "warn", word: "Due on the dead-letter queue" };
  if (m.deliveries > 1) return { level: "warn", word: isRabbitMq(m) ? "Delivered more than once" : "Received more than once" };
  return { level: "info", word: "" };
}
