/*
  The messages behind a part of the Overview's flow map, from recorded spans: the rows PartMessages draws under a
  selected producer, consumer or dead-letter queue. A backend passes the events of the page's range (self-hosted
  Trailbeam: GET /api/timeline-events, the Event explorer's events) and gets one row per message, newest first,
  each with its whole history in the range, oldest event first. Pure.
*/
import type { Ev } from "./events.ts";

/** One message under a part of the flow map, with its whole history so far, oldest event first. */
export type PartMessage = {
  id: string;
  producer: string;
  receiveCount: number;
  sentMs: number;
  /* the time of its newest event, which a dead-lettered row is listed by */
  lastMs: number;
  outcome: "visible" | "in_flight" | "deleted" | "failed" | "dead_lettered";
  lastError?: string;
  events: Ev[];
  payload?: unknown;
};

/** Which part: a producer of the queue, a consumer of it, or its dead-letter queue. `queue` is the queue's name as events carry it. */
export type PartOf = { queue: string; part: "producer" | "consumer" | "dlq"; name?: string };

/** A message's outcome so far, from its events oldest first. */
export function outcomeOf(evs: readonly Ev[]): PartMessage["outcome"] {
  if (evs.some((e) => e.event === "dead_letter")) return "dead_lettered";
  if (evs.some((e) => e.event === "delete" && e.status === "ok")) return "deleted";
  const last = evs[evs.length - 1];
  if (!last) return "visible";
  if (last.event === "processing_error" || last.status === "error") return "failed";
  if (last.event === "receive" || last.event === "visibility_change") return "in_flight";
  return "visible";
}

/* when the row's first column says it happened: dead-lettered at its newest event, sent at its send, received at its first receive by the consumer */
function shownAt(m: PartMessage, p: PartOf): number {
  if (p.part === "dlq") return m.lastMs;
  if (p.part === "producer") return m.sentMs;
  return m.events.find((e) => e.event === "receive" && (!p.name || e.consumer === p.name))?.ms ?? m.sentMs;
}

/** One row per message of `queue` that the part handled in the events, newest first. */
export function partMessagesOf(events: readonly Ev[], p: PartOf): PartMessage[] {
  const byId = new Map<string, Ev[]>();
  for (const e of events) {
    if (e.queue !== p.queue || !e.messageId) continue;
    const list = byId.get(e.messageId);
    if (list) list.push(e); else byId.set(e.messageId, [e]);
  }
  const rows: PartMessage[] = [];
  for (const [id, list] of byId) {
    /* the server lists events newest first: reversed, events at the same millisecond keep their order */
    const evs = [...list].reverse().sort((a, b) => a.ms - b.ms);
    const mine = p.part === "dlq" ? evs.some((e) => e.event === "dead_letter")
      : p.part === "producer" ? evs.some((e) => e.event === "send" && e.producer === p.name)
      : evs.some((e) => e.event === "receive" && e.consumer === p.name);
    if (!mine) continue;
    const send = evs.find((e) => e.event === "send");
    /* the newest processing error; a move to the dead-letter queue is an error event too, but no error of the handler */
    const back = [...evs].reverse();
    const failed = back.find((e) => e.event === "processing_error") ?? back.find((e) => e.status === "error" && e.event !== "dead_letter");
    const body = evs.find((e) => e.body !== undefined)?.body;
    rows.push({
      id,
      producer: send?.producer || evs.find((e) => e.producer)?.producer || "",
      receiveCount: evs.reduce((n, e) => Math.max(n, e.receiveCount ?? 0), 0),
      sentMs: send?.ms ?? evs[0].ms,
      lastMs: evs[evs.length - 1].ms,
      outcome: outcomeOf(evs),
      ...(failed?.detail ? { lastError: failed.detail } : {}),
      events: evs,
      ...(body !== undefined ? { payload: body } : {}),
    });
  }
  return rows.sort((a, b) => shownAt(b, p) - shownAt(a, p) || (a.id < b.id ? -1 : 1));
}
