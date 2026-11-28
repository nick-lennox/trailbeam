/*
  "In ‹queue› now": the instrumented messages that are in a queue right now, each visible or in flight, and how they
  compare with what SQS says the queue holds. The shape both backends give: the hosted app's in-queue.ts (loadInQueue)
  and the Python server's GET /api/queues/{queue}/now (trailbeam.server.queue_now). Plain data, so the page
  (QueueNowLive) reads it in the browser. How each value is worked out from the spans is said where it is built.
*/
import type { Ev } from "./events.ts";

/** SQS's VisibilityTimeout when a queue's own setting was not read. */
export const SQS_DEFAULT_VISIBILITY_S = 30;

export type QueueConfig = {
  visibilityTimeoutS: number | null;
  retentionS: number | null;
  maxReceiveCount: number | null;
  dlq: string | null;
  arn: string | null;
  lambdaTimeoutS?: number;
};

export type HeldMessage = {
  id: string;
  producer: string;
  consumer: string;
  receiveCount: number;
  state: "visible" | "in_flight";
  sentMs: number;
  lastReceiveMs: number | null;
  /* in flight: when the visibility timeout runs out, how long is left, and the share used */
  untilMs: number | null;
  leftS: number;
  used: number;
  /* the visibility timeout that applies: the queue's, or one a ChangeMessageVisibility set */
  timeoutS: number | null;
  lastError?: string;
  events: Ev[];
};

/* a message at maxReceiveCount whose last visibility timeout ran out: inferred to be moved on the next receive attempt, not seen
   on the dead-letter queue yet. timedOutMs is when that timeout ran out, not a move time */
export type NotSeenOnDlq = { id: string; timedOutMs: number; receiveCount: number; producer: string; consumer: string; sentMs: number; events: Ev[] };
export type Deleted = { id: string | null; atMs: number };

export type InQueueView = {
  nowMs: number;
  config: QueueConfig & { visibilityKnown: boolean };
  held: HeldMessage[];
  /* more messages matched than were read */
  capped: boolean;
  instrumented: { visible: number; inFlight: number };
  /* what SQS says the queue holds, when an AWS read gave it */
  totals: { visible: number; inFlight: number; delayed: number | null } | null;
  /* totals minus instrumented, never below zero; over: spans account for more than SQS holds */
  uninstrumented: { visible: number; inFlight: number; over: boolean } | null;
  /* inferred, never counted as dead-lettered */
  notSeenOnDlq: NotSeenOnDlq[];
  /* receives on the dead-letter queue that named this queue as the source, by message, in the past hour: the moves counted */
  seenOnDlq: number;
  deleted: Deleted[];
  producers: { name: string; n: number }[];
};

/** "Receive n was not deleted …", said as what it is: inferred from no delete being seen by the time the timeout ran out. */
export function notDeletedText(n: number, defaultTimeout: boolean, why = "no delete of it was stored"): string {
  return `Inferred: receive ${n} was not deleted before its visibility timeout ran out (${why}${defaultTimeout ? `; the timeout used is SQS's default ${SQS_DEFAULT_VISIBILITY_S} s, as the queue's setting was not read` : ""})`;
}

/** A message at maxReceiveCount whose last timeout ran out (contract 9b): inferred to move on the next receive attempt, not yet seen. */
export function notSeenOnDlqText(n: number, max: number): string {
  return `Inferred: receive count ${n} reached maxReceiveCount ${max}, so SQS moves it on the next receive attempt; not seen on the DLQ yet`;
}

/** A message as it is at nowMs: its visibility timeout counted down, or run out. A timeout that runs out on the page's clock is
    inferred: the delete may simply not have reached the page yet, and the timeout may be SQS's default (defaultTimeout). */
export function heldAt(m: HeldMessage, nowMs: number, max: number | null, defaultTimeout = false): HeldMessage & { left?: boolean } {
  if (m.state !== "in_flight" || m.untilMs === null) return m;
  if (nowMs < m.untilMs) {
    const total = (m.timeoutS ?? 0) * 1000;
    return { ...m, leftS: Math.max(0, Math.ceil((m.untilMs - nowMs) / 1000)), used: total ? Math.min(1, 1 - (m.untilMs - nowMs) / total) : 1 };
  }
  if (max !== null && m.receiveCount >= max) return { ...m, left: true, lastError: notSeenOnDlqText(m.receiveCount, max) };
  return { ...m, state: "visible", leftS: 0, used: 0, lastError: m.lastError ?? notDeletedText(m.receiveCount, defaultTimeout, "its timeout ran out on this page's clock, and no delete has reached the page; one may still arrive") };
}
