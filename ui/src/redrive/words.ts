/*
  Redrive's words (QV-590): what a redrive's move task means, for the log, the confirmation and a message's
  timeline. Moved from the hosted app's lib/app/redrive-words.ts and redrive.ts (redrivenNotice) so both products
  say the same thing; the Python server keeps only next_check and its constants (server/.../redrive.py), the same
  numbers as here. No AWS SDK, so the browser can use all of it.

  SQS moves a whole dead-letter queue, oldest first. It cannot move one message, and each message moved gets a new
  message ID.
*/

/* A dead-letter queue: its name, and the account and region of the queue that sends to it, when known. */
export type DlqPlace = { name: string; account: string | null; region: string | null };

/*
  Measured on real SQS (QV-60 §2): ListMessageMoveTasks can say COMPLETED for a move that has not moved
  everything yet, or whose count is wrong. A move started about 5 s after 2 messages reached the
  dead-letter queue finished "0 of 0" and left both there; a move of 4 finished "0 of 4" with all 4 back
  on the source queue. SQS sizes a move from its approximate count, which lags. So a finished move is
  only believed once the dead-letter queue's own depth is read again, SETTLE_MS after SQS said it finished.
*/
export const SETTLE_MS = 60_000;
/* A finished move older than this, never checked, is not checked any more: its queue has moved on. */
export const CHECK_WITHIN_MS = 15 * 60_000;
const FINAL = ["COMPLETED", "CANCELLED", "FAILED"];
export const isFinal = (status: string | null) => status !== null && FINAL.includes(status);

/* The part of a log row the words below read. statusAt is when the row's status last changed; left is the dead-letter queue's depth, read at leftAt. */
export type MoveRow = { status: string | null; moved: number | null; toMove: number | null; failure: string | null; statusAt: number | null; left: number | null; leftAt: number | null };

/** What the next status read of a started move should do: read the task, read the dead-letter queue's depth, or nothing. */
export function nextCheck(l: MoveRow, now: number): "task" | "depth" | "wait" | "done" {
  if (!isFinal(l.status)) return "task";
  if (l.leftAt !== null || l.statusAt === null || now - l.statusAt > CHECK_WITHIN_MS) return "done";
  return now - l.statusAt >= SETTLE_MS ? "depth" : "wait";
}

/** "2 of 4 moved", in SQS's words: ToMove is the size SQS gave the move when it started, not what is left. */
export function counted(l: { moved: number | null; toMove: number | null }): string | null {
  if (l.moved === null) return null;
  if (l.toMove === null) return `${l.moved} moved`;
  return l.toMove === 0 && l.moved === 0 ? "SQS counted 0 messages to move" : `${l.moved} of ${l.toMove} moved`;
}

/**
  A move's FailureReason in words. Measured on real SQS (QV-60 §2): messages sent straight to a
  dead-letter queue carry no DeadLetterQueueSourceArn, so a move back to "their source" fails with
  CouldNotDetermineMessageSource and moves none of them. SQS's own code stays at the end, for searching.
*/
export function failureWords(failure: string | null, dlqName: string): string | null {
  if (!failure) return null;
  if (/CouldNotDetermineMessageSource/i.test(failure)) return `SQS did not move these messages to ${dlqName} itself, so they have no source queue to go back to. That happens when a message is sent straight to a dead-letter queue. They are still in ${dlqName}. (SQS: CouldNotDetermineMessageSource)`;
  if (/^[A-Za-z.]+$/.test(failure)) return `SQS stopped the move: ${failure}.`;
  return failure;
}

export type MoveState = "moving" | "checking" | "emptied" | "left" | "unchecked" | "stopped" | "failed";

/** The move's state as Trailbeam believes it, and one or two sentences for the log, in the queue's name. */
export function moveWords(l: MoveRow, dlqName: string, now: number): { state: MoveState; word: string; detail: string | null } {
  const c = counted(l);
  const sqsSaid = c ? ` (${c})` : "";
  // measured (QV-60 §2): a cancel at 1 message/s answered "1 moved", and the move still ended CANCELLED with 4 of 4 moved
  if (l.status === "CANCELLING") return { state: "moving", word: "Cancel requested", detail: `${l.failure ? `${l.failure} ` : ""}SQS can keep moving messages after a cancel, so the count comes from SQS once the move stops.` };
  if (!isFinal(l.status)) return { state: "moving", word: c ? `Moving, ${c}` : "Moving", detail: null };
  if (l.status === "COMPLETED") {
    if (l.leftAt === null) return nextCheck(l, now) === "done"
      ? { state: "unchecked", word: "SQS says done", detail: `SQS reported the move finished${sqsSaid}, and Trailbeam did not read ${dlqName}'s depth afterwards, so it can't say every message moved.` }
      : { state: "checking", word: "Checking", detail: `SQS reports the move finished${sqsSaid}. SQS can say that before it has moved everything, so Trailbeam reads ${dlqName}'s depth a minute later.` };
    if (l.left === 0) return { state: "emptied", word: "Done", detail: `${dlqName} was empty a minute after the move finished.${c && (l.moved === 0 || (l.toMove !== null && l.moved !== l.toMove)) ? ` SQS counted ${c.replace("SQS counted ", "")}; its counts are approximate.` : ""}` };
    const left = l.left === null ? "SQS gave no count for it" : `it still held ${l.left} ${l.left === 1 ? "message" : "messages"}`;
    return { state: "left", word: "Done, messages left", detail: `SQS reported the move finished${sqsSaid}, but a minute later ${left}. SQS sizes a move from its approximate count, which can miss messages that reached ${dlqName} just before. Redrive again to move them.` };
  }
  if (l.status === "FAILED") return { state: "failed", word: c ? `Failed, ${c}` : "Failed", detail: failureWords(l.failure, dlqName) };
  const after = l.leftAt === null ? "" : l.left === 0 ? ` ${dlqName} was empty a minute later, so every message moved before the cancel took hold.`
    : l.left !== null ? ` A minute later ${dlqName} held ${l.left} ${l.left === 1 ? "message" : "messages"}.` : "";
  return { state: "stopped", word: c ? `Cancelled, ${c}` : "Cancelled", detail: `${l.failure ?? ""}${after}`.trim() || null };
}

/**
  The warning for the confirmation when a message reached the dead-letter queue within SETTLE_MS: SQS
  sizes the move from its approximate count, and a move started 5 s after 2 messages arrived moved 0.
*/
export function freshWarning(dlqName: string, newest: number | null | undefined, now: number): string | null {
  if (newest === null || newest === undefined || now - newest >= SETTLE_MS) return null;
  const s = Math.max(1, Math.round((now - newest) / 1000));
  return `A message reached ${dlqName} ${s} s ago. SQS sizes a move from its approximate count, which can miss messages that arrived in the last minute, so wait a minute before redriving.`;
}

/* ── a redriven message on its own timeline ──────────────────────────── */

/* One row of the redrive log, as much of it as redrivenNotice reads. */
export type LoggedRedrive = MoveRow & {
  dlqName: string; accountId: string; region: string; messageIds: string[]; requestedBy: string; requestedByEmail: string | null;
  requestedAt: number; outcome: "started" | "refused" | "failed";
};

const utc = (ms: number) => new Date(ms).toISOString().slice(0, 16).replace("T", " ") + " UTC";

/**
  What the timeline of a message in a dead-letter queue says once a redrive was started on that queue.
  SQS moves the whole queue, so the message moved whether or not it was picked, if it was there when the
  move started (its last observed event is before the start). It arrives on the source queue with a new
  message ID. SQS does not report which new ID belongs to which old one, and the span contract has no
  field that carries over the move, so Trailbeam says the timeline continues under a new ID and does not
  guess which. A move SQS calls finished is only said to have moved the message once the dead-letter
  queue was seen empty a minute later (moveWords). Null when no started redrive covers the message.
*/
export function redrivenNotice(log: LoggedRedrive[], m: { messageId: string; dlq: DlqPlace; lastSeen: number }, now = Date.now()): { code: string; message: string } | null {
  const hit = log
    .filter((l) => l.outcome === "started" && l.dlqName === m.dlq.name)
    .filter((l) => (!m.dlq.account || l.accountId === m.dlq.account) && (!m.dlq.region || l.region === m.dlq.region))
    .filter((l) => l.messageIds.includes(m.messageId) || l.requestedAt >= m.lastSeen)
    .sort((a, b) => b.requestedAt - a.requestedAt)[0];
  if (!hit) return null;
  const who = hit.requestedByEmail ?? hit.requestedBy;
  const when = `${utc(hit.requestedAt)} by ${who}`;
  // measured on real SQS (QV-60 §2): 11 of 11 moved messages got new IDs, kept body and attributes, lost the trace header
  const after = "SQS gives each message it moves a new message ID. The moved message keeps its body and message attributes, but not its trace header, and its sent time and receive count start again. SQS does not say which new ID belongs to which old one, so this message's timeline continues on the source queue under an ID Trailbeam cannot link to this one yet.";
  const w = moveWords(hit, hit.dlqName, now);
  const c = counted(hit);
  const sqsSaid = c ? ` (${c})` : "";
  switch (w.state) {
    case "emptied":
      return { code: "redriven", message: `Redriven: a redrive started ${when} moved every message in ${hit.dlqName} back to its source queue, this one included. ${w.detail} ${after}` };
    case "checking": case "unchecked": case "left":
      return { code: "redriven", message: `A redrive of ${hit.dlqName} started ${when}. ${w.detail} This message may be back on its source queue under a new message ID, or still here.` };
    case "failed":
      return { code: "redriven", message: `A redrive of ${hit.dlqName} started ${when} failed${sqsSaid}.${w.detail ? ` ${w.detail}` : ""} This message may be back on its source queue under a new message ID, or still here.` };
    case "moving":
      if (hit.status === "CANCELLING") return { code: "redriven", message: `A redrive of ${hit.dlqName} started ${when} was asked to cancel. SQS can keep moving messages after a cancel, so this message may be back on its source queue under a new message ID, or still here.` };
      return { code: "redriven", message: `A redrive started ${when} is moving every message in ${hit.dlqName} back to its source queue${sqsSaid}, this one included. ${after}` };
    case "stopped":
      return { code: "redriven", message: `A redrive of ${hit.dlqName} started ${when} stopped (${String(hit.status).toLowerCase()})${sqsSaid}. SQS moves oldest first and does not say which messages moved, so this message may be back on its source queue under a new message ID, or still here.` };
    default:
      return { code: "redriven", message: `A redrive started ${when} is moving every message in ${hit.dlqName} back to its source queue${sqsSaid}, this one included. ${after}` };
  }
}
