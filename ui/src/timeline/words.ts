/*
  The timeline's words and small rules, without the view: the main entry's (@trailbeam/ui), so a server
  module can import them. The view itself (Timeline.tsx) draws its hover notes with the design system's
  Tooltip, so it is reached through @trailbeam/ui/design (test/server-import.test.ts).
*/
import type { BlindSpot, Timeline, TimelineEvent } from "./types.ts";

/** Each order source, as a reader sees it. Proposed (paid) sources are marked. */
export const ORDER_SOURCE: Record<string, { label: string; long: string; proposed?: boolean; placement?: boolean }> = {
  clock: { label: "host clocks", long: "One step ended before the other started, on the hosts' clocks." },
  process_order: { label: "call order, one process", long: "Both calls were made by the same process, and one returned before the other was issued." },
  receive_count: { label: "receive count", long: "SQS's receive count went up between the two receives." },
  sqs_timestamps: { label: "SQS timestamps", long: "SQS's own SentTimestamp is before its ApproximateFirstReceiveTimestamp." },
  inferred_from: { label: "inferred from neighbors", long: "The step has no time. It is placed by the observed steps it was inferred from.", placement: true },
  sns_envelope: { label: "SNS envelope", long: "The SNS envelope on the receive names the topic. It gives the publish no time, so it sits before that receive.", placement: true },
  sqs_dlq_source: { label: "SQS DLQ report", long: "SQS reported the move on the dead-letter receive. It gives the move no time, so it sits between that receive and the last one before it.", placement: true },
  amqp_x_death: { label: "x-death header", long: "RabbitMQ reported the move: the delivery from the dead-letter queue carries an x-death header naming the queue it came from. It gives the move no time, so it sits between that delivery and the last one before it.", placement: true },
  invocation_id: { label: "same Lambda request ID", long: "The AWS record names the same Lambda request ID as a Trailbeam span.", proposed: true },
  cloudtrail_time: { label: "CloudTrail time, whole seconds", long: "CloudTrail's eventTime is in whole seconds, so it proves order only when steps are more than a second apart.", proposed: true },
};

/** A RabbitMQ message's order sources, where they differ from SQS's (QV-308): the count RabbitMQ gives a delivery. */
export const ORDER_SOURCE_RABBITMQ: Record<string, { label: string; long: string; placement?: boolean }> = {
  ...ORDER_SOURCE,
  receive_count: { label: "receive count", long: "The delivery's count went up between the two deliveries on one queue: x-delivery-count, x-acquired-count or redelivered, as RabbitMQ gave them." },
};

export const BLIND_SPOTS: Record<BlindSpot, string> = {
  entry_id_is_message_id:
    "This record is tied to the message only because Lambda puts the message ID in the batch entry ID. AWS documents that field as chosen by the caller and doesn't promise it. If Lambda changes it, this step goes away.",
  separate_roles:
    "The function is named only because each function reading this queue has its own IAM role. With a shared role, CloudTrail can't say which function's trigger made the delete.",
  receives_not_in_cloudtrail:
    "CloudTrail never records receives, so it can't show when Lambda received the message, or how many times.",
  no_per_message_cause:
    "AWS gives no per-message cause for throttling or a recursion stop. The metric shows something happened to the function then. It doesn't show it happened to this message.",
  logs_end_not_cause:
    "The log says how the invocation ended, not what the handler was doing when it did.",
  dlq_not_verified_on_real_sqs:
    "Observed dead-letter moves are not yet verified on real SQS.",
};

/** "02:14:07.118", or "02:40:05" for whole-second times. */
export function clock(iso: string, precision: "s" | "ms" = "ms"): string {
  const t = iso.slice(11, 23);
  return precision === "s" ? t.slice(0, 8) : t;
}

/** The step's trace in the team's tracing tool (TRAILBEAM_TRACE_LINK), when there is one. Only http(s) links. */
export function traceUrl(e: TimelineEvent): string | null {
  const url = "span" in e ? e.span?.trace_url : undefined;
  return url && /^https?:\/\//i.test(url) ? url : null;
}

/** What a recorded PurgeQueue call says about the message, after its state: "(observed: PurgeQueue at …)". */
export function purgeNote(t: Pick<Timeline, "purge">): string | null {
  const p = t.purge;
  if (!p) return null;
  return p.status === "purged" ? `(observed: PurgeQueue at ${clock(p.start)})` : `possibly purged (PurgeQueue at ${clock(p.start)})`;
}

export const STATE_WORDS: Record<Timeline["state"], string> = {
  deleted: "Deleted",
  deleted_inferred: "Deleted (inferred)",
  processing: "Being handled (inferred)",
  not_deleted: "No delete seen",
  purged: "Purged",
};

/** A RabbitMQ message's state (QV-307's words): done when every queue's copy was acked, refused or dead-lettered. */
export const STATE_WORDS_RABBITMQ: Record<Timeline["state"], string> = {
  ...STATE_WORDS,
  deleted: "Done",
  processing: "Being handled (inferred)",
  not_deleted: "No ack seen",
};
export const DONE_NOTE_RABBITMQ = "Done: every queue's copy was acked, refused or moved to a dead-letter queue, and the copy on the dead-letter queue, if any, was acked or refused.";

/** What caused a RabbitMQ message's return to its queue (contract A10), as the step names it. */
export const RETURN_CAUSES: Record<string, string> = {
  requeued: "requeued by the consumer",
  channel_closed: "its channel closed before an ack",
  consumer_timeout: "consumer timeout (consumer_timeout)",
  consumer_cancelled: "the broker cancelled the consumer",
  not_observed: "cause not observed",
};

/** Why a RabbitMQ queue dead-lettered a message, by x-death reason (https://www.rabbitmq.com/docs/dlx). */
export const DEAD_LETTER_REASONS: Record<string, string> = {
  rejected: "a consumer refused it with basic.reject or basic.nack with requeue = false",
  expired: "its time to live (TTL) ran out",
  maxlen: "the queue was over its length limit (x-max-length)",
  delivery_limit: "it was returned to the queue more times than x-delivery-limit allows",
};

/** How a queue's copy of a RabbitMQ message ended (contract A6). */
export const COPY_ENDED: Record<string, string> = {
  acked: "Acked",
  rejected: "Refused, not requeued",
  moved: "Moved to a dead-letter queue",
};
export const COPY_OPEN = "Not settled yet";

/** A server notice's docs link, by its code. */
export const NOTICE_LINKS: Record<string, [string, string]> = {
  default_key: ["Why the key matters", "https://github.com/nick-lennox/trailbeam#add-it-to-your-services"],
};
