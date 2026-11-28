import type { Timeline } from "./timeline/types.ts";
import { isRabbitMq } from "./systems.ts";
import { RETURN_CAUSES } from "./timeline/words.ts";

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;
const names = (xs: string[]) => (xs.length <= 1 ? xs.join("") : `${xs.slice(0, -1).join(", ")} and ${xs[xs.length - 1]}`);

/** The services that received the message, in the order they first did. */
export function consumersOf(t: Timeline): string[] {
  return [...new Set(t.events.flatMap((e) => (e.type === "received" && e.consumer.service_name ? [e.consumer.service_name] : [])))];
}

/**
  Why a message received twice was received again: receive 1 released with ChangeMessageVisibility to 0,
  or its visibility timeout expired. Null when the timeline shows neither, so the headline stays as it was.
*/
function againFor(t: Timeline): string | null {
  if (t.deliveries !== 2) return null;
  const second = t.events.find((e) => e.type === "received" && e.delivery === 2);
  const name = second?.type === "received" ? second.consumer.service_name : undefined;
  const by = name ? ` by ${name}` : "";
  const released = t.events.some((e) => e.type === "visibility_changed" && e.delivery === 1 && e.result === "ok" && e.visibility_timeout_s === 0);
  if (released) return `Released with ChangeMessageVisibility after receive 1, received again${by} and deleted.`;
  const expired = t.events.some((e) => e.type === "visibility_timeout_expired" && e.after_delivery === 1);
  if (expired) return `The visibility timeout expired during receive 1; received again${by} and deleted.`;
  return null;
}

const exchangeName = (x: string) => (x === "" || x === "amq.default" ? "the default exchange" : `exchange ${x}`);

/** A RabbitMQ message's sentence (QV-308), in RabbitMQ's terms: published, delivered, acked, requeued, dead-lettered. */
function amqpAnswer(t: Timeline): string {
  const consumers = consumersOf(t);
  const by = consumers.length ? ` by ${names(consumers)}` : "";
  const n = t.deliveries;
  const copies = t.copies ?? [];
  const pubs = t.publishes ?? 0;
  if (t.in_dead_letter_queue) {
    const before = t.events.filter((e) => e.type === "received" && e.queue === t.source_queue).length;
    return before
      ? `Dead-lettered to ${t.current_queue} after ${plural(before, "delivery", "deliveries")} on ${t.source_queue}.`
      : `Dead-lettered from ${t.source_queue} to ${t.current_queue}. No delivery on ${t.source_queue} was observed.`;
  }
  if (t.dead_letter_queue_move === "not_seen_on_dlq_yet") return `Returned to ${t.source_queue} more times than its x-delivery-limit allows, so RabbitMQ dead-letters it, or drops it with no dead-letter exchange. Not seen on a dead-letter queue yet.`;
  if (n === 0) {
    const returned = t.events.find((e) => e.type === "returned");
    if (returned?.type === "returned") return `Published to ${exchangeName(returned.exchange)}, and no queue took it: the broker returned it to the publisher with basic.return.`;
    return `Published to ${t.source_queue} and not delivered yet.`;
  }
  if (t.state === "deleted") {
    if (copies.length > 1) return `Routed to ${plural(copies.length, "queue")}, ${names(copies.map((c) => c.queue))}. Each queue's copy was delivered${by} and acked.`;
    if (pubs > 1) return `Published ${plural(pubs, "time")} under one message ID, delivered ${plural(n, "time")}${by} and acked.`;
    if (n === 1) return `Delivered once${by} and acked.`;
    const back = t.events.find((e) => e.type === "returned_to_queue" && e.after_delivery === 1);
    if (n === 2 && back?.type === "returned_to_queue") return `Returned to the queue after delivery 1 (${RETURN_CAUSES[back.cause] ?? back.cause}), delivered again${by} and acked.`;
    return `Delivered ${plural(n, "time")}${by}, then acked.`;
  }
  return `Delivered ${plural(n, "time")}${by}. No ack seen yet.`;
}

/** The one sentence at the top of a timeline: what happened to the message, from the timeline itself. */
export function answerFor(t: Timeline): string {
  if (isRabbitMq(t)) return amqpAnswer(t);
  const consumers = consumersOf(t);
  const by = consumers.length ? ` by ${names(consumers)}` : "";
  const n = t.deliveries;
  if (t.in_dead_letter_queue) {
    // Receives on the source queue only: the receive from the dead-letter queue is how the move was seen, not one before it.
    const before = t.events.filter((e) => e.type === "received" && e.queue === t.source_queue).length;
    return before
      ? `Moved to the dead-letter queue ${t.current_queue} after ${plural(before, "observed receive")} on ${t.source_queue}.`
      : `Moved from ${t.source_queue} to the dead-letter queue ${t.current_queue}. No receive on ${t.source_queue} was observed.`;
  }
  if (t.dead_letter_queue_move === "not_seen_on_dlq_yet") return "SQS's receive count reached its queue's maxReceiveCount. Unless the last receive deletes it, SQS moves it to the dead-letter queue (inferred); not seen there yet.";
  if (t.state === "deleted") return n === 1
      ? `Received once${by} and deleted.`
      : againFor(t) ?? `Received ${plural(n, "time")}${by}, then deleted.`;
  if (t.state === "deleted_inferred") return n === 1 ? `Handled once${by}. The Lambda trigger deleted it.` : `Received ${plural(n, "time")}${by}. The last invocation succeeded, so the Lambda trigger deleted it.`;
  if (t.state === "purged") return `${n === 0 ? `Sent to ${t.current_queue}` : `Received ${plural(n, "time")}${by}`}, then removed by a PurgeQueue call on ${t.current_queue}. No delete was seen.`;
  if (t.state === "processing") return `Probably being handled${by} now (inferred): the Lambda invocation has no outcome yet and started less than the Lambda timeout ago.`;
  if (n === 0) return `Sent to ${t.source_queue} and not received yet.`;
  return `Received ${plural(n, "time")}${by}. No delete seen yet.`;
}
