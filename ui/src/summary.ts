import type { Timeline } from "./timeline/types.ts";

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;
const names = (xs: string[]) => (xs.length <= 1 ? xs.join("") : `${xs.slice(0, -1).join(", ")} and ${xs[xs.length - 1]}`);

/** The services that received the message, in the order they first did. */
export function consumersOf(t: Timeline): string[] {
  return [...new Set(t.events.flatMap((e) => (e.type === "received" && e.consumer.service_name ? [e.consumer.service_name] : [])))];
}

/** The one sentence at the top of a timeline: what happened to the message, from the timeline itself. */
export function answerFor(t: Timeline): string {
  const consumers = consumersOf(t);
  const by = consumers.length ? ` by ${names(consumers)}` : "";
  const n = t.deliveries;
  if (t.in_dead_letter_queue) return `Moved to the dead-letter queue ${t.current_queue} after ${plural(n, "receive")}.`;
  if (t.dead_letter_queue_move === "not_seen_on_dlq_yet") return `Received ${plural(n, "time")}, more than its queue's maxReceiveCount. Not seen on the dead-letter queue yet.`;
  if (t.state === "deleted") return n === 1 ? `Received once${by} and deleted.` : `Received ${plural(n, "time")}${by}, then deleted.`;
  if (t.state === "deleted_inferred") return n === 1 ? `Handled once${by}. The Lambda trigger deleted it.` : `Received ${plural(n, "time")}${by}. The last invocation succeeded, so the Lambda trigger deleted it.`;
  if (t.state === "processing") return `Being handled${by} now.`;
  if (n === 0) return `Sent to ${t.source_queue} and not received yet.`;
  return `Received ${plural(n, "time")}${by} and not deleted.`;
}
