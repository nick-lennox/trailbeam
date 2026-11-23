/*
  The Body tab's rules (contract/body-view.md), for a server that keeps its own store: which body a
  message keeps, and why it has none. Self-hosted Trailbeam implements the same rules in Python
  (python/src/trailbeam/server/bodies.py); both are checked against contract/fixtures/body-views.json.
*/
import type { BodyReason, MessageBody, StoredBody } from "./types.ts";

/** What one span of the message said about its body (contract 15.2). Pass them in time order. */
export type BodySpan = {
  kind: string; // send | receive | outcome | ...
  captureStatus: string | null;
  bodySize: number | null;
  /** trailbeam.body.offloaded_size; optional for a store that doesn't keep it */
  offloadedSize?: number | null;
  tenant: string | null;
  tenantSource: string | null;
  /** it carried a body the server did not keep */
  bodyDropped: boolean;
  traceId: string;
  spanId: string;
};

const STATUS_REASONS = new Set<string>(["off", "md5_mismatch", "redaction_failed", "exceeds_capture_limit"]);

/**
  The Body tab of one message. kept: the message's one kept body (the first send's, else the first
  receive's), or null. spans: its spans that carry contract 3.2 attributes, in time order. counts: its
  send and receive spans, counting those with no 3.2 attribute (an older package).
*/
export function bodyView(messageId: string, kept: StoredBody | null, spans: BodySpan[], counts: { sends: number; receives: number }): MessageBody {
  const ordered = [...spans.filter((s) => s.kind === "send"), ...spans.filter((s) => s.kind !== "send")];
  const size = ordered.find((s) => s.bodySize !== null)?.bodySize ?? null;
  if (kept) {
    const from = spans.find((s) => s.traceId === kept.trace_id && s.spanId === kept.span_id);
    const offloaded = from?.offloadedSize ?? ordered.find((s) => (s.offloadedSize ?? null) !== null)?.offloadedSize ?? null;
    const t = ordered.find((s) => s.tenant !== null);
    return {
      message_id: messageId, kind: "body", body: { ...kept, capture_status: kept.capture_status ?? null },
      size: from?.bodySize ?? size, offloaded_size: offloaded, tenant: t?.tenant ?? null, tenant_source: t?.tenantSource ?? null,
    };
  }
  const send_status = spans.find((s) => s.kind === "send" && s.captureStatus)?.captureStatus ?? null;
  const receive_status = spans.filter((s) => s.kind === "receive" && s.captureStatus).at(-1)?.captureStatus ?? null;
  const reason: BodyReason = spans.some((s) => s.bodyDropped) ? "not_opted_in"
    : counts.sends > 0 ? (!send_status ? "package_too_old" : STATUS_REASONS.has(send_status) ? (send_status as BodyReason) : "unknown")
    : "producer_not_instrumented";
  return { message_id: messageId, kind: "none", reason, size, sends: counts.sends, receives: counts.receives, send_status, receive_status };
}

/** Each reason in words. Each product adds a hint saying where its own server setting is. */
export const NO_BODY_TEXT: Record<BodyReason, string> = {
  not_opted_in: "The package captured this body, but this Trailbeam doesn't keep bodies for this queue, so it isn't shown. "
    + "Either it was dropped when it arrived, or it arrived before the queue was taken off the list and is still stored, only hidden.",
  producer_not_instrumented: "The producer runs no Trailbeam package, so there is no send, and no receive's copy of the body was kept.",
  package_too_old: "The package that sent this message is from before contract revision 3.2, which added payload capture. Update it to capture bodies.",
  off: "Payload capture is off for this queue in the producer's package: the queue is not in its body queue list (TRAILBEAM_CAPTURE_BODY_QUEUES).",
  md5_mismatch: "The body the package held did not match SQS's MD5 for the message, so it was not exported.",
  redaction_failed: "Redaction failed on this body, so the package did not export it. It fails closed.",
  exceeds_capture_limit: "The body, after redaction, is larger than the package's capture limit. It is not exported, and not truncated.",
  unknown: "No body was captured for this message.",
};

/** What else the spans say, under the reason. */
export function noBodyNotes(v: Extract<MessageBody, { kind: "none" }>): string[] {
  if (v.reason === "unknown" && v.send_status) return [`The send's capture status is ${v.send_status}.`];
  if (v.reason !== "producer_not_instrumented") return [];
  if (v.receives > 0 && !v.receive_status) return ["The consumer's package is also from before contract revision 3.2."];
  if (v.receive_status === "off") return ["Capture is off for this queue in the consumer's configuration too."];
  if (v.receive_status) return [`The last receive's capture status is ${v.receive_status}.`];
  return [];
}
