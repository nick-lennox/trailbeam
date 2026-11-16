/*
  The Body tab's data: Trailbeam's GET /api/messages/{id}/body (contract/body-view.md). Self-hosted
  Trailbeam returns it as is; the hosted app builds the same shape with bodyView (./rules.ts).
*/

/** Why a message has no body, the first that applies (contract/body-view.md). */
export type BodyReason =
  | "not_opted_in" | "producer_not_instrumented" | "package_too_old"
  | "off" | "md5_mismatch" | "redaction_failed" | "exceeds_capture_limit" | "unknown";

/** The one body kept for a message, as stored: after the package redacted it. */
export type StoredBody = {
  body: string;
  format: string | null; // "json" | "text"
  redacted_count: number | null;
  envelope: string | null; // "sns" | "eventbridge"
  from_kind: "send" | "receive";
  service: string | null;
  queue: string;
  trace_id: string;
  span_id: string;
  time_unix_nano: string;
};

export type BodyFound = {
  message_id: string; kind: "body"; body: StoredBody;
  /** messaging.message.body.size: before redaction */
  size: number | null; tenant: string | null; tenant_source: string | null;
};

export type BodyMissing = {
  message_id: string; kind: "none"; reason: BodyReason; size: number | null;
  sends: number; receives: number;
  /** trailbeam.body.capture_status of the first send with one, and of the last receive with one */
  send_status: string | null; receive_status: string | null;
};

export type MessageBody = BodyFound | BodyMissing;
