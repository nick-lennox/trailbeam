/*
  GET /api/events (contract/openapi.yaml, operation list-events): the recorded steps in a time window, newest first,
  and on the first page their counts per time bucket. The hosted app's GET /api/w/<workspace>/events answers the same.
*/

export const STEP_KINDS = ["send", "receive", "delete", "change_visibility", "outcome"] as const;
export type StepKind = (typeof STEP_KINDS)[number];

/** One recorded step. Every field is present; one the span didn't carry is null. */
export type EventStep = {
  /** when the step ended, ISO 8601 in UTC to the microsecond */
  time: string;
  time_unix_nano: string;
  start_unix_nano: string | null;
  kind: StepKind;
  message_id: string | null;
  queue: string;
  service_name: string | null;
  service_version: string | null;
  /** deletes and visibility changes: whether SQS accepted the call */
  result: "ok" | "failed" | null;
  error_code: string | null;
  /** Lambda outcomes: success, failed_exception, failed_reported or failed_invalid_response */
  outcome: string | null;
  error_type: string | null;
  receive_count: number | null;
  /** visibility changes: the new timeout, in seconds */
  visibility_timeout: number | null;
  trace_id: string;
  span_id: string;
};

export type KindCounts = Record<StepKind, number>;

export type EventHistogram = {
  bucket_ms: number;
  buckets: ({ start_ms: number } & KindCounts)[];
  totals: KindCounts;
  total: number;
};

export type EventPage = {
  from_ms: number;
  to_ms: number;
  events: EventStep[];
  next_cursor: string | null;
  /** on the first page only (no cursor) */
  histogram: EventHistogram | null;
};
