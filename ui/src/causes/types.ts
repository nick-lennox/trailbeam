/*
  Stuck messages grouped by cause, as GET /api/causes returns them (contract/openapi.yaml, CausePage). The
  group is the hosted app's (trailbeam-app lib/app/causes.ts, CauseGroup) field for field, and the
  dead-letter queue is its DlqGroup, so one view reads either product's. Both make the same groups of the
  same timelines: test/fixtures/causes-parity.json.
*/
import type { MessageSummary } from "../messages/Messages.tsx";

export type CauseKind = "filter" | "fn_timeout" | "fn_oom" | "exc" | "invalid_batch_response" | "reported_failure" | "no_outcome"
  | "delete_failed" | "not_deleted" | "in_flight" | "fifo_blocked" | "never_received";
/** what a cause rests on: Trailbeam's spans, a function's log, CloudTrail, or an inference from what is missing */
export type Basis = "spans" | "function logs" | "CloudTrail" | "inferred";
export type CauseOutcome = "dead_lettered" | "stuck" | "filtered";

/** a consumer service.version the cause was seen on: how many messages, and when its evidence was first and last seen (ms) */
export type VersionSeen = { version: string; count: number; firstMs: number; lastMs: number };

export type CauseGroup = {
  /** the first 12 hex characters of the SHA-256 of `key`: the same in both products */
  id: string;
  key: string;
  kind: CauseKind;
  signature: string;
  label: string;
  basis: Basis;
  inferred: boolean;
  /** why an inferred cause is thought so */
  why: string | null;
  queue: string;
  queueKey: string;
  consumer: string;
  /** the worst outcome among its messages */
  outcome: CauseOutcome;
  count: number;
  counts: Record<CauseOutcome, number>;
  /** of counts.dead_lettered, those inferred from maxReceiveCount (not seen on the DLQ yet) */
  dlqInferred: number;
  /** earliest and latest evidence of the cause, ms */
  firstMs: number;
  lastMs: number;
  /** every message's evidence time, ms, for a trend over any window */
  times: number[];
  tenants: string[];
  /** the newest raw error text (as stored, redacted), and the message it is from */
  sampleError: string | null;
  sampleMid: string;
  /** up to 50 messages, newest evidence first */
  examples: string[];
  /** earlier deliveries' failures, in words, with how many deliveries had them */
  earlier: { what: string; n: number }[];
  /** messages still on their queue whose latest delivery failed, received twice or more: being retried */
  retrying: number;
  versions: VersionSeen[];
};

/** Messages in one dead-letter queue. `newest`: the latest time any of them was seen, ms, or null. */
export type DlqGroup = {
  key: string;
  name: string;
  account: string | null;
  region: string | null;
  messageIds: string[];
  sources: string[];
  newest: number | null;
};

/** A stuck message with its group's `id`. */
export type CauseMessage = MessageSummary & { cause: string };

/** GET /api/causes */
export type CausePage = {
  older_than_s: number;
  from_ms: number | null;
  to_ms: number | null;
  /** more than `read_limit` messages are stuck: the groups are of the `read_limit` oldest */
  capped: boolean;
  read_limit: number;
  /** stuck messages in the groups (in the range, when there is one) */
  total: number;
  in_dead_letter_queue: number;
  groups: CauseGroup[];
  dead_letter_queues: DlqGroup[];
  /** the `cause` asked for */
  cause: string | null;
  /** messages of that cause (every one when it has none), of which `messages` holds up to `limit`, oldest first */
  listed: number;
  messages: CauseMessage[];
};
