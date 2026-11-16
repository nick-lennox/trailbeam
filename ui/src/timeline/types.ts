/*
  The shape of one message's timeline, as Trailbeam returns it from
  GET /api/messages/{message_id} (trailbeam: python/src/trailbeam/server/timeline.py, https://docs.trailbeam.app/reference/api).

  Everything below the "PROPOSED" line is not in Trailbeam yet. It is the paid app's
  AWS-side view (slice 1, screen 3), drawn here so the design can be reviewed. Those
  fields are a design proposal for the API, not a contract.
*/

/** The four facts that can prove one observed step came before another (order.py SOURCES). */
export type OrderSource = "clock" | "process_order" | "receive_count" | "sqs_timestamps";
/** How a step without a time of its own is placed: by its anchors (inferred) or by SQS's DLQ report. */
export type PlacementSource = "inferred_from" | "sqs_dlq_source";

export type Consumer = {
  service_name?: string;
  client_id?: string;
  source?: "lambda" | "worker" | string;
  pid?: number;
  thread_name?: string;
  faas_name?: string;
  faas_invocation_id?: string;
};

export type AfterEntry = {
  event: string;
  type: string;
  sources: (OrderSource | PlacementSource | ProposedOrderSource)[];
  source: OrderSource | PlacementSource | ProposedOrderSource;
  why: string;
};

type Observed = {
  id: string;
  inferred: false;
  group: number;
  time: string;
  start: string | null;
  end: string;
  start_observed: boolean;
  queue: string;
  consumer: Consumer;
  after: AfterEntry[];
};

export type SentEvent = Observed & { type: "sent"; batch_entry_id?: string };
export type ReceivedEvent = Observed & {
  type: "received";
  delivery: number;
  receive_count: number;
  sent_timestamp?: string;
  first_receive_timestamp?: string;
  dlq_source_arn?: string;
};
export type DeletedEvent = Observed & {
  type: "deleted";
  delivery: number | null;
  result: "ok" | "failed" | "error";
  error_code?: string;
  status: "final" | "stale_handle" | "failed" | "unmatched_handle";
};
/** ChangeMessageVisibility on one delivery's receipt handle. A timeout of 0 releases the message at once. */
export type VisibilityChangedEvent = Observed & {
  type: "visibility_changed";
  delivery: number | null;
  result: "ok" | "failed";
  error_code?: string;
  visibility_timeout_s: number | null;
};
export type LambdaOutcomeEvent = Observed & {
  type: "lambda_outcome";
  /** contract 4.4: success, or how it failed (the handler threw; listed in batchItemFailures; an invalid batchItemFailures) */
  outcome: "success" | "failed_exception" | "failed_reported" | "failed_invalid_response";
  error_type?: string;
  /** contract 15.5 (3.2): the thrown error's message, redacted in the package, for queues with error text turned on */
  error_message?: string;
  delivery?: number;
};

type Windowed = {
  id: string;
  /** null for a step placed alone between two groups (an inferred step, or the DLQ move) */
  group: number | null;
  time: null;
  window: { from: string | null; to: string | null };
  reason: string;
  queue: string;
  after: AfterEntry[];
  before?: AfterEntry;
};

export type InferredEvent = Windowed & {
  inferred: true;
  type: "visibility_timeout_expired" | "deliveries_not_observed" | "deleted_by_lambda" | "lambda_invocation_ended_without_outcome";
  delivery?: number;
  count?: number;
  after_delivery?: number;
  before_delivery?: number;
};

/** Observed, but with no time: SQS reports the move on the dead-letter receive (contract 9b). */
export type DlqMoveEvent = Windowed & {
  inferred: false;
  type: "moved_to_dead_letter_queue";
  source: "sqs_dlq_source";
  reported_by: string;
  source_arn: string;
  reported_on_delivery: number;
  from_queue: string;
  to_queue: string;
};

export type Group = {
  group: number;
  concurrent: boolean;
  label: "concurrent, order unknown" | null;
  observed_events: number;
  inferred_events: number;
  ordered_inside: number;
  start: string | null;
  end: string | null;
};

export type Notice = { code: string; message: string };

export type Timeline = {
  message_id: string;
  state: "deleted" | "deleted_inferred" | "processing" | "not_deleted";
  source_queue: string;
  current_queue: string;
  in_dead_letter_queue: boolean;
  dead_letter_queue_move: "observed" | "not_seen_on_dlq_yet" | null;
  send_observed: boolean;
  deliveries: number;
  successful_deliveries: number;
  first_seen: string;
  first_seen_clock: "host" | "sqs";
  last_seen: string;
  concurrent_groups: number;
  groups: Group[];
  events: TimelineEvent[];
  order: { edges: { from: string; to: string; sources: string[] }[]; edges_by_source: Record<string, number>; conflicts: unknown[] };
  notices: Notice[];
  /** PROPOSED (paid): which workspace scope this message belongs to. */
  scope?: Scope;
};

/* ───────────────────────── PROPOSED: the paid AWS-side view ───────────────────────── */

/** Proposed order source: an AWS record names the same Lambda request ID as a Trailbeam span. */
export type ProposedOrderSource = "invocation_id" | "cloudtrail_time";

export type AwsSource = "cloudtrail" | "function_logs" | "metric_nearby";

/** Blind spots a step's evidence depends on. Each has fixed copy in the UI (BLIND_SPOTS). */
export type BlindSpot =
  | "entry_id_is_message_id" // F1, F4: Lambda puts the message ID in the caller-chosen batch entry ID (undocumented)
  | "separate_roles" // F4: naming the function only works when functions use separate IAM roles
  | "receives_not_in_cloudtrail" // CloudTrail never records receives
  | "no_per_message_cause" // F2, F3: AWS gives no per-message cause; a nearby metric is not proof
  | "logs_end_not_cause" // F5: the log line says how the invocation ended, not what the handler was doing
  | "dlq_not_verified_on_real_sqs"; // observed DLQ move: not yet verified on real SQS

export type AwsEvent = {
  id: string;
  group: number;
  type: "aws_step";
  /** False when AWS recorded the step. True only for "never_handled": an absence, read next to a metric. */
  inferred: boolean;
  aws_kind: "trigger_delete" | "filter_drop" | "second_function" | "timeout" | "out_of_memory" | "never_handled";
  source: AwsSource;
  /** When AWS stamped it. CloudTrail's eventTime is whole seconds; log lines are milliseconds. */
  time: string | null;
  time_precision: "s" | "ms" | null;
  window?: { from: string | null; to: string | null };
  queue: string;
  function_name?: string;
  role?: string;
  record: Record<string, string | number>;
  summary: string;
  blind_spots: BlindSpot[];
  after: AfterEntry[];
};

export type TimelineEvent = SentEvent | ReceivedEvent | DeletedEvent | VisibilityChangedEvent | LambdaOutcomeEvent | InferredEvent | DlqMoveEvent | AwsEvent;

/** PROPOSED (paid): where a message lives in a multi-account workspace. */
export type Scope = { account: string; account_name: string; region: string; environment: string; tenant?: string; demo?: boolean };
