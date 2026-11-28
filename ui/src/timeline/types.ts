/*
  The shape of one message's timeline, as Trailbeam returns it from
  GET /api/messages/{message_id} (trailbeam: server/src/trailbeam/server/timeline.py, https://docs.trailbeam.app/reference/api).

  Everything below the "PROPOSED" line is not in Trailbeam yet. It is the paid app's
  AWS-side view (slice 1, screen 3), drawn here so the design can be reviewed. Those
  fields are a design proposal for the API, not a contract.
*/

/** The four facts that can prove one observed step came before another (order.py SOURCES). */
export type OrderSource = "clock" | "process_order" | "receive_count" | "sqs_timestamps";
/** How a step without a time of its own is placed: by its anchors (inferred), by SQS's DLQ report, or by the SNS
 *  envelope on a receive (contract 3.4). */
export type PlacementSource = "inferred_from" | "sqs_dlq_source" | "sns_envelope" | "amqp_x_death";

export type Consumer = {
  service_name?: string;
  /** The resource's service.version: the build that made the call (QV-362). Left out when not set. */
  service_version?: string;
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
  /** The span behind the step. ``trace_url``: its trace in the team's tracing tool, when the server has a
   *  trace link template (TRAILBEAM_TRACE_LINK); the step then shows "Open trace". */
  span?: { trace_id: string; span_id: string; trace_url?: string };
  after: AfterEntry[];
};

/** ``deduplicated``: a FIFO send SQS answered with an earlier send's MessageId; it enqueued nothing (QV-246). */
export type SentEvent = Observed & {
  type: "sent"; batch_entry_id?: string; deduplicated?: true; reason?: string;
} & Partial<AmqpPublishFields>;
export type ReceivedEvent = Observed & {
  type: "received";
  delivery: number;
  receive_count: number;
  sent_timestamp?: string;
  first_receive_timestamp?: string;
  dlq_source_arn?: string;
} & Partial<AmqpDeliveryFields>;
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

/** A recorded PurgeQueue call on the message's queue (contract section 18), shown when it removed the message
 *  (`per_sqs_documentation`) or may have (`possible`). The call is recorded; which messages it deleted is not. */
export type QueuePurgedEvent = Observed & {
  type: "queue_purged";
  source: "purge_queue";
  result: "ok";
  removal: "per_sqs_documentation" | "possible";
  reason: string;
};

/** The `purge` object of a message (list items and the timeline): the recorded purge that removed it, or may have. */
export type Purge = {
  status: "purged" | "possibly_purged";
  queue: string;
  start: string;
  end: string;
  /** the end plus 60 s: SQS says a purge takes up to 60 s */
  window_end: string;
  span: { trace_id: string; span_id: string };
};

/** Observed, but with no time: SQS reports the move on the dead-letter receive (contract 9b). */
export type DlqMoveEvent = Windowed & {
  inferred: false;
  type: "moved_to_dead_letter_queue";
  /** sqs_dlq_source: SQS's DeadLetterQueueSourceArn; amqp_x_death: RabbitMQ's x-death header (contract A8) */
  source: "sqs_dlq_source" | "amqp_x_death";
  reported_by: string;
  /** SQS only */
  source_arn?: string;
  reported_on_delivery: number;
  from_queue: string;
  to_queue: string;
  /** RabbitMQ only (x-death): why the queue dead-lettered it, how many times, and when it first did (broker clock) */
  dead_letter_reason?: "rejected" | "expired" | "maxlen" | "delivery_limit" | string;
  dead_letter_count?: number;
  first_dead_lettered?: string;
  first_dead_lettered_ms?: number;
};

/** An SNS Publish (or one message of a PublishBatch) that this message's receives join (contract 3.4, section 21).
 *  The topic's step, not the message's: the same step is on every SQS message it reached. `queue` is the topic's
 *  name and `consumer` the process that published. */
export type PublishedEvent = Observed & {
  type: "published";
  source: "sns_publish";
  topic: string;
  topic_arn: string;
  sns_message_id: string;
  publish_id?: string;
  batch_entry_id?: string;
  /** envelope_message_id: the envelope's MessageId and TopicArn (raw delivery off); publish_id: the
   *  trailbeam.publish_id attribute (raw delivery on) */
  joined_by: ("envelope_message_id" | "publish_id")[];
  /** the SQS messages whose receives join this publish, by queue (this message among them) */
  reached: { queue: string; message_ids: string[] }[];
  reached_truncated: boolean;
  /** SentTimestamp minus the envelope's Timestamp, both AWS clocks; only when joined through the envelope */
  sns_delivery_ms?: number;
  reason: string;
};

/** Observed from the envelope, with no time: a receive's SNS envelope names the topic, and no recorded publish
 *  joins it (the publisher runs no Trailbeam package). Placed before that receive. */
export type ArrivedViaSnsEvent = Omit<Windowed, "queue"> & {
  inferred: false;
  type: "arrived_via_sns";
  source: "sns_envelope";
  topic: string;
  topic_arn: string;
  sns_message_id: string;
  reported_on_delivery: number;
  /** the envelope's Timestamp, on SNS's clock; not used to place the step */
  sns_timestamp?: string;
  sns_timestamp_ms?: number;
  sns_delivery_ms?: number;
};

/** A recorded EventBridge PutEvents entry this message's receives join by the event ID in the body (contract section
 *  25.3, added in 3.6). One event can reach several queues, each as a different SQS message, so the same step is on
 *  each of them, and `reached` lists them. It replaces the inferred `arrived_via_eventbridge` step. `queue` is the
 *  event bus's name and `consumer` the process that called PutEvents. */
export type PutEventStep = Observed & {
  type: "put_event";
  source: "eventbridge_put_events";
  /** the event bus's name (`default` when the entry named none) */
  event_bus: string;
  /** the entry's EventBusName as given: a name or an ARN */
  event_bus_given?: string;
  /** the event ID EventBridge returned for the entry; the event's `id` */
  event_id: string;
  /** the entry's index in the call's Entries, from 0 */
  entry_index?: number;
  /** the entry's Source, such as `com.example.orders` */
  eventbridge_source?: string;
  detail_type?: string;
  joined_by: "event_id";
  /** the SQS messages whose receives join this entry, by queue (this message among them) */
  reached: { queue: string; message_ids: string[] }[];
  reached_truncated: boolean;
  /** the event's `time` from this message's receive (whole seconds); not used to place the step */
  event_time?: string;
  event_time_ms?: number;
  /** true when the sender gave the entry a Time, so event_time is the sender's value, not EventBridge's */
  event_time_given: boolean;
  /** the first receive's SentTimestamp minus event_time_ms: both AWS clocks unless event_time_given; up to a second
   *  too long, as `time` has whole seconds */
  eventbridge_delivery_ms?: number;
  reason: string;
};

/** Inferred from the body's shape (contract section 23.3, added in 3.5): a receive's body is an EventBridge event, so
 *  an EventBridge rule most likely sent the message. No time; placed before that receive. The event does not name the
 *  rule. */
export type ArrivedViaEventBridgeEvent = Windowed & {
  inferred: true;
  type: "arrived_via_eventbridge";
  source: "eventbridge_event";
  event_id: string;
  /** the event's `source`, such as `aws.ec2` */
  eventbridge_source: string;
  detail_type: string;
  account?: string;
  region?: string;
  reported_on_delivery: number;
  /** the event's `time` (whole seconds); not used to place the step */
  event_time?: string;
  event_time_ms?: number;
  /** SentTimestamp minus event_time_ms, both AWS clocks */
  delivery_ms?: number;
};

/** Inferred from the body's shape (contract section 23.3, added in 3.5): a receive's body is an S3 event notification
 *  or S3's test message. No time; placed before that receive. */
export type S3Event = Windowed & {
  inferred: true;
  type: "s3_event";
  source: "s3_event";
  /** without the `s3:` prefix, such as `ObjectCreated:Put`; or `s3:TestEvent` */
  s3_event_name: string;
  test_event: boolean;
  bucket: string;
  /** URL-encoded as S3 writes it, redacted; only from queues whose body is captured */
  object_key?: string;
  sequencer?: string;
  configuration_id?: string;
  record_count?: number;
  region?: string;
  reported_on_delivery: number;
  event_time?: string;
  event_time_ms?: number;
  delivery_ms?: number;
};

/* ───────────────────────── RabbitMQ (contract 4.0, span-contract-amqp.md; QV-307) ───────────────────────── */

/** What a RabbitMQ publish adds to the send step. `queue` is the exchange's name (amq.default for the default exchange). */
export type AmqpPublishFields = {
  exchange: string;
  /** the AMQP method: basic.publish */
  operation: string | null;
  result: "ok" | "failed" | "error" | string;
  /** published on a channel with publisher confirms, and confirmed */
  confirmed: boolean;
  mandatory: boolean;
  /** which publish of this message ID this is, from 1 */
  publish: number;
  published_again?: true;
  routing_key?: string;
  /** the routing key's keyed hash, when the package did not export the key (body capture off) */
  routing_key_hmac?: string;
  error_code?: string;
  message_id_source?: "package";
};

/** What a RabbitMQ delivery adds to the received step. `receive_count` is the count Trailbeam reads (A7). */
export type AmqpDeliveryFields = {
  /** basic.deliver (a consumer) or basic.get */
  operation: string | null;
  redelivered: boolean;
  /** where receive_count came from: first_delivery, x-acquired-count, x-delivery-count, redelivered */
  receive_count_source: string;
  consumer_tag: string;
  exchange: string;
  routing_key: string;
  routing_key_hmac: string;
  x_delivery_count: number;
  x_acquired_count: number;
  /** the x-death entry this delivery carries, when it came from a dead-letter queue */
  dead_letter: Record<string, unknown>;
};

/** A settle of one delivery: basic.ack (acked), basic.nack or basic.reject with requeue = true (requeued) or false (rejected). */
type AmqpSettle = Observed & {
  delivery: number | null;
  operation: string | null;
  result: "ok" | "failed" | "error" | string;
  error_code?: string;
  multiple?: boolean;
};
export type AckedEvent = AmqpSettle & { type: "acked"; status: "final" | "stale_handle" | "failed" | "unmatched_handle" };
export type RequeuedEvent = AmqpSettle & { type: "requeued"; requeue: true };
export type RejectedEvent = AmqpSettle & { type: "rejected"; requeue: false; reason: string };
/** The channel closed while the delivery was not settled: RabbitMQ returns its unacked deliveries to the queue. */
export type ChannelClosedEvent = AmqpSettle & {
  type: "channel_closed";
  initiator: "client" | "broker" | "network" | string | null;
  reply_code?: number;
  reply_text?: string;
  consumer_timeout?: true;
  reason: string;
};
/** basic.return: a mandatory publish no queue took; the broker returned it to the publisher. `queue` is the exchange. */
export type ReturnedEvent = Observed & {
  type: "returned";
  exchange: string;
  operation: string | null;
  reply_code: number | null;
  reply_text?: string;
  routing_key?: string;
  routing_key_hmac?: string;
  reason: string;
};
/** Inferred: the exchange routed the publish to these queues, counted from the queues it was delivered on (a lower bound). */
export type RoutedEvent = Omit<Windowed, "queue"> & {
  inferred: true;
  type: "routed";
  exchange: string;
  queues: string[];
  count: number;
  lower_bound: boolean;
};
/** Inferred from the next delivery: the message went back to its queue, with the cause the spans show (A10). */
export type ReturnedToQueueEvent = Windowed & {
  inferred: true;
  type: "returned_to_queue";
  cause: "requeued" | "channel_closed" | "consumer_timeout" | "consumer_cancelled" | "not_observed";
  cause_observed: boolean;
  after_delivery: number;
  before_delivery: number;
};
/** One queue's copy of a RabbitMQ message (A6): a queue is (virtual host, queue). `ended`: how the copy left the queue. */
export type QueueCopy = { queue: string; vhost: string; deliveries: number; ended: "acked" | "rejected" | "moved" | string | null };

export type AmqpEvent = AckedEvent | RequeuedEvent | RejectedEvent | ChannelClosedEvent | ReturnedEvent | RoutedEvent | ReturnedToQueueEvent;

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
  state: "deleted" | "deleted_inferred" | "processing" | "not_deleted" | "purged";
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
  /** contract 3.3: the recorded PurgeQueue call that removed the message, or may have */
  purge?: Purge;
  concurrent_groups: number;
  groups: Group[];
  events: TimelineEvent[];
  order: { edges: { from: string; to: string; sources: string[] }[]; edges_by_source: Record<string, number>; conflicts: unknown[] };
  notices: Notice[];
  /** the messaging system, on a RabbitMQ message's timeline (an SQS timeline leaves it out); see ../systems.ts */
  system?: "rabbitmq";
  /** RabbitMQ: the publishes of its message ID (more than one is a publisher retry) */
  publishes?: number;
  /** RabbitMQ: one copy per queue the message was delivered on (A6); a fanout to two queues has two */
  copies?: QueueCopy[];
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

export type TimelineEvent = SentEvent | ReceivedEvent | DeletedEvent | VisibilityChangedEvent | LambdaOutcomeEvent | QueuePurgedEvent | PublishedEvent | PutEventStep | InferredEvent | DlqMoveEvent | ArrivedViaSnsEvent | ArrivedViaEventBridgeEvent | S3Event | AwsEvent | AmqpEvent;

/** PROPOSED (paid): where a message lives in a multi-account workspace. */
export type Scope = { account: string; account_name: string; region: string; environment: string; tenant?: string; demo?: boolean };
