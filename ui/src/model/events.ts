/*
  The Event explorer's events: one row per thing that happened to a message, as the hosted app's
  explorer and dashboards list them. eventsOfTimeline makes them from one message's timeline (the
  shape GET /api/messages/{id} returns, which the hosted engine builds too), so both products list the
  same events for the same spans: fixtures/viewmodel-parity.json.

    send                one per observed SendMessage
    receive             one per observed receive, with its receive count and, on the first
                        delivery, how long the message waited in the queue
    delete              an observed DeleteMessage, or (inferred) the Lambda trigger's delete after
                        a successful invocation
    processing_error    a Lambda invocation whose outcome span says it failed, or (inferred) one
                        that never reported an outcome within the Lambda timeout
    visibility_change   an observed ChangeMessageVisibility: the consumer set the message's visibility timeout
    visibility_expired  inferred: a later receive of the same message proves the timeout ran out
    dead_letter         the move SQS reported on a receive from the dead-letter queue
    filtered            CloudTrail: Lambda's trigger deleted the message without running the function, as its
                        event filter didn't match (an aws_step on the timeline, which only the hosted app adds)

  A RabbitMQ message's events (QV-308) are of the same kinds, so the chart, the filters and the dashboards count them
  with SQS's, as dashboards_api.py does: a publish is a send and an unroutable one (basic.return) a failed send, a
  delivery a receive, an ack a delete, a requeue, a refusal or a closed channel a processing error, a return to the
  queue (inferred from the next delivery) the visibility_expired kind, and the move the x-death header reports a
  dead_letter. Each is named by its AMQP method (`call`), or by EVENT_NAME_RABBITMQ, and carries `system`.

  Steps the engine infers about unobserved receives are not events here, nor a RabbitMQ publish's routed step.
  Processing time is from the wrapper's receive to its outcome for Lambda (the outcome span is zero-length), and from
  the end of the receive to the start of its delete for other consumers (time spent between the two calls, an approximation).
  Pure: no server, no sample data.
*/

import { RABBITMQ, isRabbitMq, type MessagingSystem } from "../systems.ts";
import { RETURN_CAUSES } from "../timeline/words.ts";

export type EventKind = "send" | "receive" | "delete" | "visibility_change" | "processing_error" | "visibility_expired" | "dead_letter" | "filtered";
/** where an event is known from */
export type EventSource = "otel" | "package" | "cloudtrail" | "function_logs" | "inferred";

export type Ev = {
  key: string;
  ms: number; // milliseconds since the epoch
  event: EventKind;
  status: "ok" | "error";
  queue: string;
  messageId: string;
  service: string;
  producer: string;
  consumer: string;
  tenant?: string;
  receiveCount?: number;
  durationMs?: number;
  /* on a receive: how long the message waited in the queue before this receive */
  waitMs?: number;
  sizeBytes?: number;
  /* on a send: a FIFO send SQS deduplicated (same MessageId as an earlier send); it enqueued nothing (QV-246) */
  deduplicated?: boolean;
  detail: string;
  source: EventSource;
  hasPage: boolean;
  /* set on events read from recorded spans. Their attributes are what the spans said, never sample values. */
  live?: boolean;
  attrs?: Record<string, string | number | undefined>;
  /* the message's kept body (contract 3.2), for a reader allowed to see it, on the one event whose span carried it */
  body?: unknown;
  /* the call the event was, as its row names it (eventName): SendMessageBatch, a Lambda trigger's ReceiveMessage, an inferred delete */
  call?: string;
  /* the messaging system, on a RabbitMQ message's events (QV-308); an SQS event leaves it out */
  system?: "rabbitmq";
};

/* An event kind's name, for the Event filter and an event with no call: a kind covers several SQS calls (a receive is
   a consumer's ReceiveMessage or a Lambda trigger's poll; a delete is DeleteMessage, DeleteMessageBatch or inferred),
   so the kind is not named after one of them. */
export const EVENT_NAME: Record<EventKind, string> = {
  send: "Send",
  receive: "Receive",
  delete: "Delete",
  visibility_change: "Visibility change",
  processing_error: "Processing failed",
  visibility_expired: "Visibility timeout expired",
  dead_letter: "Moved to dead-letter queue",
  filtered: "Dropped by event filter",
};

/* A RabbitMQ event kind's name, in RabbitMQ's terms (QV-308): the kinds are SQS's, so that both count together. */
export const EVENT_NAME_RABBITMQ: Record<EventKind, string> = {
  send: "Publish",
  receive: "Delivery",
  delete: "Ack",
  visibility_change: "Visibility change",
  processing_error: "Requeued or refused",
  visibility_expired: "Returned to the queue",
  dead_letter: "Dead-lettered",
  filtered: "Dropped by event filter",
};

/* A kind's name where SQS and RabbitMQ events are listed together. */
const EVENT_NAME_BOTH: Record<EventKind, string> = {
  ...EVENT_NAME,
  send: "Send or publish",
  receive: "Receive or delivery",
  delete: "Delete or ack",
  processing_error: "Processing failed, requeued or refused",
  visibility_expired: "Visibility timeout expired or returned to the queue",
};

/** A kind's name for the events of these systems (`undefined` is SQS, as an event's `system`): SQS's, RabbitMQ's, or both. */
export function kindName(kind: EventKind, systems: ReadonlySet<MessagingSystem | undefined>): string {
  const rabbit = systems.has(RABBITMQ);
  const sqs = !rabbit || [...systems].some((x) => x !== RABBITMQ);
  return (!rabbit ? EVENT_NAME : sqs ? EVENT_NAME_BOTH : EVENT_NAME_RABBITMQ)[kind];
}

/* The calls Lambda's event source mapping makes for a function, which the function's own code never calls, and the
   delete Trailbeam infers from a successful handler, as an event row names them. */
export const CALL = {
  lambdaReceive: "ReceiveMessage (Lambda trigger)",
  lambdaDelete: "DeleteMessageBatch (Lambda trigger)",
  inferredDelete: "Delete (inferred)",
} as const;

/** An event's name in a row: the call it was, else its kind's name. */
export const eventName = (e: Pick<Ev, "event" | "call" | "system">) => e.call ?? (e.system === RABBITMQ ? EVENT_NAME_RABBITMQ : EVENT_NAME)[e.event];

export const EVENT_COLOR: Record<EventKind, string> = {
  send: "var(--color-series-2)",
  receive: "var(--color-series-1)",
  delete: "var(--color-good)",
  visibility_change: "var(--color-ink-3)",
  processing_error: "var(--color-critical)",
  visibility_expired: "var(--color-warning-text)",
  dead_letter: "var(--color-critical)",
  filtered: "var(--color-serious)",
};

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
/** The time of day in UTC: 14:42:58.622 */
export const fmtT = (ms: number) => {
  const p = (n: number, w = 2) => String(n).padStart(w, "0");
  const t = ((Math.round(ms) % 86_400_000) + 86_400_000) % 86_400_000;
  return `${p(Math.floor(t / 3600000))}:${p(Math.floor(t / 60000) % 60)}:${p(Math.floor(t / 1000) % 60)}.${p(t % 1000, 3)}`;
};
/** The event list's date, as a log explorer writes it, in UTC: Nov 14 14:42:58.622 */
export const fmtDate = (ms: number) => { const d = new Date(ms); return `${MONTHS[d.getUTCMonth()]} ${d.getUTCDate()} ${fmtT(ms)}`; };

/* how a failed outcome is worded, as the open-source timeline words it (timeline/ErrorCauses.tsx): the handler
   threw; it returned normally and listed the message in batchItemFailures; or it returned a batchItemFailures Lambda rejected */
export const OUTCOME_WORDS: Record<string, string> = {
  failed_exception: "The handler threw",
  failed_reported: "Reported in batchItemFailures",
  failed_invalid_response: "Invalid batchItemFailures response",
};

/** A failed handler's one line: the exception class, and its message when the queue has error text on (contract 15.5). */
export const handlerFailed = (type?: string | null, message?: string | null, outcome?: string | null) => {
  const lead = (outcome && OUTCOME_WORDS[outcome]) || "The handler failed";
  return type ? `${lead}: ${type}${message ? `: ${message}` : ""}` : message ? `${lead}: ${message}` : lead;
};

/* ── where a queue lives ──────────────────────────────────────────────── */

export type QueuePlace = { name: string; account: string | null; region: string | null };

const REGION = /(?:^|\.)((?:[a-z]{2}(?:-gov|-iso[a-z]?)?)-[a-z]+-\d+)(?=\.|$)/;

/**
  Where a queue lives, read from the aws.sqs.queue.url every Trailbeam span carries:
  https://sqs.<region>.amazonaws.com/<account>/<name>, and the older, VPC endpoint, China and local-emulator forms of it.
*/
export function parseQueueUrl(url: string | null | undefined): QueuePlace | null {
  if (!url) return null;
  let u: URL;
  try { u = new URL(url); } catch { return null; }
  const parts = u.pathname.split("/").filter(Boolean);
  const name = parts[parts.length - 1];
  if (!name) return null;
  const before = parts[parts.length - 2];
  const account = before && /^\d{12}$/.test(before) ? before : null;
  const host = u.hostname.toLowerCase();
  const region = REGION.exec(host)?.[1] ?? (host === "queue.amazonaws.com" ? "us-east-1" : null);
  return { name: decodeURIComponent(name), account, region };
}

/* ── one message's events, from its timeline ──────────────────────────── */

type Doc = Record<string, unknown>;

/** What one span said beyond the timeline (contract 3.2), as both backends store it: the fields eventsOfTimeline reads. */
export type SpanMeta = {
  kind: string; traceId: string; spanId: string;
  tenant: string | null; tenantSource: string | null; bodySize: number | null; groupId: string | null; sequenceNumber: string | null;
  errorMessage: string | null; batchSize: number | null; batchIndex: number | null;
};

/** What a message's spans say about it, beyond its timeline: its tenant (and where it was read from), FIFO group and sequence number, and body size. The send's value wins. */
export function metaSummary(metas: readonly Pick<SpanMeta, "kind" | "tenant" | "tenantSource" | "groupId" | "sequenceNumber" | "bodySize">[]): {
  tenant: string | null; tenantSource: string | null; groupId: string | null; sequenceNumber: string | null; bodySize: number | null } {
  const ordered = [...metas.filter((m) => m.kind === "send"), ...metas.filter((m) => m.kind !== "send")];
  const first = <K extends "groupId" | "sequenceNumber" | "bodySize">(k: K) => ordered.find((m) => m[k] !== null)?.[k] ?? null;
  const tenantOf = ordered.find((m) => m.tenant !== null);
  return { tenant: tenantOf?.tenant ?? null, tenantSource: tenantOf?.tenantSource ?? null, groupId: first("groupId") as string | null,
    sequenceNumber: first("sequenceNumber") as string | null, bodySize: first("bodySize") as number | null };
}

/** A send that only CloudTrail saw (the hosted app's AWS records): the package didn't see it, so it names the producer. */
export type TrailSendEvent = { queue: string; timeMs: number; call?: string; producer: string; byService: boolean };

export type TimelineEventsOptions = {
  /** each queue's URL, by queue name, for the event's account and region */
  urls?: ReadonlyMap<string, string> | Record<string, string>;
  /** where each queue lives, by queue name, when it is known without a URL (a flow's account and region); before urls */
  places?: Record<string, { account: string | null; region: string | null }>;
  /** the message's spans' 3.2 attributes */
  metas?: readonly SpanMeta[];
  /** the kept body, for a reader allowed to see it, and the span that carried it */
  body?: { value: unknown; traceId: string; spanId: string };
  /** a send CloudTrail recorded that the package didn't see */
  trailSend?: TrailSendEvent;
};

const NS_PER_MS = BigInt(1_000_000);
const msOf = (ns: unknown) => Number(BigInt(String(ns)) / NS_PER_MS);
const isoMs = (iso: unknown) => (typeof iso === "string" ? Date.parse(iso) : NaN);

/* how long a Lambda handler ran: from the wrapper's receive (when it got the event, before the handler ran) to its outcome
   (contract section 2). The outcome span is zero-length, so its own start and end give 0; its length is used only when no
   receive was stored for the invocation and the span has one. */
const handlerMs = (r: Doc | undefined, start: number | null, end: number) =>
  r ? Math.max(0, end - msOf(r.end_unix_nano)) : start !== null && end > start ? end - start : undefined;

/**
  The events of one message, from its timeline (GET /api/messages/{id}'s `events`, or the hosted engine's build()).
  The part of the hosted app's eventsOf (lib/app/live-events.ts) that runs after the timeline is built.
*/
export function eventsOfTimeline(t: Doc, o: TimelineEventsOptions = {}): Ev[] {
  const mid = String(t.message_id);
  const evs = (t.events ?? []) as Doc[];
  const urls = o.urls instanceof Map ? o.urls : new Map(Object.entries((o.urls ?? {}) as Record<string, string>));
  const metas = o.metas ?? [];

  const svc = (e: Doc) => ((e.consumer as Doc | undefined)?.service_name as string | undefined) ?? "";
  const sends = evs.filter((e) => e.type === "sent");
  const receives = evs.filter((e) => e.type === "received");
  const outcomes = evs.filter((e) => e.type === "lambda_outcome");
  const ctSend = sends.length ? undefined : o.trailSend;
  const producer = sends.length ? svc(sends[0]) : ctSend ? ctSend.producer : "";
  const consumerOf = (delivery: unknown) => {
    const r = receives.find((x) => x.delivery === delivery);
    return r ? svc(r) : receives.length ? svc(receives[receives.length - 1]) : "";
  };
  const receiveOf = (delivery: unknown) => receives.find((x) => x.delivery === delivery);
  const firstSend = sends.length ? msOf(sends[0].end_unix_nano) : null;

  const about = metaSummary(metas);
  const spanMeta = new Map(metas.map((m) => [`${m.traceId}:${m.spanId}`, m]));
  const spanOf = (e: Doc) => { const sp = e.span as { trace_id?: string; span_id?: string } | undefined; return sp?.trace_id ? `${sp.trace_id}:${sp.span_id}` : ""; };

  const amqp = isRabbitMq(t as { message_id?: string; system?: string });
  const out: Ev[] = [];
  const evSpan = new Map<Ev, string>();
  const push = (e: Doc, ev: EventKind, ms: number, fields: Partial<Ev> & { service: string; consumer: string }) => {
    if (!Number.isFinite(ms)) return;
    const queue = String(e.queue ?? t.current_queue);
    const place = o.places?.[queue] ?? parseQueueUrl(urls.get(queue));
    const error = ev === "processing_error" || ev === "dead_letter" || fields.status === "error";
    const own = spanMeta.get(spanOf(e));
    const made: Ev = {
      key: `${mid}:${e.id}`, ms, event: ev, status: error ? "error" : "ok", queue, messageId: mid, producer, detail: "",
      source: "package", hasPage: true, live: true,
      ...(amqp ? { system: RABBITMQ as "rabbitmq" } : {}),
      ...(about.tenant ? { tenant: about.tenant } : {}),
      ...(ev === "send" && own?.bodySize !== null && own?.bodySize !== undefined ? { sizeBytes: own.bodySize } : {}),
      attrs: {
        // SQS returns it on a receive, for that receive: on no other event (a RabbitMQ delivery has receiveCount only)
        "message.ApproximateReceiveCount": !amqp && ev === "receive" && Number(e.receive_count) > 0 ? Number(e.receive_count) : undefined,
        "message.tenant": about.tenant ?? undefined,
        "message.tenantSource": about.tenant ? about.tenantSource ?? undefined : undefined,
        "message.MessageGroupId": about.groupId ?? undefined,
        "message.SequenceNumber": about.sequenceNumber ?? undefined,
        "message.BodySize": about.bodySize ?? undefined,
        // SQS's own timestamps, as a receive returned them
        "message.SentTimestamp": typeof e.sent_timestamp_ms === "number" ? e.sent_timestamp_ms : undefined,
        "message.ApproximateFirstReceiveTimestamp": typeof e.first_receive_timestamp_ms === "number" ? e.first_receive_timestamp_ms : undefined,
        // on a Lambda receive: the batch the record came in, as the receive span carried it
        "batch.size": ev === "receive" ? own?.batchSize ?? undefined : undefined,
        "batch.index": ev === "receive" ? own?.batchIndex ?? undefined : undefined,
        "queue.account": place?.account ?? undefined,
        "queue.region": place?.region ?? undefined,
      },
      ...fields,
    };
    out.push(made);
    evSpan.set(made, spanOf(e));
  };

  if (ctSend) {
    const e: Doc = { id: "ct-send", queue: ctSend.queue };
    push(e, "send", ctSend.timeMs, { service: producer, consumer: consumerOf(1), source: "cloudtrail", call: ctSend.call,
      detail: `Recorded by CloudTrail (whole seconds). Sent by ${ctSend.byService ? "the AWS service " : ""}${producer}` });
  }
  /* a RabbitMQ step's event (QV-308), named by its AMQP method. A count the step has none of is left out, as the port does. */
  const count = (v: unknown) => (v === null || v === undefined ? undefined : Number(v));
  const amqpEvent = (e: Doc, end: number, start: number | null) => {
    const op = typeof e.operation === "string" && e.operation ? e.operation : undefined;
    const failed = e.result !== undefined && e.result !== null && e.result !== "ok";
    const code = e.error_code ? `: ${e.error_code}` : "";
    const settle = () => {
      const r = receiveOf(e.delivery);
      return { service: svc(e), consumer: svc(e), receiveCount: r ? count(r.receive_count) : undefined,
        durationMs: r && start !== null ? Math.max(0, start - msOf(r.end_unix_nano)) : undefined };
    };
    switch (e.type) {
      case "sent":
        push(e, "send", end, { service: svc(e), consumer: consumerOf(1), call: op ?? "basic.publish", status: failed ? "error" : "ok",
          detail: failed ? `${op ?? "basic.publish"} failed${code}` : e.published_again ? String(e.reason ?? "") : "" });
        break;
      case "returned":
        push(e, "send", end, { service: svc(e), consumer: "", call: op ?? "basic.return", status: "error",
          detail: `Unroutable: no queue took it, so the broker returned it to the publisher${e.reply_code !== undefined && e.reply_code !== null ? ` (${e.reply_code}${e.reply_text ? ` ${e.reply_text}` : ""})` : ""}` });
        break;
      case "received": {
        const sentAt = firstSend;
        const wait = e.delivery === 1 && Number(e.receive_count) === 1 && sentAt !== null ? Math.max(0, end - sentAt) : undefined;
        push(e, "receive", end, { service: svc(e), consumer: svc(e), receiveCount: count(e.receive_count), waitMs: wait, call: op ?? "basic.deliver",
          detail: e.dead_letter ? "Delivered from the dead-letter queue" : "" });
        break;
      }
      case "acked":
        push(e, "delete", end, { ...settle(), call: op ?? "basic.ack", status: failed ? "error" : "ok",
          detail: failed ? `${op ?? "basic.ack"} failed${code}` : e.status === "stale_handle" ? "Acked a delivery that was not its copy's latest"
            : e.status === "unmatched_handle" ? "No observed delivery matches this ack" : "" });
        break;
      case "requeued":
        push(e, "processing_error", end, { ...settle(), call: op ?? "basic.nack", detail: failed ? `${op ?? "basic.nack"} failed${code}` : "Requeued by the consumer (requeue = true)" });
        break;
      case "rejected":
        push(e, "processing_error", end, { ...settle(), call: op ?? "basic.reject",
          detail: failed ? `${op ?? "basic.reject"} failed${code}` : "Refused by the consumer (requeue = false): dead-lettered if the queue has a dead-letter exchange, dropped otherwise" });
        break;
      case "channel_closed":
        push(e, "processing_error", end, { ...settle(), call: op ?? "channel.close", detail: String(e.reason ?? "") });
        break;
      case "returned_to_queue": {
        const before = receiveOf(e.after_delivery);
        push(e, "visibility_expired", isoMs((e.window as Doc).to), { service: "rabbitmq", consumer: before ? svc(before) : "",
          receiveCount: before ? count(before.receive_count) : undefined, source: "inferred",
          detail: `Delivery ${e.after_delivery} went back to the queue: ${RETURN_CAUSES[String(e.cause)] ?? String(e.cause)}. Inferred from the next delivery` });
        break;
      }
      case "moved_to_dead_letter_queue": {
        const dlqReceive = receiveOf(e.reported_on_delivery);
        push(e, "dead_letter", isoMs((e.window as Doc).to), { queue: String(e.from_queue), service: "rabbitmq", consumer: consumerOf(Number(e.reported_on_delivery) - 1),
          receiveCount: dlqReceive ? count(dlqReceive.receive_count) : undefined,
          detail: `Dead-lettered from ${e.from_queue} to ${e.to_queue}${e.dead_letter_reason ? ` (${e.dead_letter_reason})` : ""}. RabbitMQ reported it in the x-death header on the delivery from ${e.to_queue}` });
        break;
      }
    }
  };
  for (const e of evs) {
    const end = e.end_unix_nano ? msOf(e.end_unix_nano) : NaN;
    const start = e.start_unix_nano ? msOf(e.start_unix_nano) : null;
    if (amqp) { amqpEvent(e, end, start); continue; }
    switch (e.type) {
      case "sent":
        push(e, "send", end, { service: svc(e), consumer: consumerOf(1), call: e.batch_entry_id ? "SendMessageBatch" : "SendMessage",
          ...(e.deduplicated ? { deduplicated: true, detail: String(e.reason ?? "") } : {}) });
        break;
      case "received": {
        // time in queue, for the first delivery: from the send (the producer's clock) or, without
        // an observed send, from SQS's SentTimestamp
        const sentAt = firstSend ?? (typeof e.sent_timestamp_ms === "number" ? e.sent_timestamp_ms : null);
        const wait = e.delivery === 1 && Number(e.receive_count) === 1 && sentAt !== null ? Math.max(0, end - sentAt) : undefined;
        const lambda = (e.consumer as Doc | undefined)?.source === "lambda";
        push(e, "receive", end, { service: svc(e), consumer: svc(e), receiveCount: Number(e.receive_count), waitMs: wait, call: lambda ? CALL.lambdaReceive : "ReceiveMessage",
          detail: e.dlq_source_arn ? `Received from the dead-letter queue` : "" });
        break;
      }
      case "deleted": {
        const r = receiveOf(e.delivery);
        const took = r && start !== null ? Math.max(0, start - msOf(r.end_unix_nano)) : undefined;
        const failed = e.result !== "ok";
        const call = e.batch_entry_id ? "DeleteMessageBatch" : "DeleteMessage";
        const detail = failed ? `${call} failed${e.error_code ? `: ${e.error_code}` : ""}`
          : e.status === "stale_handle" ? "Deleted with the receipt handle of an earlier receive" : e.status === "unmatched_handle" ? "No observed receive returned this receipt handle" : "";
        push(e, "delete", end, { service: svc(e), consumer: svc(e), receiveCount: r ? Number(r.receive_count) : undefined, durationMs: took, detail, status: failed ? "error" : "ok", call });
        break;
      }
      case "visibility_changed": {
        const r = receiveOf(e.delivery);
        const failed = e.result !== "ok";
        const call = e.batch_entry_id ? "ChangeMessageVisibilityBatch" : "ChangeMessageVisibility";
        const secs = e.visibility_timeout_s;
        const detail = failed ? `${call} failed${e.error_code ? `: ${e.error_code}` : ""}` : typeof secs === "number" ? `Visibility timeout set to ${secs} s` : "";
        push(e, "visibility_change", end, { service: svc(e), consumer: svc(e), receiveCount: r ? Number(r.receive_count) : undefined, detail, status: failed ? "error" : "ok", call });
        break;
      }
      case "lambda_outcome": {
        if (e.outcome === "success") break; // its delete is the inferred deleted_by_lambda step
        const r = receiveOf(e.delivery);
        const took = handlerMs(r, start, end);
        push(e, "processing_error", end, { service: svc(e), consumer: svc(e), receiveCount: r ? Number(r.receive_count) : undefined, durationMs: took,
          detail: handlerFailed(e.error_type as string | undefined, spanMeta.get(spanOf(e))?.errorMessage ?? (e.error_message as string | undefined), e.outcome as string | undefined) });
        break;
      }
      case "deleted_by_lambda": {
        const at = isoMs((e.window as Doc).from);
        const o = outcomes.find((x) => x.outcome === "success" && x.delivery === e.delivery && msOf(x.end_unix_nano) === at) ?? outcomes.find((x) => x.outcome === "success" && x.delivery === e.delivery);
        const r = receiveOf(e.delivery);
        const took = o ? handlerMs(r, o.start_unix_nano ? msOf(o.start_unix_nano) : null, msOf(o.end_unix_nano)) : undefined;
        push(e, "delete", at, { service: r ? svc(r) : "", consumer: r ? svc(r) : "", receiveCount: r ? Number(r.receive_count) : undefined, durationMs: took,
          detail: "The handler succeeded, so the Lambda trigger deleted the message. That delete is inferred", source: "inferred", call: CALL.inferredDelete });
        break;
      }
      case "lambda_invocation_ended_without_outcome": {
        const r = receiveOf(e.delivery);
        push(e, "processing_error", isoMs((e.window as Doc).to), { service: r ? svc(r) : "", consumer: r ? svc(r) : "", receiveCount: r ? Number(r.receive_count) : undefined,
          detail: "No outcome within the Lambda timeout: the invocation most likely timed out or ran out of memory", source: "inferred" });
        break;
      }
      case "visibility_timeout_expired": {
        const before = receiveOf(e.after_delivery);
        push(e, "visibility_expired", isoMs((e.window as Doc).to), { service: "sqs", consumer: before ? svc(before) : "", receiveCount: before ? Number(before.receive_count) : undefined,
          detail: `Receive ${e.after_delivery} was not deleted in time, so the message became visible again. Inferred from the next receive`, source: "inferred" });
        break;
      }
      case "aws_step": {
        const at = isoMs(e.time);
        const fn = typeof e.function_name === "string" ? e.function_name : "";
        const r = e.delivery ? receiveOf(e.delivery) : undefined;
        const common = { service: fn || "lambda", consumer: fn, detail: String(e.summary ?? ""), receiveCount: r ? Number(r.receive_count) : undefined };
        if (e.aws_kind === "filter_drop") push(e, "filtered", at, { ...common, source: "cloudtrail", status: "error" });
        else if (e.aws_kind === "trigger_delete" || e.aws_kind === "second_function") push(e, "delete", at, { ...common, source: "cloudtrail", call: CALL.lambdaDelete });
        else if (e.aws_kind === "timeout" || e.aws_kind === "out_of_memory") {
          const took = typeof (e.record as Doc)?.Duration === "string" ? Number.parseFloat(String((e.record as Doc).Duration)) : undefined;
          push(e, "processing_error", at, { ...common, source: "function_logs", durationMs: took });
        }
        break;
      }
      case "moved_to_dead_letter_queue": {
        const dlqReceive = receiveOf(e.reported_on_delivery);
        push(e, "dead_letter", isoMs((e.window as Doc).to), { queue: String(e.from_queue), service: "sqs", consumer: consumerOf(Number(e.reported_on_delivery) - 1),
          receiveCount: dlqReceive ? Number(dlqReceive.receive_count) : undefined, detail: `Moved from ${e.from_queue} to ${e.to_queue}. SQS reported it on the receive from ${e.to_queue}` });
        break;
      }
    }
  }
  // the body goes on the event whose span carried the kept copy, else the message's first event
  if (o.body && out.length) {
    const at = `${o.body.traceId}:${o.body.spanId}`;
    (out.find((x) => evSpan.get(x) === at) ?? out[0]).body = o.body.value;
  }
  return out;
}
