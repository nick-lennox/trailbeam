/*
  The shared timeline: one message's whole life, step by step.

  Designed once for both products, from one API shape (./types.ts, Trailbeam's GET /api/messages/{id}).
  Self-hosted Trailbeam draws it in its shell (../../shell); the hosted app draws it from the same package.
  The hosted app only adds rows open-source Trailbeam can't have yet (AWS-side steps) and the scope line;
  it never restyles the rest. On the message page (../message/MessagePage.tsx) the page draws the heading,
  the state and the notices, so the view leaves them out (`inPage`).

  Restyled on the design system in row 3 of the one-UI plan: role tokens (ink, line, surface), so it takes
  the theme it is drawn in; values in the mono face, labels and words never; serif only for headings;
  hover notes are the design system's Tooltip, never `title`. Reached through @trailbeam/ui/design, never
  the main entry, which has the words (./words.ts).

  Honesty rules this file enforces:
  - Every step names its source (SourceTag). Inferred steps are dashed, everywhere.
  - Steps in one "concurrent, order unknown" group sit in one box, drawn as bars on one time
    scale, in no implied sequence.
  - Every proven order between steps shows what proved it, in the API's words.
  - AWS-side steps carry their blind spots in plain sight, not behind a click.
  - A failed handler shows its error text on its step, and the failures are counted by cause at the foot.

  No script needed to read it: details/summary does the expanding. The hover notes need one; what they say
  is never the only place it is said.
*/
import type { ReactNode } from "react";
import { ChevronRight, Info } from "lucide-react";
import type {
  AfterEntry, BlindSpot, Group, Notice, PublishedEvent, PutEventStep, Timeline, TimelineEvent,
} from "./types.ts";
import { ErrorCauses } from "./ErrorCauses.tsx";
import { errorCauses } from "../errors.ts";
import { BLIND_SPOTS, NOTICE_LINKS, ORDER_SOURCE, STATE_WORDS, clock, purgeNote, traceUrl } from "./words.ts";
import { Tooltip } from "../design/Tooltip.tsx";

/* ── Words ──────────────────────────────────────────────────────────────── */

const AWS_SOURCE_LABEL = { cloudtrail: "CloudTrail", function_logs: "Function logs", metric_nearby: "Nearby metric" } as const;
const AWS_SOURCE_NOTE = {
  cloudtrail: "Recorded by AWS CloudTrail data events, which cover code Trailbeam doesn't instrument.",
  function_logs: "Recorded by Lambda in the function's own log (a REPORT line), matched to the message by the invocation's request ID.",
  metric_nearby: "A CloudWatch metric for the function moved at about this time. It is not proof it happened to this message.",
} as const;
const RECORDED_NOTE = "Recorded by a Trailbeam package in your service.";
const INFERRED_NOTE = "Not recorded by any source. Trailbeam deduced it from the steps it saw and the queue's settings.";

function title(e: TimelineEvent): string {
  switch (e.type) {
    case "sent": return e.deduplicated ? `Sent again to ${e.queue}, deduplicated by SQS` : `Sent to ${e.queue}`;
    case "received": return e.dlq_source_arn ? "Picked up from the dead-letter queue" : `Picked up, attempt ${e.receive_count}`;
    case "deleted":
      return e.status === "final" ? "Handled and deleted" : e.status === "stale_handle" ? "Deleted, but too late" : e.status === "failed" ? "Delete failed" : "Deleted (no matching pickup)";
    case "visibility_changed":
      return e.result !== "ok" ? "Visibility change failed" : e.visibility_timeout_s === 0 ? "Released back to the queue" : `Visibility timeout set to ${e.visibility_timeout_s} s`;
    case "lambda_outcome": return e.outcome === "success" ? "Handler succeeded" : `Handler failed${e.error_type ? `: ${e.error_type}` : ""}`;
    case "visibility_timeout_expired": return "Visibility timeout ended";
    case "deliveries_not_observed": return `${e.count} pickup${e.count === 1 ? "" : "s"} not observed`;
    case "deleted_by_lambda": return "Deleted by Lambda";
    case "lambda_invocation_ended_without_outcome": return "Invocation ended with no outcome";
    case "moved_to_dead_letter_queue": return "Moved to the dead-letter queue";
    case "queue_purged": return e.removal === "possible" ? "Queue purged (may have removed it)" : "Queue purged";
    case "published": return `Published to topic ${e.topic}`;
    case "put_event": return `Put on EventBridge bus ${e.event_bus}`;
    case "arrived_via_sns": return `Arrived via SNS topic ${e.topic}`;
    case "arrived_via_eventbridge": return `Arrived via an EventBridge rule: ${e.detail_type}`;
    case "s3_event": return e.test_event ? `S3 test message for bucket ${e.bucket}` : `S3 ${e.s3_event_name} in ${e.bucket}`;
    case "aws_step":
      return { trigger_delete: "Deleted by Lambda's trigger", filter_drop: "Dropped by the event filter", second_function: "Taken by another function",
        timeout: "Timed out", out_of_memory: "Ran out of memory", never_handled: "Sent, never handled" }[e.aws_kind];
  }
}

/** Short label for an event, as the concurrent box names its members ("e2 pickup 1"). */
function shortLabel(e: TimelineEvent): string {
  switch (e.type) {
    case "sent": return e.deduplicated ? "the deduplicated send" : "the send";
    case "received": return `pickup ${e.delivery}`;
    case "deleted": return `delete${e.delivery ? ` of ${e.delivery}` : ""}`;
    case "visibility_changed": return `visibility change${e.delivery ? ` of ${e.delivery}` : ""}`;
    case "lambda_outcome": return "handler outcome";
    case "moved_to_dead_letter_queue": return "DLQ move";
    case "queue_purged": return "the purge";
    case "published": return "the publish";
    case "put_event": return "the PutEvents entry";
    case "arrived_via_sns": return "SNS delivery";
    case "arrived_via_eventbridge": return "EventBridge delivery";
    case "s3_event": return "S3 notification";
    case "visibility_timeout_expired": return "timeout ended";
    case "deliveries_not_observed": return "unseen pickups";
    case "deleted_by_lambda": return "Lambda delete";
    case "lambda_invocation_ended_without_outcome": return "no outcome";
    case "aws_step": return { trigger_delete: "trigger delete", filter_drop: "filter drop", second_function: "other function",
      timeout: "timed out", out_of_memory: "out of memory", never_handled: "never handled" }[e.aws_kind];
  }
}

/** How the record names an earlier step ("After e3 (the visibility change on delivery 1)"). */
function refLabel(e: TimelineEvent): string {
  if (e.type === "visibility_changed") return `the visibility change${e.delivery ? ` on delivery ${e.delivery}` : ""}`;
  if (e.type === "deleted") return `the delete${e.delivery ? ` on delivery ${e.delivery}` : ""}`;
  return shortLabel(e);
}

/** Who or where: the line under the time. */
function where(e: TimelineEvent): string {
  if (e.type === "aws_step") return e.aws_kind !== "never_handled" && e.function_name ? `${e.function_name} (AWS)` : "AWS side";
  if (e.type === "arrived_via_sns") return "SNS";
  if (e.type === "arrived_via_eventbridge") return `${e.eventbridge_source} (EventBridge)`;
  if (e.type === "s3_event") return "S3";
  if (e.type === "moved_to_dead_letter_queue" || e.inferred) return "SQS";
  const name = e.consumer.service_name ?? "unknown service";
  return e.consumer.service_version ? `${name} ${e.consumer.service_version}` : name;
}

/** The ending a node shows: good, retry, lost (a bad ending), plain. */
type Tone = "good" | "retry" | "lost" | "plain";
function tone(e: TimelineEvent): Tone {
  if (e.type === "deleted" && e.result !== "ok") return "lost";
  if (e.type === "deleted" && e.status === "final") return "good";
  if (e.type === "aws_step" && e.aws_kind === "trigger_delete") return "good";
  if (e.type === "lambda_outcome") return e.outcome === "success" ? "good" : "lost";
  if (e.type === "moved_to_dead_letter_queue" || e.type === "aws_step") return "lost";
  if (e.type === "queue_purged") return e.removal === "possible" ? "retry" : "lost";
  if (e.type === "deleted" && e.status !== "final") return "retry";
  if (e.type === "received" && e.receive_count > 1 && !e.dlq_source_arn) return "retry";
  return "plain";
}
const TONE_COLOR: Record<Tone, string> = {
  good: "var(--color-good)", retry: "var(--color-warning)", lost: "var(--color-critical)", plain: "var(--color-ink-2)",
};

/* ── Time ───────────────────────────────────────────────────────────────── */

const ns = (iso: string) => {
  const [, frac = "0"] = iso.replace("Z", "").split(".");
  return Date.parse(iso.slice(0, 19) + "Z") * 1e6 + Number(frac.padEnd(9, "0").slice(0, 9));
};
const ms = (iso: string) => ns(iso) / 1e6;
function dur(msv: number): string {
  if (msv < 1000) return `${Math.round(msv)} ms`;
  if (msv < 60_000) return `${(msv / 1000).toFixed(1)} s`;
  return `${Math.floor(msv / 60_000)} min ${Math.round((msv % 60_000) / 1000)} s`;
}
const shortId = (id: string) => (id.length > 14 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id);

/* ── Marks ──────────────────────────────────────────────────────────────── */

const mono = "font-mono tabular-nums";
const link = "text-brand hover:underline";

/**
 * Where a step came from: a label, solid when something recorded it, dashed when Trailbeam inferred it. A label,
 * not a value, so it is in the sans face; `note` is its hover note.
 */
export function SourceTag({ kind, note, children }: { kind: "recorded" | "inferred"; note?: ReactNode; children: ReactNode }) {
  return (
    <Tooltip content={note} tabIndex={note ? 0 : undefined}
      className={`inline-flex h-5 shrink-0 items-center rounded-md border px-1.5 text-micro whitespace-nowrap ${
        kind === "inferred" ? "border-dashed border-ink-3/70 text-ink-3" : "border-line bg-surface text-ink-2"}`}>
      {children}
    </Tooltip>
  );
}

function sourceTags(e: TimelineEvent) {
  if (e.type === "aws_step") {
    return (
      <>
        {e.inferred && <SourceTag kind="inferred" note={INFERRED_NOTE}>inferred</SourceTag>}
        <SourceTag kind="recorded" note={AWS_SOURCE_NOTE[e.source]}>{AWS_SOURCE_LABEL[e.source]}</SourceTag>
        <span className="text-xs font-medium text-ink-3">AWS side</span>
      </>
    );
  }
  if (e.type === "moved_to_dead_letter_queue") {
    return <SourceTag kind="recorded" note="SQS reported the move: the receive from the dead-letter queue names the queue it came from.">SQS DLQ receive</SourceTag>;
  }
  if (e.type === "arrived_via_sns") return <SourceTag kind="recorded" note="The SNS envelope on the message's body names the topic.">SNS envelope</SourceTag>;
  if (e.type === "arrived_via_eventbridge" || e.type === "s3_event") {
    return (
      <SourceTag kind="inferred" note="Read from the message's body. Nothing recorded the step itself.">
        inferred from the {e.type === "s3_event" ? "S3 event body" : "EventBridge event body"}
      </SourceTag>
    );
  }
  if (e.inferred) return <SourceTag kind="inferred" note={INFERRED_NOTE}>inferred</SourceTag>;
  return <SourceTag kind="recorded" note={RECORDED_NOTE}>Trailbeam package</SourceTag>;
}

/** A step's node on the rail. Solid = recorded, dashed hollow = inferred. */
export function Node({ e, size = 14 }: { e: TimelineEvent; size?: number }) {
  const t = tone(e);
  const c = TONE_COLOR[t];
  const r = size / 2;
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} aria-hidden className="block shrink-0">
      {e.inferred ? (
        <circle cx={r} cy={r} r={r - 1.25} fill="var(--color-surface)" stroke="var(--color-ink-3)" strokeWidth="1.5" strokeDasharray="2.4 2" />
      ) : t === "lost" ? (
        <>
          <circle cx={r} cy={r} r={r - 1.25} fill="var(--color-surface)" stroke={c} strokeWidth="1.5" />
          <circle cx={r} cy={r} r={r / 3} fill={c} />
        </>
      ) : (
        <circle cx={r} cy={r} r={r - 1.5} fill={c} stroke="var(--color-surface)" strokeWidth="1.5" />
      )}
    </svg>
  );
}

/* ── Order between groups ───────────────────────────────────────────────── */

type ById = Map<string, TimelineEvent>;

/** The proven links from this group back to earlier groups, with their sources. */
type Link = AfterEntry & { target: string };
function linksInto(members: TimelineEvent[], earlierIds: Set<string>): Link[] {
  const out: Link[] = [];
  for (const m of members) for (const a of m.after) if (earlierIds.has(a.event)) out.push({ ...a, target: m.id });
  return out;
}

/**
 * The connector between two groups: only the rail, dashed when the later group is placed by
 * inference. What proves the order is in each step's "Why it's here, and the record".
 */
function OrderLink({ entries }: { entries: Link[] }) {
  const inferred = entries.flatMap((x) => x.sources).every((s) => s === "inferred_from");
  return (
    <li className="grid grid-cols-[1.5rem_1fr] gap-x-3 md:grid-cols-[8.5rem_1.5rem_1fr]" aria-hidden data-connector={inferred ? "inferred" : "observed"}>
      <span className="hidden md:block" />
      <span className="relative flex h-4 justify-center">
        <span className={`absolute inset-y-0 w-0 border-l ${inferred ? "border-dashed border-ink-3/60" : "border-line-strong"}`} />
      </span>
      <span />
    </li>
  );
}

/* ── One step ───────────────────────────────────────────────────────────── */

/** One fact about a step: a label in words, and its value in the mono face; a fact that is only words has no value. */
type Fact = [label: string, value?: string];

function facts(e: TimelineEvent): Fact[] {
  const out: Fact[] = [];
  if ("consumer" in e) {
    const c = e.consumer;
    if (c.faas_invocation_id) out.push(["Request", shortId(c.faas_invocation_id)]);
    else if (c.pid !== undefined) {
      out.push(["Process", String(c.pid)]);
      if (c.thread_name) out.push(["Thread", c.thread_name]);
    }
  }
  if (e.type === "sent" && e.deduplicated) out.push(["Deduplicated, so it enqueued nothing"]);
  if (e.type === "received") {
    out.push(["Delivery", String(e.delivery)], ["Receive count", String(e.receive_count)]);
    if (e.dlq_source_arn) out.push(["From", e.queue]);
  }
  if (e.type === "deleted" || e.type === "visibility_changed") {
    out.push(["Delivery", String(e.delivery ?? "unknown")], e.result === "ok" ? ["SQS accepted it"] : ["Error", e.error_code ?? "unknown"]);
  }
  if (e.type === "lambda_outcome" && e.delivery) out.push(["Delivery", String(e.delivery)]);
  if (e.type === "queue_purged") out.push(["PurgeQueue, accepted by SQS"]);
  if (e.type === "published") {
    out.push([e.batch_entry_id ? "PublishBatch" : "Publish"], [e.joined_by.includes("envelope_message_id") ? "Joined by the envelope" : "Joined by the publish ID"]);
    if (e.sns_delivery_ms !== undefined) out.push(["SNS delivery", dur(e.sns_delivery_ms)]);
  }
  if (e.type === "put_event") {
    out.push(["PutEvents"], ...(e.entry_index !== undefined ? [["Entry", String(e.entry_index)] as Fact] : []), ["Joined by the event ID"]);
    if (e.eventbridge_delivery_ms !== undefined) {
      out.push(["EventBridge delivery", dur(e.eventbridge_delivery_ms)]);
      if (e.event_time_given) out.push(["Measured from the sender's time"]);
    }
  }
  if (e.type === "arrived_via_sns") {
    out.push(["Reported on delivery", String(e.reported_on_delivery)]);
    if (e.sns_delivery_ms !== undefined) out.push(["SNS delivery", dur(e.sns_delivery_ms)]);
  }
  if (e.type === "arrived_via_eventbridge" || e.type === "s3_event") {
    out.push(["Body of delivery", String(e.reported_on_delivery)]);
    if (e.delivery_ms !== undefined) out.push(["Delivery took", dur(e.delivery_ms)]);
  }
  if (e.type === "moved_to_dead_letter_queue") out.push(["From", e.from_queue], ["To", e.to_queue], ["Reported on delivery", String(e.reported_on_delivery)]);
  if (e.type === "aws_step") {
    if (e.role) out.push(["Role", e.role]);
    if (typeof e.record.eventName === "string") out.push(["Event", e.record.eventName]);
  }
  if ((e.type === "received" || e.type === "lambda_outcome") && "start_observed" in e && !e.start_observed) out.push(["Start not observed"]);
  return out;
}

function Facts({ items }: { items: Fact[] }) {
  if (!items.length) return null;
  return (
    <p className="mt-1 flex flex-wrap gap-x-4 gap-y-0.5 text-xs text-ink-3" data-facts="">
      {items.map(([k, v], i) => (
        <span key={i}>{k}{v !== undefined && <> <span className={`${mono} text-caption text-ink-2`}>{v}</span></>}</span>
      ))}
    </p>
  );
}

function TraceLink({ e }: { e: TimelineEvent }) {
  const url = traceUrl(e);
  if (!url) return null;
  return (
    <a href={url} target="_blank" rel="noopener noreferrer" aria-label={`Open trace for step ${e.id}`} className={`text-[13px] ${link}`}>Open trace</a>
  );
}

function TimeCell({ e }: { e: TimelineEvent }) {
  if (e.time === null) return <span className="text-xs text-ink-3">no time</span>;
  const precision = e.type === "aws_step" && e.time_precision === "s" ? "s" : "ms";
  return (
    <span className={`${mono} text-caption text-ink-2`}>
      {clock(e.time, precision)}
      {precision === "s" && (
        <Tooltip content="CloudTrail gives this time in whole seconds." tabIndex={0} className="font-sans text-xs text-ink-3"> whole seconds</Tooltip>
      )}
    </span>
  );
}

function BlindSpotNote({ spot }: { spot: BlindSpot }) {
  return (
    <li className="border-l-2 border-warning pl-3 text-[13px] leading-[1.5]">
      <span className="mr-2 text-xs font-medium text-warning-text">Blind spot</span>
      <span className="text-ink-2">{BLIND_SPOTS[spot]}</span>
    </li>
  );
}

function OrderTag({ s }: { s: string }) {
  return <SourceTag kind={s === "inferred_from" ? "inferred" : "recorded"} note={ORDER_SOURCE[s]?.long}>{ORDER_SOURCE[s]?.label ?? s}</SourceTag>;
}

function Why({ e, byId }: { e: TimelineEvent; byId: ById }) {
  const rows: { label: string; entry: AfterEntry }[] = [
    ...e.after.map((a) => ({ label: `After ${a.event} (${refLabel(byId.get(a.event)!)})`, entry: a })),
    ...("before" in e && e.before ? [{ label: `Before ${e.before.event} (${refLabel(byId.get(e.before.event)!)})`, entry: e.before }] : []),
  ];
  return (
    <dl className="grid gap-y-2 text-[13px] leading-[1.45]">
      {rows.map(({ label, entry }) => (
        <div key={label + entry.source}>
          <dt className="flex flex-wrap items-center gap-2 text-ink">
            {label}
            {entry.sources.map((s) => <OrderTag key={s} s={s} />)}
          </dt>
          <dd className="mt-0.5 text-ink-2">{entry.why}</dd>
        </div>
      ))}
    </dl>
  );
}

function Detail({ e }: { e: TimelineEvent }) {
  const rows: [string, string][] = [];
  if ("window" in e && e.window) rows.push(["Window", `${e.window.from ? clock(e.window.from) : "unknown"} to ${e.window.to ? clock(e.window.to) : "now"}`]);
  if ("start" in e && e.start !== undefined) {
    rows.push(["Call", e.start ? `${clock(e.start)} to ${clock(e.end)} (${dur(ms(e.end) - ms(e.start))})` : `ended ${clock(e.end)}, start not observed`]);
  }
  if (e.type === "received") {
    if (e.sent_timestamp) rows.push(["SentTimestamp (SQS)", clock(e.sent_timestamp)]);
    if (e.first_receive_timestamp) rows.push(["First receive (SQS)", clock(e.first_receive_timestamp)]);
    if (e.dlq_source_arn) rows.push(["DeadLetterQueueSourceArn", e.dlq_source_arn]);
  }
  if (e.type === "moved_to_dead_letter_queue") rows.push(["Source ARN", e.source_arn]);
  if (e.type === "published" || e.type === "arrived_via_sns") {
    rows.push(["Topic ARN", e.topic_arn], ["SNS message ID", e.sns_message_id]);
    if (e.type === "published") {
      if (e.publish_id) rows.push(["trailbeam.publish_id", e.publish_id]);
      if (e.batch_entry_id) rows.push(["Batch entry ID", e.batch_entry_id]);
    }
    if (e.type === "arrived_via_sns" && e.sns_timestamp) rows.push(["Timestamp (SNS)", clock(e.sns_timestamp)]);
    if (e.sns_delivery_ms !== undefined) rows.push(["SNS delivery", `${dur(e.sns_delivery_ms)}: SentTimestamp (SQS) − Timestamp (SNS), both AWS clocks`]);
  }
  if (e.type === "put_event") {
    rows.push(["Event bus", e.event_bus]);
    if (e.event_bus_given && e.event_bus_given !== e.event_bus) rows.push(["EventBusName (given)", e.event_bus_given]);
    rows.push(["Event ID", e.event_id]);
    if (e.entry_index !== undefined) rows.push(["Entry", `${e.entry_index} (index in the call's Entries)`]);
    if (e.eventbridge_source) rows.push(["source", e.eventbridge_source]);
    if (e.detail_type) rows.push(["detail-type", e.detail_type]);
    if (e.event_time) rows.push([e.event_time_given ? "Event time (sender's)" : "Event time (EventBridge)", clock(e.event_time, "s")]);
    if (e.eventbridge_delivery_ms !== undefined) {
      rows.push(["EventBridge delivery", e.event_time_given
        ? `${dur(e.eventbridge_delivery_ms)}: SentTimestamp (SQS) − event time. The sender gave the entry its Time, so this is measured from the sender's clock, not EventBridge's.`
        : `${dur(e.eventbridge_delivery_ms)}: SentTimestamp (SQS) − event time, both AWS clocks. The event time is in whole seconds, so this can be up to a second too long.`]);
    }
  }
  if (e.type === "arrived_via_eventbridge") {
    rows.push(["source", e.eventbridge_source], ["detail-type", e.detail_type], ["Event ID", e.event_id]);
    if (e.account) rows.push(["Account", e.account]);
    if (e.region) rows.push(["Region", e.region]);
  }
  if (e.type === "s3_event") {
    rows.push(["Event", e.s3_event_name], ["Bucket", e.bucket]);
    if (e.object_key) rows.push(["Key", e.object_key]);
    if (e.sequencer) rows.push(["sequencer", e.sequencer]);
    if (e.configuration_id) rows.push(["Notification configuration", e.configuration_id]);
    if (e.record_count) rows.push(["Records in the message", `${e.record_count} (this step is the first)`]);
    if (e.region) rows.push(["Region", e.region]);
  }
  if ((e.type === "arrived_via_eventbridge" || e.type === "s3_event") && e.event_time) {
    rows.push(["Event time (AWS)", clock(e.event_time)]);
    if (e.delivery_ms !== undefined) rows.push(["Delivery", `${dur(e.delivery_ms)}: SentTimestamp (SQS) − event time, both AWS clocks`]);
  }
  if (e.type === "aws_step") for (const [k, v] of Object.entries(e.record)) rows.push([k, String(v)]);
  if (!rows.length) return null;
  return (
    <dl className="grid grid-cols-1 gap-x-4 gap-y-1 sm:grid-cols-[minmax(9rem,max-content)_1fr]">
      {rows.map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="text-xs text-ink-3">{k}</dt>
          <dd className={`${mono} text-caption break-all text-ink-2`}>{v}</dd>
        </div>
      ))}
    </dl>
  );
}

/** Where another message's timeline is, for the messages a publish reached. Without one they are not links. A prop, not a
    context: a context made when the module loads breaks a server-side import of the package (React's server build can't make one),
    and the hosted app's route handlers import it (test/server-import.test.ts). */
type Href = ((id: string) => string) | undefined;

/** The SQS messages an SNS publish or a PutEvents entry reached, by queue: "Reached orders: m-1 (this message); billing: m-2". */
export function Reached({ e, self, href }: { e: PublishedEvent | PutEventStep; self: string; href?: Href }) {
  return (
    <p className="mt-1.5 max-w-[40rem] text-[13px] leading-[1.5] text-ink-2" data-reached>
      <span className="mr-1.5 text-ink">Reached</span>
      {e.reached.map((r, i) => (
        <span key={r.queue}>
          {i > 0 && "; "}
          {r.queue}:{" "}
          {r.message_ids.map((id, j) => (
            <span key={id}>
              {j > 0 && ", "}
              {id === self ? <><span className={`${mono} text-caption`}>{shortId(id)}</span> (this message)</>
                : (
                  <Tooltip content={id} className={`${mono} text-caption`}>
                    {href ? <a className={link} href={href(id)}>{shortId(id)}</a> : <span tabIndex={0}>{shortId(id)}</span>}
                  </Tooltip>
                )}
            </span>
          ))}
        </span>
      ))}
      {e.reached_truncated && <span className="text-ink-3">, and more (the first 100 receives are listed)</span>}
    </p>
  );
}

function reasonOf(e: TimelineEvent): string | null {
  if (e.type === "aws_step") return e.summary;
  if (e.type === "lambda_outcome" && e.error_message) return e.error_message;
  if ("reason" in e && e.reason) return e.reason;
  return null;
}

function Step({ e, byId, self, href, open = false, compact = false, last = false }: { e: TimelineEvent; byId: ById; self: string; href?: Href; open?: boolean; compact?: boolean; last?: boolean }) {
  const reason = reasonOf(e);
  const spots: BlindSpot[] = e.type === "aws_step" ? e.blind_spots : e.type === "moved_to_dead_letter_queue" ? ["dlq_not_verified_on_real_sqs"] : [];
  const attempt = e.type === "received" && e.receive_count > 1 && !e.dlq_source_arn;
  const failed = tone(e) === "lost";
  return (
    <li className={`grid grid-cols-[1.5rem_1fr] gap-x-3 ${compact ? "" : "md:grid-cols-[8.5rem_1.5rem_1fr]"}`}>
      {/* time and where: its own column from md, a line above the title on phones */}
      <div className={`hidden pt-[3px] text-right ${compact ? "" : "md:block"}`}>
        <TimeCell e={e} />
        <div className="mt-0.5 truncate text-xs text-ink-3">{where(e)}</div>
      </div>
      <div className="relative flex justify-center pt-1">
        {/* the rail runs on to the next step; steps inside a concurrent box get none */}
        {!compact && !last && <span aria-hidden className="absolute top-[22px] bottom-0 w-0 border-l border-line-strong" />}
        <Node e={e} />
      </div>
      <div className="min-w-0 pb-4">
        <div className={`mb-1 flex gap-x-3 text-xs text-ink-3 ${compact ? "" : "md:hidden"}`}>
          <TimeCell e={e} /> <span className="truncate">{where(e)}</span>
        </div>
        <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1.5">
          <span className={`${mono} text-micro text-ink-3`}>{e.id}</span>
          <h4 className={`text-[15px] leading-[1.45] font-medium ${attempt ? "text-warning-text" : failed ? "text-critical-text" : "text-ink"}`}>{title(e)}</h4>
          {sourceTags(e)}
          <TraceLink e={e} />
        </div>
        <Facts items={facts(e)} />
        {reason && <p className="mt-1.5 max-w-[38rem] text-sm leading-[1.5] text-ink-2">{reason}</p>}
        {(e.type === "published" || e.type === "put_event") && <Reached e={e} self={self} href={href} />}
        {spots.length > 0 && <ul className="mt-2.5 grid max-w-[40rem] gap-2">{spots.map((s) => <BlindSpotNote key={s} spot={s} />)}</ul>}
        <details className="group mt-2" open={open}>
          <summary className="inline-flex cursor-pointer list-none items-center gap-1 rounded-md text-[13px] text-ink-3 hover:text-ink [&::-webkit-details-marker]:hidden">
            <ChevronRight size={13} aria-hidden className="transition-transform group-open:rotate-90" />
            Why it&rsquo;s here, and the record
          </summary>
          <div className="mt-2 grid max-w-[40rem] gap-3 border-l border-line py-1 pl-4">
            {e.after.length > 0 || ("before" in e && e.before) ? <Why e={e} byId={byId} /> : <p className="text-[13px] text-ink-2">The first step seen for this message.</p>}
            <Detail e={e} />
          </div>
        </details>
      </div>
    </li>
  );
}

/* ── Concurrent box ─────────────────────────────────────────────────────── */

type Mark = { kind: "span" | "end-only" | "point" | "window"; from?: number; to: number; windowFrom?: string };

function markOf(e: TimelineEvent): Mark | null {
  if (e.type === "aws_step") return e.time ? { kind: "point", to: ms(e.time) } : null;
  if ("window" in e && e.window && e.time === null) {
    return { kind: "window", from: e.window.from ? ms(e.window.from) : undefined, to: e.window.to ? ms(e.window.to) : Infinity, windowFrom: e.window.from ?? undefined };
  }
  if ("end" in e) return e.start ? { kind: "span", from: ms(e.start), to: ms(e.end) } : { kind: "end-only", to: ms(e.end) };
  return null;
}

function ConcurrentBox({ g, members, byId, self, href }: { g: Group; members: TimelineEvent[]; byId: ById; self: string; href?: Href }) {
  const marks = members.map((m) => [m, markOf(m)] as const);
  // The scale covers what was observed. Inferred windows are clipped to it and say so.
  const pts = marks.flatMap(([, k]) => (!k || k.kind === "window" ? [] : [k.to, ...(k.from !== undefined ? [k.from] : [])]));
  let lo = Math.min(...pts), hi = Math.max(...pts);
  if (!(hi > lo)) { lo -= 5; hi += 5; }
  const pad = (hi - lo) * 0.08;
  lo -= pad; hi += pad;
  const x = (v: number) => `${(((Math.min(Math.max(v, lo), hi) - lo) / (hi - lo)) * 100).toFixed(2)}%`;
  const ids = new Set(members.map((m) => m.id));
  const insidePairs = members.flatMap((m) => [
    ...m.after.filter((a) => ids.has(a.event)).map((a) => ({ a: a.event, b: m.id, src: a.sources })),
    ...("before" in m && m.before && ids.has(m.before.event) ? [{ a: m.id, b: m.before.event, src: m.before.sources }] : []),
  ]).filter((p, i, arr) => arr.findIndex((q) => q.a === p.a && q.b === p.b) === i);
  const span = (pts.length ? Math.max(...pts) - Math.min(...pts) : 0);

  return (
    <li className="grid grid-cols-[1.5rem_1fr] gap-x-3 md:grid-cols-[8.5rem_1.5rem_1fr]">
      <div className="hidden pt-3 text-right md:block">
        {g.start ? <span className={`${mono} text-caption text-ink-2`}>{clock(g.start)}</span> : <span className="text-xs text-ink-3">start unknown</span>}
        <div className="text-xs text-ink-3">{members.length} steps</div>
      </div>
      <div className="relative flex justify-center" aria-hidden>
        <span className="absolute inset-y-0 w-0 border-l border-line-strong" />
      </div>
      <section aria-label={`Concurrent, order unknown: ${members.length} steps`} className="mb-4 min-w-0 rounded-card border border-line-strong bg-sunken/50">
        <header className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 border-b border-line px-4 py-3">
          <h4 className="font-serif text-[15px] font-semibold text-ink">Concurrent, order unknown</h4>
          <p className="text-xs text-ink-3">
            {g.observed_events} observed, {g.inferred_events} inferred{span > 0 ? `, observed within ${dur(span)}` : ""}
          </p>
          <p className="w-full max-w-[38rem] text-[13px] leading-[1.45] text-ink-2">
            Nothing observed proves which of these came first. They are drawn on one time scale so you can see the overlap; read them as happening together, not top to bottom.
          </p>
        </header>

        {/* One time scale for every member. */}
        <div className="px-4 pt-3 pb-2" role="img" aria-label={`Time scale from ${clock(new Date(lo).toISOString())} to ${clock(new Date(hi).toISOString())}`}>
          <ul className="grid gap-2">
            {marks.map(([m, k]) => (
              <li key={m.id} className="grid grid-cols-1 gap-x-3 gap-y-1 sm:grid-cols-[10.5rem_1fr] sm:items-center">
                <span className="flex items-center gap-1.5 truncate text-xs text-ink-2">
                  <Node e={m} size={10} /> <span className={`${mono} text-micro text-ink-3`}>{m.id}</span> {shortLabel(m)}
                </span>
                <span className="relative block h-4">
                  <span className="absolute inset-x-0 top-1/2 h-px -translate-y-1/2 bg-line" />
                  {k?.kind === "span" && (
                    <span className="absolute top-1/2 h-1.5 min-w-[3px] -translate-y-1/2 rounded-full bg-brand" style={{ left: x(k.from!), width: `calc(${x(k.to)} - ${x(k.from!)})` }} />
                  )}
                  {k?.kind === "end-only" && (
                    <>
                      <span className="absolute top-1/2 h-1.5 -translate-y-1/2 rounded-l-full bg-gradient-to-r from-transparent to-brand/70" style={{ left: 0, width: x(k.to) }} />
                      <span className="absolute top-1/2 h-2.5 w-[3px] -translate-y-1/2 rounded-full bg-brand" style={{ left: `calc(${x(k.to)} - 1.5px)` }} />
                    </>
                  )}
                  {k?.kind === "point" && (
                    <span className={`absolute top-1/2 size-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 ${m.inferred ? "border-dashed border-ink-3" : tone(m) === "good" ? "border-good" : "border-critical"} bg-surface`} style={{ left: x(k.to) }} />
                  )}
                  {k?.kind === "window" && (
                    <span className={`absolute top-1/2 h-2.5 -translate-y-1/2 border border-dashed ${m.inferred ? "border-ink-3/80" : "border-critical/80"} ${k.from !== undefined && k.from < lo ? "rounded-r-full border-l-0" : "rounded-full"}`}
                      style={{ left: x(k.from ?? lo), width: `calc(${x(k.to)} - ${x(k.from ?? lo)})` }} />
                  )}
                </span>
                {k?.kind === "window" && k.from !== undefined && k.from < lo && (
                  <span className={`text-xs ${m.inferred ? "text-ink-3" : "text-critical-text"} sm:col-start-2`}>
                    The window opens at <span className={mono}>{clock(k.windowFrom!)}</span>, {dur(lo + pad - k.from)} before this scale
                  </span>
                )}
                {k?.kind === "end-only" && (
                  <span className="text-xs text-ink-3 sm:col-start-2">Start not observed: only the end is known</span>
                )}
              </li>
            ))}
          </ul>
          <div className={`mt-2 flex justify-between ${mono} text-micro text-ink-3 sm:ml-[11.25rem]`}>
            <span>{clock(new Date(lo + pad).toISOString())}</span>
            <span>{clock(new Date(hi - pad).toISOString())}</span>
          </div>
        </div>

        {insidePairs.length > 0 && (
          <p className="mx-4 border-t border-line py-2 text-xs text-ink-3">
            Ordered inside: {insidePairs.map((p) => `${p.a} before ${p.b} (${p.src.map((s) => ORDER_SOURCE[s]?.label ?? s).join(", ")})`).join("; ")}. Every other pair: unknown.
          </p>
        )}

        <ul className="border-t border-line px-3 pt-4 md:px-4">
          {members.map((m) => <Step key={m.id} e={m} byId={byId} self={self} href={href} compact />)}
        </ul>
      </section>
    </li>
  );
}

/* ── The timeline ───────────────────────────────────────────────────────── */

/** A server notice: `backticked` spans are commands or names, drawn as code; some codes get a docs link. */
export function NoticeText({ n }: { n: Notice }) {
  const docs = NOTICE_LINKS[n.code];
  return (
    <>
      {n.message.split(/`([^`]+)`/).map((part, i) => (i % 2 ? <code key={i} className="font-mono text-caption text-ink">{part}</code> : part))}
      {docs && <> <a className={link} href={docs[1]}>{docs[0]}</a></>}
    </>
  );
}

/** The notices about a message, each with its mark: drawn by the message page above its tabs, or by the timeline on its own. */
export function Notices({ notices, className = "" }: { notices: Notice[]; className?: string }) {
  if (!notices.length) return null;
  return (
    <ul className={`grid gap-2 ${className}`} aria-label="Notices">
      {notices.map((n) => (
        <li key={n.code} className="flex gap-2 rounded-card border border-line bg-surface px-4 py-3 text-sm leading-relaxed text-ink-2 shadow-card">
          <Info size={15} className="mt-0.5 shrink-0 text-ink-3" aria-hidden />
          <span><NoticeText n={n} /></span>
        </li>
      ))}
    </ul>
  );
}

const stateTone = (t: Timeline) =>
  t.in_dead_letter_queue || t.state === "purged" ? "text-critical-text" : t.state === "not_deleted" || t.state === "processing" ? "text-warning-text" : "text-good-text";

export type TimelineViewProps = {
  t: Timeline;
  /** the one-sentence answer (answerFor); the message page draws it as its heading, so the view leaves it out there */
  answer: string;
  /** steps whose "Why it's here" starts open */
  openSteps?: string[];
  /** where another message's timeline is (the messages an SNS publish reached link to it) */
  messageHref?: (id: string) => string;
  /** drawn inside the message page, which draws the message's ID, the answer, the state and the notices itself */
  inPage?: boolean;
};

export function TimelineView({ t, answer, openSteps = [], messageHref, inPage = false }: TimelineViewProps) {
  const byId: ById = new Map(t.events.map((e) => [e.id, e]));
  const earlier = new Set<string>();
  const items: ReactNode[] = [];
  const lastId = t.events[t.events.length - 1]?.id;
  const drawn = new Set<number>();
  // In the API's order. A step placed alone between two groups (an inferred step, or the DLQ move)
  // has no group of its own, so it is drawn where it sits, between them.
  for (const e of t.events) {
    if (e.group !== null && drawn.has(e.group)) continue;
    const g = e.group === null ? null : t.groups.find((x) => x.group === e.group) ?? null;
    const members = g ? t.events.filter((x) => x.group === g.group) : [e];
    if (earlier.size) {
      const links = linksInto(members, earlier);
      if (links.length) items.push(<OrderLink key={`o${e.id}`} entries={links} />);
    }
    if (g?.concurrent) items.push(<ConcurrentBox key={`g${g.group}`} g={g} members={members} byId={byId} self={t.message_id} href={messageHref} />);
    else for (const m of members) items.push(<Step key={m.id} e={m} byId={byId} self={t.message_id} href={messageHref} open={openSteps.includes(m.id)} last={m.id === lastId} />);
    if (g) drawn.add(g.group);
    for (const m of members) earlier.add(m.id);
  }
  const s = t.scope;
  const purged = purgeNote(t);
  const figures: [string, ReactNode][] = [
    ...(inPage ? [] : [["State", <span key="s" className={stateTone(t)}>{STATE_WORDS[t.state]}{t.in_dead_letter_queue ? ", in the DLQ" : ""}{purged && <span className="text-ink-3"> {purged}</span>}</span>] as [string, ReactNode]]),
    ["Queue now", <span key="q" className={mono}>{t.current_queue}</span>],
    ["Receives", <span key="r"><span className={mono}>{t.deliveries}</span> <span className="text-ink-3">(<span className={mono}>{t.successful_deliveries}</span> handled)</span></span>],
    ["First seen", <span key="f"><span className={mono}>{clock(t.first_seen)}</span> <span className="text-ink-3">{t.first_seen_clock === "sqs" ? "SQS clock" : "host clock"}</span></span>],
  ];

  return (
    <article className="font-sans text-ink" data-timeline="">
      <header className={`border-b border-line px-4 md:px-6 ${inPage ? "py-3" : "pt-5 pb-5"}`}>
        {!inPage && (
          <>
            <p className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-ink-3">
              <span>Message</span>
              <span className={`${mono} text-caption break-all text-ink-2`}>{t.message_id}</span>
            </p>
            {s && (
              <p className="mt-1.5 flex flex-wrap gap-x-2 text-xs text-ink-3">
                {[s.account_name, s.region, s.environment].filter(Boolean).map((v, i) => (<span key={i} className="contents">{i > 0 && <span aria-hidden>/</span>}<span>{v}</span></span>))}
                {s.tenant && (<><span aria-hidden>/</span><span>tenant <span className={mono}>{s.tenant}</span></span></>)}
              </p>
            )}
            <h3 className="mt-3 max-w-[44rem] font-serif text-title-sm text-ink">{answer}</h3>
          </>
        )}
        <dl className={`grid grid-cols-2 gap-x-6 gap-y-3 sm:flex sm:flex-wrap sm:gap-x-8 ${inPage ? "" : "mt-4"}`}>
          {figures.map(([k, v]) => (
            <div key={k}>
              <dt className="text-xs text-ink-3">{k}</dt>
              <dd className="text-caption text-ink">{v}</dd>
            </div>
          ))}
        </dl>
      </header>
      {!inPage && <Notices notices={t.notices} className="border-b border-line px-4 py-3 md:px-6" />}
      <ol className="px-3 pt-5 pb-2 md:px-6" aria-label="Steps, in proven order">
        {items}
      </ol>
      <ErrorCauses causes={errorCauses(t)} />
    </article>
  );
}
