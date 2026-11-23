/*
  The shared timeline: one message's whole life, step by step, on the sky.

  Designed once for both products, from one API shape (./types.ts, Trailbeam's GET /api/messages/{id}).
  Self-hosted Trailbeam draws it in its shell (../../shell); the hosted app imports it from this package.
  The hosted app only adds rows open-source Trailbeam can't have (AWS-side steps) and the scope line;
  it never restyles the rest.

  Honesty rules this file enforces:
  - Every step names its source (SourceTag). Inferred steps are dashed, everywhere.
  - Steps in one "concurrent, order unknown" group sit in one box, drawn as bars on one time
    scale, in no implied sequence.
  - Every proven order between steps shows what proved it (OrderLink), in the API's words.
  - AWS-side steps carry their blind spots in plain sight, not behind a click.
  - A failed handler shows its error text on its step, and the failures are counted by cause at the foot.

  No client JS: details/summary does the expanding, so it works with scripts off.
*/
import type {
  AfterEntry, AwsEvent, BlindSpot, Group, Timeline, TimelineEvent,
} from "./types.ts";
import { ErrorCauses } from "./ErrorCauses.tsx";
import { errorCauses } from "../errors.ts";

/* ── Words ──────────────────────────────────────────────────────────────── */

/** Each order source, as a reader sees it. Proposed (paid) sources are marked. */
export const ORDER_SOURCE: Record<string, { label: string; long: string; proposed?: boolean; placement?: boolean }> = {
  clock: { label: "host clocks", long: "One step ended before the other started, on the hosts' clocks." },
  process_order: { label: "call order, one process", long: "Both calls were made by the same process, and one returned before the other was issued." },
  receive_count: { label: "receive count", long: "SQS's receive count went up between the two receives." },
  sqs_timestamps: { label: "SQS timestamps", long: "SQS's own SentTimestamp is before its ApproximateFirstReceiveTimestamp." },
  inferred_from: { label: "inferred from neighbors", long: "The step has no time. It is placed by the observed steps it was inferred from.", placement: true },
  sqs_dlq_source: { label: "SQS DLQ report", long: "SQS reported the move on the dead-letter receive. It gives the move no time, so it sits between that receive and the last one before it.", placement: true },
  invocation_id: { label: "same Lambda request ID", long: "The AWS record names the same Lambda request ID as a Trailbeam span.", proposed: true },
  cloudtrail_time: { label: "CloudTrail time, whole s", long: "CloudTrail's eventTime is in whole seconds, so it proves order only when steps are more than a second apart.", proposed: true },
};

export const BLIND_SPOTS: Record<BlindSpot, string> = {
  entry_id_is_message_id:
    "This record is tied to the message only because Lambda puts the message ID in the batch entry ID. AWS documents that field as chosen by the caller and doesn't promise it. If Lambda changes it, this step goes away.",
  separate_roles:
    "The function is named only because each function reading this queue has its own IAM role. With a shared role, CloudTrail can't say which function's trigger made the delete.",
  receives_not_in_cloudtrail:
    "CloudTrail never records receives, so it can't show when Lambda picked the message up, or how many times.",
  no_per_message_cause:
    "AWS gives no per-message cause for throttling or a recursion stop. The metric shows something happened to the function then. It doesn't show it happened to this message.",
  logs_end_not_cause:
    "The log says how the invocation ended, not what the handler was doing when it did.",
  dlq_not_verified_on_real_sqs:
    "Observed dead-letter moves are not yet verified on real SQS.",
};

const AWS_SOURCE_LABEL = { cloudtrail: "CloudTrail", function_logs: "function logs", metric_nearby: "nearby metric" } as const;

function title(e: TimelineEvent): string {
  switch (e.type) {
    case "sent": return `Sent to ${e.queue}`;
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
    case "aws_step":
      return { trigger_delete: "Deleted by Lambda's trigger", filter_drop: "Dropped by the event filter", second_function: "Taken by another function",
        timeout: "Timed out", out_of_memory: "Ran out of memory", never_handled: "Sent, never handled" }[e.aws_kind];
  }
}

/** Short label for an event when another step refers to it ("after e2 · receive 1"). */
function shortLabel(e: TimelineEvent): string {
  switch (e.type) {
    case "sent": return "the send";
    case "received": return `pickup ${e.delivery}`;
    case "deleted": return `delete${e.delivery ? ` of ${e.delivery}` : ""}`;
    case "visibility_changed": return `visibility change${e.delivery ? ` of ${e.delivery}` : ""}`;
    case "lambda_outcome": return "handler outcome";
    case "moved_to_dead_letter_queue": return "DLQ move";
    case "visibility_timeout_expired": return "timeout ended";
    case "deliveries_not_observed": return "unseen pickups";
    case "deleted_by_lambda": return "Lambda delete";
    case "lambda_invocation_ended_without_outcome": return "no outcome";
    case "aws_step": return { trigger_delete: "trigger delete", filter_drop: "filter drop", second_function: "other function",
      timeout: "timed out", out_of_memory: "out of memory", never_handled: "never handled" }[e.aws_kind];
  }
}

/** Who/where, the mono line under the time. */
function where(e: TimelineEvent): string {
  if (e.type === "aws_step") return e.aws_kind === "never_handled" ? "AWS side" : e.function_name ? `AWS · ${e.function_name}` : "AWS side";
  if (e.type === "moved_to_dead_letter_queue" || e.inferred) return "SQS";
  return e.consumer.service_name ?? "unknown service";
}

/** The ending a node shows: good (trail), retry (ember), lost (bad ending), plain. */
function tone(e: TimelineEvent): "trail" | "ember" | "lost" | "plain" {
  if (e.type === "deleted" && e.result !== "ok") return "lost";
  if (e.type === "deleted" && e.status === "final") return "trail";
  if (e.type === "aws_step" && e.aws_kind === "trigger_delete") return "trail";
  if (e.type === "lambda_outcome") return e.outcome === "success" ? "trail" : "lost";
  if (e.type === "moved_to_dead_letter_queue" || e.type === "aws_step") return "lost";
  if (e.type === "deleted" && e.status !== "final") return "ember";
  if (e.type === "received" && e.receive_count > 1 && !e.dlq_source_arn) return "ember";
  return "plain";
}

/* ── Time ───────────────────────────────────────────────────────────────── */

const ns = (iso: string) => {
  const [, frac = "0"] = iso.replace("Z", "").split(".");
  return Date.parse(iso.slice(0, 19) + "Z") * 1e6 + Number(frac.padEnd(9, "0").slice(0, 9));
};
const ms = (iso: string) => ns(iso) / 1e6;
/** "02:14:07.118", or "02:40:05" for whole-second times. */
export function clock(iso: string, precision: "s" | "ms" = "ms"): string {
  const t = iso.slice(11, 23);
  return precision === "s" ? t.slice(0, 8) : t;
}
function dur(msv: number): string {
  if (msv < 1000) return `${Math.round(msv)} ms`;
  if (msv < 60_000) return `${(msv / 1000).toFixed(1)} s`;
  return `${Math.floor(msv / 60_000)} min ${Math.round((msv % 60_000) / 1000)} s`;
}
const shortId = (id: string) => (id.length > 14 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id);

/* ── Marks ──────────────────────────────────────────────────────────────── */

export function SourceTag({ kind, children }: { kind: "recorded" | "inferred"; children: React.ReactNode }) {
  return (
    <span className={`inline-flex h-5 shrink-0 items-center rounded-[5px] border px-1.5 font-mono text-micro whitespace-nowrap ${
      kind === "inferred" ? "border-dashed border-sky-ink-3/70 text-sky-ink-3" : "border-sky-ink-3/50 text-sky-ink-2"}`}>
      {children}
    </span>
  );
}

function sourceTags(e: TimelineEvent) {
  if (e.type === "aws_step") {
    return (
      <>
        {e.inferred && <SourceTag kind="inferred">inferred</SourceTag>}
        <SourceTag kind="recorded">{AWS_SOURCE_LABEL[e.source]}</SourceTag>
        <span className="text-xs text-sky-ink-3 font-medium">AWS side</span>
      </>
    );
  }
  if (e.type === "moved_to_dead_letter_queue") return <SourceTag kind="recorded">SQS · DLQ receive</SourceTag>;
  if (e.inferred) return <SourceTag kind="inferred">inferred</SourceTag>;
  return <SourceTag kind="recorded">Trailbeam pkg</SourceTag>;
}

/** A step's node on the rail. Solid = recorded, dashed hollow = inferred. */
export function Node({ e, size = 14 }: { e: TimelineEvent; size?: number }) {
  const t = tone(e);
  const c = { trail: "var(--color-trail)", ember: "var(--color-ember)", lost: "var(--color-lost)", plain: "var(--color-sky-ink-2)" }[t];
  const r = size / 2;
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} aria-hidden className="block shrink-0">
      {e.inferred ? (
        <circle cx={r} cy={r} r={r - 1.25} fill="var(--color-sky)" stroke="var(--color-sky-ink-3)" strokeWidth="1.5" strokeDasharray="2.4 2" />
      ) : t === "lost" ? (
        <>
          <circle cx={r} cy={r} r={r - 1.25} fill="var(--color-sky)" stroke={c} strokeWidth="1.5" />
          <circle cx={r} cy={r} r={r / 3} fill={c} />
        </>
      ) : (
        <circle cx={r} cy={r} r={r - 1.5} fill={c} stroke="var(--color-sky)" strokeWidth="1.5" />
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

/** The connector between two groups: says what proves the later group comes after. */
function OrderLink({ entries, byId }: { entries: Link[]; byId: ById }) {
  const sources = [...new Set(entries.flatMap((x) => x.sources))];
  const inferred = sources.every((s) => s === "inferred_from");
  const targets = [...new Set(entries.map((x) => x.target))];
  // Into a concurrent box, each member gets its own line: sources never blur across members.
  const lines = targets.map((t) => {
    const es = entries.filter((x) => x.target === t);
    return { target: t, froms: [...new Set(es.map((x) => x.event))], sources: [...new Set(es.flatMap((x) => x.sources))] };
  });
  return (
    <li className="grid grid-cols-[1.5rem_1fr] gap-x-3 md:grid-cols-[8.5rem_1.5rem_1fr]" aria-label={`Then. Order from ${sources.map((s) => ORDER_SOURCE[s]?.label ?? s).join(", ")}`}>
      <span className="hidden md:block" />
      <span className="relative flex justify-center" aria-hidden>
        <span className={`absolute inset-y-0 w-0 border-l ${inferred ? "border-dashed border-sky-ink-3/60" : "border-sky-ink-3/60"}`} />
      </span>
      <ul className="grid gap-1 py-1.5">
        {lines.map((l) => {
          const placed = l.sources.every((s) => ORDER_SOURCE[s]?.placement);
          return (
            <li key={l.target} className="flex flex-wrap items-center gap-x-2 gap-y-0.5 font-mono text-micro text-sky-ink-3">
              <span aria-hidden>↓</span>
              {lines.length > 1 && <span className="text-sky-ink-2">{l.target}</span>}
              <span>{placed ? "placed after" : "after"} {l.froms.map((f) => `${f} ${shortLabel(byId.get(f)!)}`).join(", ")}</span>
              <span aria-hidden>·</span>
              {l.sources.map((s) => (
                <span key={s} className={`inline-flex items-center gap-1 ${s === "inferred_from" ? "text-sky-ink-3" : "text-sky-ink-2"}`}>
                  {ORDER_SOURCE[s]?.label ?? s}
                  {ORDER_SOURCE[s]?.proposed && <span className="text-sky-ink-3">(proposed)</span>}
                </span>
              )).reduce<React.ReactNode[]>((acc, el, i) => (i ? [...acc, <span key={`s${i}`} aria-hidden>+</span>, el] : [el]), [])}
            </li>
          );
        })}
      </ul>
    </li>
  );
}

/* ── One step ───────────────────────────────────────────────────────────── */

function facts(e: TimelineEvent): string[] {
  const out: string[] = [];
  if ("consumer" in e) {
    const c = e.consumer;
    if (c.faas_invocation_id) out.push(`request ${shortId(c.faas_invocation_id)}`);
    else if (c.pid !== undefined) out.push(`pid ${c.pid}${c.thread_name ? ` · ${c.thread_name}` : ""}`);
  }
  if (e.type === "received") {
    out.push(`delivery ${e.delivery}`, `receive count ${e.receive_count}`);
    if (e.dlq_source_arn) out.push(`from ${e.queue}`);
  }
  if (e.type === "deleted" || e.type === "visibility_changed") out.push(`delivery ${e.delivery ?? "?"}`, e.result === "ok" ? "SQS accepted" : `error ${e.error_code ?? ""}`);
  if (e.type === "lambda_outcome" && e.delivery) out.push(`delivery ${e.delivery}`);
  if (e.type === "moved_to_dead_letter_queue") out.push(`${e.from_queue} → ${e.to_queue}`, `reported on delivery ${e.reported_on_delivery}`);
  if (e.type === "aws_step") {
    if (e.role) out.push(e.role);
    if (typeof e.record.eventName === "string") out.push(e.record.eventName);
  }
  if ((e.type === "received" || e.type === "lambda_outcome") && "start_observed" in e && !e.start_observed) out.push("start not observed");
  return out;
}

function TimeCell({ e }: { e: TimelineEvent }) {
  if (e.time === null) {
    return <span className="font-mono text-caption text-sky-ink-3">no time</span>;
  }
  const precision = e.type === "aws_step" && e.time_precision === "s" ? "s" : "ms";
  return (
    <span className="font-mono text-caption text-sky-ink-2">
      {clock(e.time, precision)}
      {precision === "s" && <span className="text-sky-ink-3"> (s)</span>}
    </span>
  );
}

function BlindSpotNote({ spot }: { spot: BlindSpot }) {
  return (
    <li className="border-l-2 border-dawn/70 pl-3 text-[13px] leading-[1.5]">
      <span className="mr-2 text-xs text-dawn font-medium">Blind spot</span>
      <span className="text-sky-ink-2">{BLIND_SPOTS[spot]}</span>
    </li>
  );
}

function Why({ e, byId }: { e: TimelineEvent; byId: ById }) {
  const rows: { label: string; entry: AfterEntry }[] = [
    ...e.after.map((a) => ({ label: `After ${a.event} (${shortLabel(byId.get(a.event)!)})`, entry: a })),
    ...("before" in e && e.before ? [{ label: `Before ${e.before.event} (${shortLabel(byId.get(e.before.event)!)})`, entry: e.before }] : []),
  ];
  return (
    <dl className="grid gap-y-2 text-[13px] leading-[1.45]">
      {rows.map(({ label, entry }) => (
        <div key={label + entry.source}>
          <dt className="flex flex-wrap items-center gap-2 text-sky-ink">
            {label}
            {entry.sources.map((s) => (
              <SourceTag key={s} kind={s === "inferred_from" ? "inferred" : "recorded"}>{ORDER_SOURCE[s]?.label ?? s}</SourceTag>
            ))}
          </dt>
          <dd className="mt-0.5 text-sky-ink-2">{entry.why}</dd>
        </div>
      ))}
    </dl>
  );
}

function Detail({ e }: { e: TimelineEvent }) {
  const rows: [string, string][] = [];
  if ("window" in e && e.window) rows.push(["Window", `${e.window.from ? clock(e.window.from) : "?"} – ${e.window.to ? clock(e.window.to) : "now"}`]);
  if ("start" in e && e.start !== undefined) {
    rows.push(["Call", e.start ? `${clock(e.start)} → ${clock(e.end)} (${dur(ms(e.end) - ms(e.start))})` : `ended ${clock(e.end)}, start not observed`]);
  }
  if (e.type === "received") {
    if (e.sent_timestamp) rows.push(["SentTimestamp (SQS)", clock(e.sent_timestamp)]);
    if (e.first_receive_timestamp) rows.push(["First receive (SQS)", clock(e.first_receive_timestamp)]);
    if (e.dlq_source_arn) rows.push(["DeadLetterQueueSourceArn", e.dlq_source_arn]);
  }
  if (e.type === "moved_to_dead_letter_queue") rows.push(["Source ARN", e.source_arn]);
  if (e.type === "aws_step") for (const [k, v] of Object.entries(e.record)) rows.push([k, String(v)]);
  if (!rows.length) return null;
  return (
    <dl className="grid grid-cols-1 gap-x-4 gap-y-1 sm:grid-cols-[minmax(9rem,max-content)_1fr]">
      {rows.map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="font-mono text-micro text-sky-ink-3">{k}</dt>
          <dd className="font-mono text-caption break-all text-sky-ink-2">{v}</dd>
        </div>
      ))}
    </dl>
  );
}

function reasonOf(e: TimelineEvent): string | null {
  if (e.type === "aws_step") return e.summary;
  if (e.type === "lambda_outcome" && e.error_message) return e.error_message;
  if ("reason" in e) return e.reason;
  return null;
}

function Step({ e, byId, open = false, compact = false, last = false }: { e: TimelineEvent; byId: ById; open?: boolean; compact?: boolean; last?: boolean }) {
  const f = facts(e);
  const reason = reasonOf(e);
  const spots: BlindSpot[] = e.type === "aws_step" ? e.blind_spots : e.type === "moved_to_dead_letter_queue" ? ["dlq_not_verified_on_real_sqs"] : [];
  const attempt = e.type === "received" && e.receive_count > 1 && !e.dlq_source_arn;
  return (
    <li id={undefined} className={`grid grid-cols-[1.5rem_1fr] gap-x-3 ${compact ? "" : "md:grid-cols-[8.5rem_1.5rem_1fr]"}`}>
      {/* time + where: its own column from md, a line above the title on phones */}
      <div className={`hidden pt-[3px] text-right ${compact ? "" : "md:block"}`}>
        <TimeCell e={e} />
        <div className="mt-0.5 truncate font-mono text-micro text-sky-ink-3">{where(e)}</div>
      </div>
      <div className="relative flex justify-center pt-1">
        {/* the rail runs on to the next step; steps inside a concurrent box get none */}
        {!compact && !last && <span aria-hidden className="absolute top-[22px] bottom-0 w-0 border-l border-sky-ink-3/60" />}
        <Node e={e} />
      </div>
      <div className="min-w-0 pb-4">
        <div className={`mb-1 flex gap-x-3 font-mono text-micro text-sky-ink-3 ${compact ? "" : "md:hidden"}`}>
          <TimeCell e={e} /> <span className="truncate">{where(e)}</span>
        </div>
        <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1.5">
          <span className="font-mono text-micro text-sky-ink-3">{e.id}</span>
          <h4 className={`text-[15px] leading-[1.45] font-medium ${attempt ? "text-ember" : "text-sky-ink"}`}>{title(e)}</h4>
          {sourceTags(e)}
        </div>
        {f.length > 0 && <p className="mt-1 font-mono text-caption text-sky-ink-3">{f.join(" · ")}</p>}
        {reason && <p className="mt-1.5 max-w-[38rem] text-[15px] leading-[1.5] text-sky-ink-2">{reason}</p>}
        {spots.length > 0 && <ul className="mt-2.5 grid max-w-[40rem] gap-2">{spots.map((s) => <BlindSpotNote key={s} spot={s} />)}</ul>}
        <details className="group mt-2" open={open}>
          <summary className="inline-flex cursor-pointer list-none items-center gap-1.5 rounded-full text-[13px] text-sky-ink-3 hover:text-sky-ink [&::-webkit-details-marker]:hidden">
            <span aria-hidden className="inline-block transition-transform group-open:rotate-90">›</span>
            Why it's here, and the record
          </summary>
          <div className="mt-2 grid max-w-[40rem] gap-3 border-l border-sky-line py-1 pl-4">
            {e.after.length > 0 || ("before" in e && e.before) ? <Why e={e} byId={byId} /> : <p className="text-[13px] text-sky-ink-2">The first step seen for this message.</p>}
            <Detail e={e} />
          </div>
        </details>
      </div>
    </li>
  );
}

/* ── Concurrent box ─────────────────────────────────────────────────────── */

type Mark = { kind: "span" | "end-only" | "point" | "window"; from?: number; to: number; clippedLeft?: boolean; clippedRight?: boolean; windowFrom?: string };

function markOf(e: TimelineEvent): Mark | null {
  if (e.type === "aws_step") return e.time ? { kind: "point", to: ms(e.time) } : null;
  if ("window" in e && e.window && e.time === null) {
    return { kind: "window", from: e.window.from ? ms(e.window.from) : undefined, to: e.window.to ? ms(e.window.to) : Infinity, windowFrom: e.window.from ?? undefined };
  }
  if ("end" in e) return e.start ? { kind: "span", from: ms(e.start), to: ms(e.end) } : { kind: "end-only", to: ms(e.end) };
  return null;
}

function ConcurrentBox({ g, members, byId }: { g: Group; members: TimelineEvent[]; byId: ById }) {
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
        <span className="font-mono text-caption text-sky-ink-2">{g.start ? clock(g.start) : "start unknown"}</span>
        <div className="font-mono text-micro text-sky-ink-3">{members.length} steps</div>
      </div>
      <div className="relative flex justify-center" aria-hidden>
        <span className="absolute inset-y-0 w-0 border-l border-sky-ink-3/60" />
      </div>
      <section aria-label={`Concurrent, order unknown: ${members.length} steps`} className="mb-4 min-w-0 rounded-[12px] border border-sky-ink-3/45 bg-sky/60">
        <header className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 border-b border-sky-line px-4 py-3">
          <p className="text-sm text-sky-ink font-medium">Concurrent, order unknown</p>
          <p className="font-mono text-micro text-sky-ink-3">
            {g.observed_events} observed · {g.inferred_events} inferred{span > 0 ? ` · observed within ${dur(span)}` : ""}
          </p>
          <p className="w-full max-w-[38rem] text-[13px] leading-[1.45] text-sky-ink-2">
            Nothing observed proves which of these came first. They are drawn on one time scale so you can see the overlap; read them as happening together, not top to bottom.
          </p>
        </header>

        {/* One time scale for every member. */}
        <div className="px-4 pt-3 pb-2" role="img" aria-label={`Time scale from ${clock(new Date(lo).toISOString())} to ${clock(new Date(hi).toISOString())}`}>
          <ul className="grid gap-2">
            {marks.map(([m, k]) => (
              <li key={m.id} className="grid grid-cols-1 gap-x-3 gap-y-1 sm:grid-cols-[10.5rem_1fr] sm:items-center">
                <span className="flex items-center gap-1.5 truncate font-mono text-micro text-sky-ink-2">
                  <Node e={m} size={10} /> {m.id} {shortLabel(m)}
                </span>
                <span className="relative block h-4">
                  <span className="absolute inset-x-0 top-1/2 h-px -translate-y-1/2 bg-sky-line" />
                  {k?.kind === "span" && (
                    <span className="absolute top-1/2 h-1.5 min-w-[3px] -translate-y-1/2 rounded-full bg-trail" style={{ left: x(k.from!), width: `calc(${x(k.to)} - ${x(k.from!)})` }} />
                  )}
                  {k?.kind === "end-only" && (
                    <>
                      <span className="absolute top-1/2 h-1.5 -translate-y-1/2 rounded-l-full bg-gradient-to-r from-transparent to-trail/70" style={{ left: 0, width: x(k.to) }} />
                      <span className="absolute top-1/2 h-2.5 w-[3px] -translate-y-1/2 rounded-full bg-trail" style={{ left: `calc(${x(k.to)} - 1.5px)` }} />
                    </>
                  )}
                  {k?.kind === "point" && (
                    <span className={`absolute top-1/2 size-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 ${m.inferred ? "border-dashed border-sky-ink-3" : tone(m) === "trail" ? "border-trail" : "border-lost"} bg-sky`} style={{ left: x(k.to) }} />
                  )}
                  {k?.kind === "window" && (
                    <span className={`absolute top-1/2 h-2.5 -translate-y-1/2 border border-dashed ${m.inferred ? "border-sky-ink-3/80" : "border-lost/80"} ${k.from !== undefined && k.from < lo ? "rounded-r-full border-l-0" : "rounded-full"}`}
                      style={{ left: x(k.from ?? lo), width: `calc(${x(k.to)} - ${x(k.from ?? lo)})` }} />
                  )}
                </span>
                {k?.kind === "window" && k.from !== undefined && k.from < lo && (
                  <span className={`font-mono text-micro ${m.inferred ? "text-sky-ink-3" : "text-lost"} sm:col-start-2`}>← window opens {clock(k.windowFrom!)}, {dur(lo + pad - k.from)} before this scale</span>
                )}
                {k?.kind === "end-only" && (
                  <span className="font-mono text-micro text-sky-ink-3 sm:col-start-2">start not observed: only the end is known</span>
                )}
              </li>
            ))}
          </ul>
          <div className="mt-2 flex justify-between font-mono text-micro text-sky-ink-3 sm:ml-[11.25rem]">
            <span>{clock(new Date(lo + pad).toISOString())}</span>
            <span>{clock(new Date(hi - pad).toISOString())}</span>
          </div>
        </div>

        {insidePairs.length > 0 && (
          <p className="mx-4 border-t border-sky-line py-2 font-mono text-micro text-sky-ink-3">
            Ordered inside: {insidePairs.map((p) => `${p.a} before ${p.b} (${p.src.map((s) => ORDER_SOURCE[s]?.label ?? s).join(", ")})`).join("; ")}. Every other pair: unknown.
          </p>
        )}

        <ul className="border-t border-sky-line px-3 pt-4 md:px-4">
          {members.map((m) => <Step key={m.id} e={m} byId={byId} compact />)}
        </ul>
      </section>
    </li>
  );
}

/* ── The timeline ───────────────────────────────────────────────────────── */

const STATE_WORDS: Record<Timeline["state"], string> = {
  deleted: "Deleted",
  deleted_inferred: "Deleted (inferred)",
  processing: "Being handled",
  not_deleted: "Not deleted",
};

export function TimelineView({ t, answer, openSteps = [] }: { t: Timeline; answer: string; openSteps?: string[] }) {
  const byId: ById = new Map(t.events.map((e) => [e.id, e]));
  const earlier = new Set<string>();
  const items: React.ReactNode[] = [];
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
      if (links.length) items.push(<OrderLink key={`o${e.id}`} entries={links} byId={byId} />);
    }
    if (g?.concurrent) items.push(<ConcurrentBox key={`g${g.group}`} g={g} members={members} byId={byId} />);
    else for (const m of members) items.push(<Step key={m.id} e={m} byId={byId} open={openSteps.includes(m.id)} last={m.id === lastId} />);
    if (g) drawn.add(g.group);
    for (const m of members) earlier.add(m.id);
  }
  const s = t.scope;
  const stateTone = t.in_dead_letter_queue || t.state === "not_deleted" ? "text-lost" : t.state === "processing" ? "text-ember" : "text-trail";

  return (
    <article className="font-serif text-sky-ink">
      <header className="border-b border-sky-line px-4 pt-5 pb-5 md:px-8 md:pt-7">
        <p className="flex flex-wrap items-center gap-x-3 gap-y-1 font-mono text-micro text-sky-ink-3">
          <span className="font-serif text-xs font-medium">Message</span>
          <span className="break-all text-sky-ink-2">{t.message_id}</span>
        </p>
        {s && (
          <p className="mt-1.5 flex flex-wrap gap-x-2 font-mono text-micro text-sky-ink-3">
            {[s.account_name, s.region, s.environment].filter(Boolean).map((x, i) => (<span key={i} className="contents">{i > 0 && <span aria-hidden>/</span>}<span>{x}</span></span>))}
            {s.tenant && (<><span aria-hidden>/</span><span>tenant {s.tenant}</span></>)}
          </p>
        )}
        <h3 className="mt-3 max-w-[44rem] text-[1.5rem] leading-[1.2] md:text-[1.875rem] md:leading-[1.15]">{answer}</h3>
        <dl className="mt-4 grid grid-cols-2 gap-x-6 gap-y-3 sm:flex sm:flex-wrap sm:gap-x-8">
          {[
            ["State", <span key="s" className={stateTone}>{STATE_WORDS[t.state]}{t.in_dead_letter_queue ? ", in the DLQ" : ""}</span>],
            ["Queue now", t.current_queue],
            ["Deliveries", `${t.deliveries} (${t.successful_deliveries} handled)`],
            ["First seen", <span key="f">{clock(t.first_seen)} <span className="text-sky-ink-3">{t.first_seen_clock === "sqs" ? "SQS clock" : "host clock"}</span></span>],
          ].map(([k, v]) => (
            <div key={k as string}>
              <dt className="text-[13px] text-sky-ink-3">{k}</dt>
              <dd className="font-mono text-caption text-sky-ink">{v}</dd>
            </div>
          ))}
        </dl>
      </header>
      {t.notices.length > 0 && (
        <ul className="grid gap-2 border-b border-sky-line px-4 py-3 md:px-8">
          {t.notices.map((n) => <li key={n.code} className="border-l-2 border-dawn/70 pl-3 text-[13px] text-sky-ink-2">{n.message}</li>)}
        </ul>
      )}
      <ol className="px-3 pt-5 pb-2 md:px-8" aria-label="Steps, in proven order">
        {items}
      </ol>
      <ErrorCauses causes={errorCauses(t)} />
    </article>
  );
}
