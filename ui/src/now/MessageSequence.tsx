"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import { fmtBytes, fmtMs, PLATFORM_LABEL, type Platform } from "../model/format.ts";
import { fmtT, type Ev } from "../model/events.ts";
import { SOURCE } from "../design/pills.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { UiLink } from "../design/nav.tsx";
import { ServiceIcon } from "../design/icons.tsx";

/* One message as a sequence diagram, shown inside an opened event row and under a message in "In ‹queue› now".
   Participants are columns. An arrow is an API call or a move. A bar on a
   lifeline is time spent in that state. Beside it: what the selected step
   did, its facts, and the message body. Styles are the .qs-* rules in
   tokens.css.

   The diagram is worked out from the message's recorded events (sequenceOf): every value is one its spans carried or
   one the queue's settings gave (QueueFacts), and anything else says "Not recorded" or "Not read". A host with events of
   its own kind (the hosted app's sample) builds the Sequence itself and passes it in, with the parts exported here. */

const ROW = 40; // height of a step and of its highlight
const GUT = 164; // room for the time and how long after the first event
const COL = 160;
const TOP = 26; // from the top of the body to the first step
const GAP_MIN = 44; // the shortest gap: rows are 40 tall
const GAP_MAX = 132; // three times that; anything longer is cut

export type SequencePart = "producer" | "queue" | "consumer" | "dlq";
export type Fact = [string, ReactNode];
export type SequenceStep = {
  ev: Ev;
  name: string;
  draw: { kind: "arrow"; a: number; b: number; call: string; inferred?: boolean } | { kind: "mark"; at: number; tone: "x" | "warn" | "tick"; text: string; inferred?: boolean };
  say: string;
  facts: Fact[];
};
export type SequenceBar = { kind: "visible" | "hidden" | "run"; col: number; from: number; to: number; word: string; num: string; open?: boolean };
export type Participant = { part: SequencePart; role: string; name: string; icon: ReactNode; platform: string };
/** a message drawn as a sequence: its participants, steps and bars, and the facts in the tabs beside it */
export type Sequence = { evs: Ev[]; parts: Participant[]; steps: SequenceStep[]; bars: SequenceBar[]; messageAttrs: Fact[]; queueAttrs: Fact[]; mappingAttrs: Fact[] };
type Step = SequenceStep;
type Bar = SequenceBar;
type Part = SequencePart;

const tint = (color: string, icon: ReactNode) => <span style={{ color }} className="grid place-items-center">{icon}</span>;
/* Each mark in the color AWS gives its service family, so the kind of service reads before the label:
   orange for compute and containers, pink for application integration (SQS), green for storage. */
const AWS = { compute: "#ED7100", integration: "#E7157B", storage: "#7AA116" };
/* The AWS service mark for what a participant runs on, in its service family's color. */
export const PLATFORM_ICON: Record<Platform, ReactNode> = {
  Lambda: tint(AWS.compute, <ServiceIcon name="lambda" size={22} />),
  EKS: tint(AWS.compute, <ServiceIcon name="eks" size={22} />),
  ECS: tint(AWS.compute, <ServiceIcon name="ecs" size={22} />),
  S3: tint(AWS.storage, <ServiceIcon name="s3" size={22} />),
  EC2: tint(AWS.compute, <ServiceIcon name="ec2" size={22} />),
  Celery: <svg viewBox="0 0 20 20" width={22} height={22} fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><rect x="3" y="3.5" width="14" height="5" rx="1.5" /><rect x="3" y="11.5" width="14" height="5" rx="1.5" /><path d="M6 6h.01M6 14h.01" /></svg>,
};
export const SQS_ICON = tint(AWS.integration, <ServiceIcon name="sqs" size={22} />);

/** a word among the values, set in the sans */
export const S = ({ children }: { children: ReactNode }) => <span className="font-sans text-[13px] text-ink-2">{children}</span>;
export const ORD = ["for the first time", "a second time", "a third time", "a fourth time", "a fifth time"];
export const hash = (s: string) => [...s].reduce((h, c) => (Math.imul(h, 31) + c.charCodeAt(0)) >>> 0, 7);
export const recorded = (e: Ev) => <S>{e.source === "inferred" ? "No source. Inferred from the queue settings" : e.source === "package" ? "Trailbeam package" : e.source === "otel" ? "OpenTelemetry" : SOURCE[e.source].label}</S>;

/* The service or resource a step belongs to, shown as its own row. */
const who = (e: Ev): [string, ReactNode] =>
  e.event === "send" ? ["Producer", e.producer] : e.event === "visibility_expired" || e.event === "dead_letter" ? ["Queue", e.queue] : ["Consumer", e.consumer];

/* Bars: the message's state on the queue, and processing on the consumer. */
export function barsOf(steps: Step[]): Bar[] {
  const bars: Bar[] = [];
  const idx = (pred: (s: Step) => boolean, from: number) => steps.findIndex((s, i) => i >= from && pred(s));
  const dur = (a: number, b: number) => fmtMs(steps[b].ev.ms - steps[a].ev.ms);
  const last = steps.length - 1;
  steps.forEach((s, i) => {
    const k = s.ev.event;
    if (k === "send" || k === "visibility_expired") {
      const to = idx((x) => ["receive", "filtered", "dead_letter"].includes(x.ev.event), i + 1);
      if (to > 0 && steps[to].ev.event !== "dead_letter") bars.push({ kind: "visible", col: 1, from: i, to, word: "visible", num: dur(i, to) });
      /* nothing after it yet: the message is still in the queue, waiting to be received */
      else if (to < 0 && i === last) bars.push({ kind: "visible", col: 1, from: i, to: last + 0.6, word: "visible", num: "", open: true });
    }
    if (k === "receive") {
      const end = idx((x) => ["processing_error", "delete"].includes(x.ev.event), i + 1);
      const nextReceive = idx((x) => x.ev.event === "receive", i + 1);
      const done = end > 0 && (nextReceive < 0 || end < nextReceive) ? end : -1;
      if (done > 0) bars.push({ kind: "run", col: 2, from: i, to: done, word: "processing", num: dur(i, done) });
      else bars.push({ kind: "run", col: 2, from: i, to: last + 0.6, word: "processing", num: "", open: true });
      const back = idx((x) => ["visibility_expired", "receive", "dead_letter", "delete"].includes(x.ev.event), i + 1);
      if (back > 0) bars.push({ kind: "hidden", col: 1, from: i, to: back, word: "in flight", num: dur(i, back) });
      else bars.push({ kind: "hidden", col: 1, from: i, to: last + 0.6, word: "in flight", num: "", open: true });
    }
  });
  return bars;
}

/* What the connected account says about the queue, for a workspace's own data: GetQueueAttributes (read: whether it was
   read) and the queue's Lambda triggers (ListEventSourceMappings; null when not read). */
export type SequenceTrigger = { functionName: string; batchSize: number | null; batchingWindowS: number | null; reportBatchItemFailures: boolean };
export type QueueFacts = {
  read: boolean; fifo: boolean | null; visibilityTimeoutS: number | null; retentionS: number | null; delayS: number | null;
  maxReceiveCount: number | null; dlq: string | null; triggers: SequenceTrigger[] | null;
};

export const NOT_RECORDED = "Not recorded";
export const NOT_READ = "Not read";
const SERVICE_ICON = <svg viewBox="0 0 20 20" width={22} height={22} fill="none" stroke="currentColor" strokeWidth="1.5"><rect x="3.5" y="3.5" width="13" height="13" rx="2.5" /></svg>;
const secs = (s: number) => (s % 86_400 === 0 ? `${s / 86_400} ${s === 86_400 ? "day" : "days"}` : `${s} s`);

/*
  The sequence for a message on a workspace's own data. Every value is one its stored spans carried (tb_msg_span, and
  tb_msg_meta's contents) or one the connected account returned (GetQueueAttributes, ListEventSourceMappings) when it was
  read; anything else says "Not recorded" or "Not read". Nothing here comes from the sample tables or is made up.
*/
export function sequenceOf(events: readonly Ev[], k?: QueueFacts): Sequence {
  const evs = [...events].sort((x, y) => x.ms - y.ms);
  const first = evs[0];
  const queue = first.queue;
  const read = !!k?.read;
  const nr = <S key="nr">{NOT_RECORDED}</S>, nread = <S key="nrd">{NOT_READ}</S>;
  // a FIFO queue's name must end in .fifo, so the name says it even when the queue's attributes were not read
  const fifo = k?.fifo ?? queue.endsWith(".fifo");
  const hasDlq = evs.some((e) => e.event === "dead_letter");
  const consumer = evs.find((e) => e.event === "receive")?.consumer || first.consumer;
  // the Lambda trigger this consumer is: one of the queue's event source mappings, named as the function
  const trigger = k?.triggers?.find((t) => t.functionName === consumer);
  const sqs = `Amazon SQS, ${fifo ? "FIFO" : "standard"} queue`;
  const parts: { part: Part; role: string; name: string; icon: ReactNode; platform: string }[] = [
    { part: "producer", role: "Producer", name: first.producer || NOT_RECORDED, icon: SERVICE_ICON, platform: "Where it runs is not recorded" },
    { part: "queue", role: "Queue", name: queue, icon: SQS_ICON, platform: sqs },
    { part: "consumer", role: "Consumer", name: consumer || NOT_RECORDED, icon: trigger ? PLATFORM_ICON.Lambda : SERVICE_ICON, platform: trigger ? `${PLATFORM_LABEL.Lambda}, from the queue's event source mappings` : "Where it runs is not recorded" },
    ...(hasDlq ? [{ part: "dlq" as const, role: "Dead-letter queue", name: k?.dlq ?? "dead-letter queue", icon: SQS_ICON, platform: sqs }] : []),
  ];
  const max = k?.maxReceiveCount ?? null;
  const maxRow: [string, ReactNode][] = max ? [["maxReceiveCount", max]] : [];
  // the visibility timeout: the queue's, or SQS's default when it was not read, said so wherever it is used
  const visKnown = read && k?.visibilityTimeoutS !== null && k?.visibilityTimeoutS !== undefined;
  const visS = visKnown ? k!.visibilityTimeoutS! : 30;
  const visRow: [string, ReactNode] = ["Visibility timeout", visKnown ? `${visS} s` : <>30 s <S>SQS&apos;s default; the queue&apos;s setting was not read</S></>];
  const fromDefault = visKnown ? "" : " (from SQS's default 30 s timeout)";
  const num = (v: unknown) => (typeof v === "number" ? v : undefined);

  let lastReceive: Ev | undefined;
  const steps: Step[] = evs.map((e): Step => {
    const src: [string, ReactNode] = ["Recorded by", recorded(e)];
    const a = e.attrs ?? {};
    if (e.event === "send")
      return {
        ev: e, name: "Sent", draw: { kind: "arrow", a: 0, b: 1, call: "SendMessage" },
        say: `${e.producer || "A producer"} sent this message to ${queue}.`,
        facts: [["API call", "SendMessage"], ...(fifo ? ([["Message group ID", a["message.MessageGroupId"] ?? nr]] as [string, ReactNode][]) : []), ["Body size", e.sizeBytes !== undefined ? fmtBytes(e.sizeBytes) : nr], src],
      };
    if (e.event === "receive") {
      const n = e.receiveCount;
      lastReceive = e;
      const size = num(a["batch.size"]), idx = num(a["batch.index"]);
      return {
        ev: e, name: n ? `Receive #${n}` : "Received", draw: { kind: "arrow", a: 1, b: 2, call: "ReceiveMessage" },
        say: `${e.consumer || "A consumer"} received the message${n ? ` ${ORD[n - 1] ?? `for receive ${n}`}` : ""}${e.waitMs !== undefined ? `, ${fmtMs(Math.round(e.waitMs))} after it was sent` : ""}.${max && n === max ? " This is the last receive the redrive policy allows." : ""}${e.detail ? ` ${e.detail}.` : ""}`,
        facts: [
          ["Receive count", n ?? nr],
          ...maxRow,
          ["In flight until", <>{fmtT(e.ms + visS * 1000)}{!visKnown && <S> from SQS&apos;s default</S>}</>],
          visRow,
          ["Received through", trigger ? <S key="d">Lambda event source mapping</S> : nr],
          ["Batch size", size !== undefined ? <>{size} <S>{size === 1 ? "message" : "messages"}</S></> : nr],
          ...(trigger ? ([["Batch size limit", trigger.batchSize ?? nread]] as [string, ReactNode][]) : []),
          ["Position in batch", idx !== undefined && size !== undefined ? <>{idx + 1} <S>of {size}</S></> : nr],
          ...(trigger ? ([["Partial batch response", <S key="p">{trigger.reportBatchItemFailures ? "On" : "Off"}</S>]] as [string, ReactNode][]) : []),
          src,
        ],
      };
    }
    if (e.event === "processing_error") {
      const back = lastReceive ? fmtT(lastReceive.ms + visS * 1000) : undefined;
      return {
        ev: e, name: "Processing failed", draw: { kind: "mark", at: 2, tone: "x", text: e.detail.length <= 34 ? e.detail : "Processing failed" },
        say: "The consumer did not finish processing the message, so it was not deleted and stayed in flight.",
        facts: [["Error", e.detail || nr], ...(e.durationMs !== undefined ? ([["Processing time", fmtMs(e.durationMs)]] as [string, ReactNode][]) : []), ["Receive count", e.receiveCount ?? nr], ...maxRow, ...(back ? ([["Visible again at", <>{back}{!visKnown && <S> from SQS&apos;s default</S>}</>]] as [string, ReactNode][]) : []), src],
      };
    }
    if (e.event === "visibility_expired")
      return {
        ev: e, name: "Visibility timeout expired", draw: { kind: "mark", at: 1, tone: "tick", text: "Visibility timeout expired", inferred: true },
        say: `A later receive shows the visibility timeout that started at the receive ran out${fromDefault}, so the message became visible again. Nothing recorded this step.`,
        facts: [["Visible again at", fmtT(e.ms)], visRow, ["Receive count so far", e.receiveCount ?? nr], src],
      };
    if (e.event === "delete")
      return {
        ev: e, name: "Deleted", draw: { kind: "arrow", a: 2, b: 1, call: "DeleteMessage" },
        say: `${e.consumer || "The consumer"} deleted the message.${e.detail ? ` ${e.detail}.` : ""}`,
        facts: [["API call", "DeleteMessage"], ...(e.durationMs !== undefined ? ([["Processing time", fmtMs(e.durationMs)]] as [string, ReactNode][]) : []), src],
      };
    if (e.event === "dead_letter")
      return {
        ev: e, name: "Dead-lettered", draw: { kind: "arrow", a: 1, b: 3, call: "Moved by the redrive policy", inferred: e.source === "inferred" },
        say: `${e.source === "inferred" ? "Nothing recorded this step. " : ""}SQS moved the message to the dead-letter queue by the redrive policy.${e.detail ? ` ${e.detail}.` : ""}`,
        facts: [["Dead-letter queue", k?.dlq ?? (read ? nr : nread)], ["Receive count", e.receiveCount ?? nr], ...maxRow, src],
      };
    return {
      ev: e, name: "Dropped by the event filter", draw: { kind: "mark", at: 1, tone: "warn", text: "Dropped by the event filter" },
      say: "The event source mapping's filter did not match this message, so Lambda removed it from the queue without invoking the function.",
      facts: [["Reason", e.detail ? <S key="r">{e.detail}</S> : nr], src],
    };
  });
  const bars = barsOf(steps);

  /* What the spans recorded about the message, under SQS's attribute names */
  const attr = (key: string) => { for (const e of evs) { const v = e.attrs?.[key]; if (v !== undefined) return v; } return undefined; };
  const sent = num(attr("message.SentTimestamp")), firstRx = num(attr("message.ApproximateFirstReceiveTimestamp"));
  const counts = evs.map((e) => e.receiveCount).filter((n): n is number => n !== undefined);
  const size = first.sizeBytes ?? num(attr("message.BodySize"));
  const tenant = first.tenant ?? attr("message.tenant");
  const tenantFrom = attr("message.tenantSource");
  const messageAttrs: [string, ReactNode][] = [
    ["MessageId", first.messageId],
    ["SentTimestamp", sent !== undefined ? fmtT(sent) : nr],
    ["ApproximateFirstReceiveTimestamp", firstRx !== undefined ? fmtT(firstRx) : counts.length ? nr : <S key="n">Not received</S>],
    ["ApproximateReceiveCount", counts.length ? Math.max(...counts) : evs.some((e) => e.event === "receive") ? nr : 0],
    ...(fifo ? ([["MessageGroupId", attr("message.MessageGroupId") ?? nr], ["SequenceNumber", attr("message.SequenceNumber") ?? nr]] as [string, ReactNode][]) : []),
    ["Body size", size !== undefined ? fmtBytes(size) : nr],
    ...(tenant !== undefined ? ([["Tenant", <>{tenant}{tenantFrom ? <S> read from {String(tenantFrom).replace(/_/g, " ")}</S> : null}</>]] as [string, ReactNode][]) : []),
  ];
  /* The queue's settings, as GetQueueAttributes returned them; none when it was not read */
  const queueAttrs: [string, ReactNode][] = read
    ? [
        ["FifoQueue", String(fifo)],
        ["VisibilityTimeout", k!.visibilityTimeoutS !== null ? `${k!.visibilityTimeoutS} s` : nread],
        ["MessageRetentionPeriod", k!.retentionS !== null ? secs(k!.retentionS) : nread],
        ["DelaySeconds", k!.delayS !== null && k!.delayS !== undefined ? `${k!.delayS} s` : nread],
        ...(k!.maxReceiveCount !== null && k!.dlq
          ? ([["RedrivePolicy.maxReceiveCount", k!.maxReceiveCount], ["RedrivePolicy dead-letter queue", k!.dlq]] as [string, ReactNode][])
          : ([["RedrivePolicy", <S key="r">None</S>]] as [string, ReactNode][])),
      ]
    : [
        ["FifoQueue", <>{String(fifo)} <S>from the queue&apos;s name</S></>],
        ["GetQueueAttributes", <S key="n">Not read: connect the account that owns {queue} to see its settings</S>],
      ];
  /* The Lambda trigger, as ListEventSourceMappings returned it */
  const mappingAttrs: [string, ReactNode][] = trigger
    ? [["BatchSize", trigger.batchSize ?? nread], ["MaximumBatchingWindowInSeconds", trigger.batchingWindowS !== null ? `${trigger.batchingWindowS} s` : nread], ["ReportBatchItemFailures", String(trigger.reportBatchItemFailures)]]
    : [];
  return { evs, parts, steps, bars, messageAttrs, queueAttrs, mappingAttrs };
}

/* history: the message's events (all of them, or just this one's when absent). sequence: the diagram, when the host built it
   (the hosted app's sample); otherwise it is worked out from history with the queue's facts. payload: its stored body.
   stacked: go by the width of the container, not the page: the panels sit beside the diagram when the container
   (marked @container by the caller) has room, and under it when not. messageHref: the message page, to offer it.
   settingsHref: where payload capture is turned on, for a message whose body was not kept. A history passed in may gain
   steps while it is open, and the diagram makes room for them. */
export type MessageSequenceProps = {
  event: Ev; history?: readonly Ev[]; sequence?: Sequence; payload?: unknown; stacked?: boolean; queue?: QueueFacts;
  messageHref?: string | null; settingsHref?: string | null;
};

export function MessageSequence({ event, history, sequence, payload, stacked = false, queue, messageHref, settingsHref }: MessageSequenceProps) {
  const own = (history ?? []).filter((e) => e.messageId === event.messageId);
  const { parts, steps, bars, messageAttrs, queueAttrs, mappingAttrs } = sequence ?? sequenceOf(own.length ? own : [event], queue);
  const [sel, setSel] = useState(Math.max(0, steps.findIndex((s) => s.ev.key === event.key)));
  /* A message that is still in a queue gains steps while it is open. The diagram moves to
     make room instead of redrawing (the .qs-live rules), and the selection stays on the
     newest step for as long as it was on the newest step. */
  const [live, setLive] = useState(false);
  useEffect(() => setLive(!!history), [history]);
  const count = useRef(steps.length);
  useEffect(() => {
    if (steps.length > count.current && sel === count.current - 1) setSel(steps.length - 1);
    count.current = steps.length;
  }, [steps.length, sel]);
  const X = (k: number) => GUT + COL * k + COL / 2;
  /* The space between two steps is to scale with the time between them. The scale is
     set per message so that two thirds of its gaps fit: the gap at that rank is drawn
     at GAP_MAX and the shorter ones in proportion to it. A longer wait would push the
     rest off the screen, so it is drawn at GAP_MAX with a break mark on the bars that
     cross it. A gap too short to keep two rows apart is drawn at GAP_MIN. The true
     duration is always written on the bar. */
  const dts = steps.slice(1).map((st, i) => st.ev.ms - steps[i].ev.ms);
  const ranked = [...dts].sort((a, b) => a - b);
  const pxPerMs = GAP_MAX / Math.max(1, ranked[Math.max(0, Math.ceil(ranked.length * 0.67) - 1)] ?? 1);
  const gaps = dts.map((dt) => ({ h: Math.min(GAP_MAX, Math.max(GAP_MIN, Math.round(dt * pxPerMs))), cut: dt * pxPerMs > GAP_MAX * 1.05, floor: dt * pxPerMs < GAP_MIN * 0.95 }));
  const ys = gaps.reduce<number[]>((a, g) => [...a, a[a.length - 1] + g.h], [0]);
  /* a whole index is a step; a bar that is still open ends a fraction past the last one */
  const mid = (i: number) => TOP + (Number.isInteger(i) ? ys[i] : ys[Math.floor(i)] + (i - Math.floor(i)) * GAP_MIN);
  const anyCut = gaps.some((g) => g.cut), anyFloor = gaps.some((g) => g.floor);
  /* The break is the one thing in the drawing that is not to scale. Its tooltip says only
     what it stands for; the person is already pointing at it. */
  const scaleNote = "A longer wait, drawn shorter.";
  const scaleWords = `The space between steps is to scale with the time between them.${anyCut ? " A break across the lines marks a longer wait, drawn shorter." : ""}${anyFloor ? " The shortest gaps are drawn at a minimum height." : ""}`;
  /* the lifelines a cut gap is marked on: those with a bar across it, or the queue's if none */
  const cutCols = (i: number) => { const c = [...new Set(bars.filter((b) => b.from <= i && b.to >= i + 1).map((b) => b.col))]; return c.length ? c : [1]; };
  const width = GUT + COL * parts.length + (parts.length > 3 ? 30 : 150);
  const message = payload !== undefined && payload !== null ? { payload } : undefined;
  const size = steps.find((s) => s.ev.event === "send")?.ev.sizeBytes;
  const btn = "inline-flex h-7 items-center rounded-md border border-line-strong bg-surface px-2.5 font-sans text-[13px] font-medium text-ink hover:border-ink-3 disabled:opacity-40 disabled:hover:border-line-strong";
  const tabs: { id: string; label: string; rows: Fact[] }[] = [
    { id: "body", label: "Message body", rows: [] },
    { id: "message", label: "Message attributes", rows: messageAttrs },
    { id: "queue", label: "Queue attributes", rows: queueAttrs },
    ...(mappingAttrs.length ? [{ id: "mapping", label: "Event source mapping", rows: mappingAttrs }] : []),
  ];
  const [tab, setTab] = useState("body");

  return (
    /* The row is as tall as the taller of the two sides. The right side is as
       tall as its longest step, so the height does not change as steps are
       selected, and the diagram's lifelines stretch to match it. */
    <div className={`qs grid gap-x-10 gap-y-6 font-sans text-[13px] text-ink-2 ${live ? "qs-live" : ""} ${stacked ? "@[70rem]:grid-cols-[max-content_minmax(0,1fr)]" : "xl:grid-cols-[max-content_minmax(0,1fr)]"}`}>
      <div className="min-w-0 overflow-x-auto pb-1">
        <div className="flex h-full flex-col" style={{ width }}>
          <div className="qs-heads" style={{ paddingLeft: GUT }}>
            {parts.map((p) => (
              <div key={p.part} className="qs-head" style={{ width: COL }}>
                <Tooltip content={p.platform} className="ic">{p.icon}</Tooltip>
                <b>{p.role}</b>
                <span className="nm">{p.name}</span>
              </div>
            ))}
          </div>
          <div className="qs-body flex-1" style={{ minHeight: mid(steps.length - 1) + GAP_MIN }}>
            {parts.map((p, k) => <i key={p.part} className="qs-life" style={{ left: X(k) }} />)}
            {bars.map((b, i) => (
              <div key={`${b.kind}-${steps[Math.floor(b.from)].ev.key}`} className={`qs-bar ${b.kind === "hidden" ? "inflight" : b.kind} ${b.open ? "open" : ""}`} style={{ left: X(b.col) - 6, top: mid(b.from), height: mid(b.to) - mid(b.from) }}>
                <span className={b.col === 2 ? "r" : "l"}>{b.word}{b.num && <b> {b.num}</b>}</span>
              </div>
            ))}
            {/* a gap drawn shorter than its time: a break across every lifeline, and any bar on it */}
            {gaps.map((g, i) => g.cut && cutCols(i).map((k) => (
              <Tooltip key={`${i}-${k}`} content={scaleNote} className="qs-break-hit" style={{ left: X(k), top: mid(i) + (mid(i + 1) - mid(i)) * 0.3 }}>
                <i className="qs-break" aria-hidden />
              </Tooltip>
            )))}
            {steps.map((s, i) => (
              <button key={s.ev.key} type="button" className={`qs-row ${i === sel ? "on" : ""}`} style={{ top: mid(i) - ROW / 2 }} aria-pressed={i === sel} aria-label={`${s.name}, ${fmtT(s.ev.ms)}`} onClick={() => setSel(i)}>
                <span className="t">
                  {fmtT(s.ev.ms)}
                  {i > 0 && <span className="ml-1.5 text-ink-3">(+{fmtMs(s.ev.ms - steps[0].ev.ms)})</span>}
                </span>
                {s.draw.kind === "arrow" ? (
                  <span
                    className={`qs-arrow ${s.draw.b > s.draw.a ? "ltr" : "rtl"} ${s.draw.inferred ? "inferred" : ""}`}
                    style={{ left: Math.min(X(s.draw.a), X(s.draw.b)) + 9, width: Math.abs(X(s.draw.b) - X(s.draw.a)) - 18 }}
                  >
                    <span>{s.draw.call}</span>
                  </span>
                ) : (
                  <>
                    <span className={`qs-mark ${s.draw.tone}`} style={{ left: X(s.draw.at) }} />
                    <span className={`qs-chip ${s.draw.tone} ${s.draw.inferred ? "inferred" : ""}`} style={{ left: X(s.draw.at) + 17 }}>{s.draw.text}</span>
                  </>
                )}
              </button>
            ))}
          </div>
          {/* with a break in the drawing, the note is the break's tooltip, and here for a screen
              reader; with none there is nothing to hover, so it stays as a caption */}
          <p className={anyCut ? "sr-only" : "mt-2 pl-2.5 text-xs text-ink-3"}>{scaleWords}</p>
        </div>
      </div>

      <div className="@container min-w-0 xl:pt-1">
        <div className="grid items-start gap-5 @3xl:grid-cols-[minmax(0,1.05fr)_minmax(0,1fr)]">
          {/* What changes: the selected step. Its buttons are in the header so they never move. */}
          <section aria-label="Selected step" className="overflow-hidden rounded-xl border border-line bg-surface">
            <header className="flex flex-wrap items-center gap-2 border-b border-line bg-sunken/50 px-3.5 py-2">
              <h3 className="mr-auto text-[13px] font-medium text-ink-2">
                Step <span className="font-mono tabular-nums text-accent">{sel + 1}</span> of <span className="font-mono tabular-nums">{steps.length}</span>
              </h3>
              <button type="button" className={btn} disabled={sel === 0} onClick={() => setSel(sel - 1)}>Previous</button>
              <button type="button" className={btn} disabled={sel === steps.length - 1} onClick={() => setSel(sel + 1)}>Next</button>
            </header>
            {/* Every step is laid out in the same cell and only the selected one shows, so the card is as tall as the longest. */}
            <div className="grid" aria-live="polite">
              {steps.map((st, i) => (
                <div key={st.ev.key} className={`col-start-1 row-start-1 px-4 pt-3.5 pb-2 ${i === sel ? "" : "invisible"}`} aria-hidden={i !== sel}>
                  <h4 className="flex items-center gap-2 text-[17px] leading-snug font-semibold text-ink"><i className="block size-2 shrink-0 rounded-full bg-accent" />{st.name}</h4>
                  <p className="mt-1 mb-3 max-w-[62ch] text-sm leading-normal text-ink-2">{st.say}</p>
                  <Rows rows={[["Time", fmtT(st.ev.ms)], who(st.ev), ...st.facts]} />
                </div>
              ))}
            </div>
          </section>

          {/* What does not change from step to step: the message and how its queue is set up. One tab at a time. */}
          <section aria-label="About the message" className="overflow-hidden rounded-xl border border-line bg-surface">
            <div role="tablist" aria-label="About the message" className="flex flex-wrap items-end gap-x-1 border-b border-line bg-sunken/50 px-2 pt-1">
              {tabs.map((t) => (
                <button
                  key={t.id}
                  type="button"
                  role="tab"
                  aria-selected={tab === t.id}
                  onClick={() => setTab(t.id)}
                  className={`-mb-px border-b-2 px-2.5 py-2 text-[13px] whitespace-nowrap ${tab === t.id ? "border-accent font-medium text-accent" : "border-transparent text-ink-3 hover:text-ink"}`}
                >
                  {t.label}
                </button>
              ))}
              {messageHref && <UiLink href={messageHref} className="mb-1.5 ml-auto rounded-md px-2.5 py-1 text-[13px] font-medium text-brand hover:bg-sunken">Open message</UiLink>}
            </div>
            {/* All tabs share one cell, so switching tabs does not change the height either. */}
            <div className="grid">
              {tabs.map((t) => (
                <div key={t.id} role="tabpanel" aria-label={t.label} className={`col-start-1 row-start-1 min-w-0 px-4 py-3.5 ${tab === t.id ? "" : "invisible"}`} aria-hidden={tab !== t.id}>
                  {t.id === "body" ? (
                    message ? (
                      <>
                        <pre className="overflow-x-auto rounded-lg border border-line bg-canvas px-3.5 py-3 font-mono text-caption leading-[1.6] text-ink">{JSON.stringify(message.payload, null, 2)}</pre>
                        <p className="mt-2 text-xs text-ink-3">{size ? `${fmtBytes(size)}. ` : ""}Shown with your redaction rules applied. The body is the same at every step.</p>
                      </>
                    ) : event.live ? (
                      <p className="rounded-lg border border-dashed border-line-strong px-3.5 py-3 text-[13px] text-ink-2">
                        The body is not shown here. If Trailbeam kept it, it is on the message&apos;s timeline.
                      </p>
                    ) : (
                      <p className="rounded-lg border border-dashed border-line-strong px-3.5 py-3 text-[13px] text-ink-2">
                        Payload capture is off for {event.queue}, so Trailbeam did not store this body.{settingsHref ? <> You can turn it on in <UiLink href={settingsHref} className="text-brand hover:underline">Settings</UiLink>.</> : null}
                      </p>
                    )
                  ) : (
                    <Rows mono rows={t.rows} />
                  )}
                </div>
              ))}
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

/* A two-column list with a hairline between rows, so each fact reads as its own line. */
export function Rows({ rows, mono = false }: { rows: Fact[]; mono?: boolean }) {
  return (
    <dl className="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-6 text-[13px]">
      {rows.map(([k, v]) => (
        <div key={k} className="col-span-2 grid grid-cols-subgrid border-t border-line py-[5px] first:border-t-0">
          <dt className={mono ? "font-mono text-caption leading-[1.55] text-ink-3" : "leading-[1.55] text-ink-3"}>{k}</dt>
          <dd className="font-mono text-caption leading-[1.55] text-ink [overflow-wrap:anywhere]">{v}</dd>
        </div>
      ))}
    </dl>
  );
}
