"use client";

import { useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { ChevronRight, CircleAlert, CircleCheck } from "lucide-react";
import { PlatformIcon } from "../design/icons.tsx";
import { TopBar, type Fresh } from "../design/TopBar.tsx";
import { HealthPill } from "../design/pills.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { MiddleTruncate } from "../design/MiddleTruncate.tsx";
import { UiLink, useNav } from "../design/nav.tsx";
import { fmtBytes, fmtMs, fmtNum, fmtRate, OVERVIEW_RANGES as RANGES, PLATFORM_LABEL, type OverviewRangeId } from "../model/format.ts";
import { OUTCOME_FILL, OUTCOME_LABEL, OUTCOME_TEXT, type Cause, type CauseOutcome } from "../model/causes.ts";
import type { Edge, OverviewData, Queue } from "../model/overview.ts";
import type { QueueLive } from "../model/queue-live.ts";
import type { InQueueView } from "../model/in-queue.ts";
import { phrase, stamp, type TimeRange } from "../model/timerange.ts";
import { TimePicker } from "../explorer/TimePicker.tsx";
import { SPAN_HEALTH_NOTE } from "../queue/model.ts";
import type { Trigger } from "../queue/types.ts";
import { QueueNowLive } from "./QueueNowLive.tsx";
import { Watching } from "./Watching.tsx";

/* The Overview, one flow at a time. A flow is a queue with the services that send to it
   and the consumer that receives from it. The list on the left is every flow, worst
   first. The one that is selected is drawn large on the right, and the area under the
   drawing says more about whichever part of it was last selected. Nothing else is on
   the page until it is asked for.

   The page has two sources. Without `data` it draws `sample` (the host passes it: the hosted app's lib/overview-sample.ts). With `data` it
   draws a workspace's own flows, built from the spans its services reported, with what
   AWS says about each queue in a connected account (data.info): its settings, depth and
   age, its Lambda triggers, and the messages in it now. Each consumer's p95 processing time
   and failures come from its deletes and processing errors in the range (data.consumerStats).
   Wherever none of these says something the sample shows the page says "No data".

   Copy that names one of the host's own features (where AWS is connected, the sample switch, how to connect a
   service) comes from the host: `copy` and `slots` (the hosted app's HostedOverview.tsx). So do its links (`hrefs`),
   and the sample's own parts: its simulated queue (slots.sampleNow) and its rows under a part (slots.partMessages). */

/** The sample topology the page draws without data: the hosted app's lib/overview-sample.ts. */
export type OverviewSample = {
  queues: Queue[];
  causes: Cause[];
  sends: Edge[];
  receives: Edge[];
  consumers: { name: string; runtime: string; queue: string }[];
  producerPlatform: Record<string, string>;
  settings: Record<string, { visibilityTimeoutS: number | null; maxReceiveCount: number | null; dlq: string | null; retentionDays: number | null }>;
  stats: Record<string, { visible: number; notVisible: number; oldestS: number }>;
  consumerStats: Record<string, { p95Ms: number; failPct: number; timeouts: number }>;
  avgBytes: Record<string, number>;
  rangeScale: Record<string, { k: number; c: number }>;
};
/** A Lambda trigger on a queue, as the consumer panel lists it (the hosted app's aws-reads Trigger fits it). */
export type OverviewTrigger = Trigger & { uuid?: string };
/** What the page draws for recorded data, with the host's triggers and "In ‹queue› now" views. */
export type OverviewFlowsData = OverviewData<OverviewTrigger, InQueueView>;

/** What the page says that depends on the host's features. */
export type OverviewCopy = {
  /** the heading when no queue has health data */
  noHealth: (queues: number) => string;
  /** added to the heading when some health is from spans alone */
  spansOnly: string;
  /** why a queue has no figures from AWS, under its card */
  noFigures: (live: QueueLive | undefined) => string;
};
/** Parts of the page only the host has. */
export type OverviewFlowsSlots = {
  /** under "No data yet": how to connect a service, or see sample data */
  emptyActions?: ReactNode;
  /** in the top bar, on recorded data: a way to the sample */
  sampleSwitch?: ReactNode;
  /** in the top bar on the sample: the host's message search */
  search?: ReactNode;
  /** on the sample, under the flow when the queue is selected: the host's simulated "In ‹queue› now" */
  sampleNow?: (queue: string) => ReactNode;
  /** under a selected producer, consumer or dead-letter queue: the messages behind it (PartMessages), or nothing */
  partMessages?: (o: PartMessagesAt) => ReactNode;
  /** in the top bar, on recorded data, before the range: the host's own controls (open source: Copy link) */
  actions?: ReactNode;
  /** in the queue's card, above its link, on recorded data: the host's own figures for it (open source: the range's totals from spans) */
  queueFacts?: (queue: string) => ReactNode;
  /** under the page, on recorded data: where the figures come from, and the same data as JSON */
  foot?: ReactNode;
};
/** Which messages a part of the flow map stands for, for slots.partMessages. */
export type PartMessagesAt = {
  queue: string; part: "producer" | "consumer" | "dlq"; producer?: string; consumer?: string;
  /* the dead-lettered count, for the dead-letter queue; the part's rate per minute; the queue's maxReceiveCount */
  count: number; perMin: number; max: number | null; real: boolean;
};
/** Where the page links to: paths in the host's app. */
export type OverviewFlowsHrefs = {
  /** this page at another range, on recorded data (the sample changes range in place) */
  range: (id: OverviewRangeId) => string;
  /** a queue's own page at the Overview's range */
  queue: (o: { name: string; range: OverviewRangeId; real: boolean }) => string;
  /** the Issues page for one queue */
  issues: (queue: string) => string;
  /** a cause's own page */
  cause: (c: Cause) => string;
  /** the Event explorer with a query typed in */
  events: (query: string) => string;
  /** the dead-lettered messages of a queue, or the messages in its dead-letter queue */
  deadLettered: (o: { queue: string; dlq: string | null; real: boolean }) => string;
  /** a message's own page */
  message: (id: string) => string;
};
const NO_SAMPLE: OverviewSample = { queues: [], causes: [], sends: [], receives: [], consumers: [], producerPlatform: {}, settings: {}, stats: {}, consumerStats: {}, avgBytes: {}, rangeScale: {} };

/** The list's heading, in counts that agree with their nouns: "The queue is healthy", not "All 1 queues are healthy". */
export function healthHeading(queues: number, judged: number, troubled: number): string {
  const unjudged = queues - judged;
  if (troubled) {
    const head = queues === 1 ? "The queue needs attention" : `${fmtNum(troubled)} of ${fmtNum(queues)} queues ${troubled === 1 ? "needs" : "need"} attention`;
    return unjudged ? `${head}; ${fmtNum(unjudged)} ${unjudged === 1 ? "has" : "have"} no health data` : head;
  }
  if (judged === queues) return queues === 1 ? "The queue is healthy" : queues === 2 ? "Both queues are healthy" : `All ${fmtNum(queues)} queues are healthy`;
  return judged === 1 ? `1 of ${fmtNum(queues)} queues has health data, and it is healthy` : `${fmtNum(judged)} of ${fmtNum(queues)} queues have health data, and they are healthy`;
}

/**
 * The figure on the flow map's DLQ node, with its note. On recorded data: the messages in the dead-letter queue now
 * when AWS said, else the receives on it in the range (all the spans can count). On the sample: the dead-lettered.
 */
export function dlqNodeCount(o: { real: boolean; queue: string; dlq: string; depth: number | null; received: number; moved: number; range: string }): { n: number; note: string } {
  if (!o.real) return { n: o.moved, note: `Messages from ${o.queue} dead-lettered in ${o.range}` };
  const moved = ` ${fmtNum(o.moved)} ${o.moved === 1 ? "message" : "messages"} moved to it in ${o.range}.`;
  if (o.depth != null) return { n: o.depth, note: `Messages in ${o.dlq} now, from CloudWatch.${moved}` };
  return { n: o.received, note: `Receives on ${o.dlq} in ${o.range} by your services that SQS marked as coming from ${o.queue}.${moved}` };
}

const NoData = () => <span className="font-sans font-normal text-ink-3">No data</span>;
/* A figure only CloudWatch has, for a queue CloudWatch has not published yet. */
const NotPublished = ({ what }: { what: string }) => (
  <Tooltip content={`CloudWatch has not published ${what} yet. It starts a few minutes after a queue is created or first used, and does not fill in the minutes before.`} className="font-sans font-normal text-ink-3">Not in CloudWatch yet</Tooltip>
);
/* CloudWatch's figures (the oldest message's age, the dead-letter queue's depth) are for the minute that
   ended a little before the read, so they trail the clock by a minute or two. They say when they are from. */
const AsOf = ({ ms }: { ms: number | null }) => (ms === null ? null : (
  <Tooltip content={`CloudWatch's latest minute, which ended at ${stamp(ms)} UTC. CloudWatch publishes each minute about half a minute after it ends, so this figure trails the true one by a minute or two.`} className="ml-1.5 font-sans text-[11px] font-normal text-ink-3">as of {stamp(ms)}</Tooltip>
));

type Part = { kind: "queue" } | { kind: "producer"; name: string } | { kind: "consumer"; name: string } | { kind: "dlq" };
/* A wire takes the color of what travels on it: sends are the send color, receives the
   receive color (the series tokens, the same ones the charts use). Trouble overrides both. */
type Tone = "ok" | "warn" | "bad" | "send" | "receive";
const TONE: Record<Tone, string> = { ok: "var(--color-ink-3)", warn: "var(--color-serious)", bad: "var(--color-critical)", send: "var(--color-series-2)", receive: "var(--color-series-1)" };
const SEND = "var(--color-series-2)", RECEIVE = "var(--color-series-1)", HANDLE = "var(--color-series-3)";
/* the small square that says which color a word goes with */
const Key = ({ color }: { color: string }) => <span aria-hidden className="mr-1.5 inline-block size-2 rounded-[2px] align-baseline" style={{ background: color }} />;
const ORDER: CauseOutcome[] = ["dead_lettered", "stuck", "filtered"];
const panel = "rounded-card border border-line bg-surface shadow-card";
const age = (s: number) => (s < 60 ? `${s} s` : `${Math.floor(s / 60)} min ${s % 60} s`);
const fieldQuery = (field: string, name: string) => `${field}:${/[\s()]/.test(name) ? `"${name}"` : name}`;

function useWidth<T extends Element>() {
  const ref = useRef<T>(null);
  const [w, setW] = useState(0);
  useLayoutEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(([e]) => setW(e.contentRect.width));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return [ref, w] as const;
}

/* Curved, dashed connectors across one gap of the drawing. Dashes move the way messages do.
   A wire can carry its rate: messages per minute over bytes per minute, set beside the wire
   where it leaves or meets a node, on the side away from its bend, so the wire never
   crosses its own label and labels on converging wires stay apart. Each label sits on a pad of the page's
   background, so no dash runs through a figure ("3.9" never reads as "-3.9"). */
type Line = { key: string; y1: number; y2: number; tone: Tone; rate?: { n: string; bytes?: string; at: "start" | "end" } };
/* a label's height: one 13 px line, or two, and its pad */
const LABEL_H = (two: boolean) => (two ? 26 : 13) + 4;
/** Where a wire's label goes: its top edge, from the end it is set at and the side away from the bend. */
export function labelTop(l: Pick<Line, "y1" | "y2"> & { at: "start" | "end"; two: boolean }): number {
  const yy = l.at === "end" ? l.y2 : l.y1;
  const other = l.at === "end" ? l.y1 : l.y2;
  const below = Math.abs(other - yy) > 1 && other < yy;
  return below ? yy + 5 : yy - 5 - LABEL_H(l.two);
}
function Wires({ lines, height }: { lines: Line[]; height: number }) {
  const [ref, w] = useWidth<HTMLDivElement>();
  return (
    <div ref={ref} className="relative" style={{ height }}>
      {w > 0 && (
        <svg width={w} height={height} className="block overflow-visible" aria-hidden>
          {lines.map((l) => {
            const end = w - 7;
            const d = `M0 ${l.y1} C${end * 0.5} ${l.y1} ${end * 0.5} ${l.y2} ${end} ${l.y2}`;
            return (
              <g key={l.key}>
                <path d={d} fill="none" stroke={TONE[l.tone]} strokeWidth={1.5} strokeDasharray="5 9" className="flow-dash" style={{ animationDuration: "0.9s" }} />
                <path d={`M${w - 8} ${l.y2 - 4} L${w - 1} ${l.y2} L${w - 8} ${l.y2 + 4} Z`} fill={TONE[l.tone]} />
              </g>
            );
          })}
        </svg>
      )}
      {w > 0 && lines.map((l) => {
        const r = l.rate;
        if (!r) return null;
        const top = labelTop({ y1: l.y1, y2: l.y2, at: r.at, two: !!r.bytes });
        return (
          <span key={`label:${l.key}`} data-wire-label="" className={`absolute rounded bg-canvas px-1 py-0.5 text-[11px] leading-[13px] whitespace-nowrap ${r.at === "end" ? "right-3 text-right" : "left-2"}`} style={{ top }}>
            <span className="block"><span className="font-mono text-ink tabular-nums">{r.n}</span><span className="text-ink-3"> messages/min</span></span>
            {r.bytes && <span className="block"><span className="font-mono text-ink-2 tabular-nums">{r.bytes}</span><span className="text-ink-3">/min</span></span>}
          </span>
        );
      })}
    </div>
  );
}

const Fact = ({ k, children }: { k: string; children: ReactNode }) => (
  <div>
    <dt className="text-xs text-ink-3">{k}</dt>
    <dd className="mt-0.5 font-mono text-[13px] text-ink tabular-nums [overflow-wrap:anywhere]">{children}</dd>
  </div>
);

export type OverviewFlowsProps = {
  data?: OverviewFlowsData; sample?: OverviewSample; flow?: string; copy: OverviewCopy; slots?: OverviewFlowsSlots; hrefs: OverviewFlowsHrefs;
  /** told when another flow is selected, so a host can keep it in the page's URL */
  onFlow?: (queue: string) => void;
  /**
   * On recorded data, the page's one time range, chosen with the Event explorer's TimePicker in place of the three
   * range buttons: any preset, a typed range or exact times. `now` is where a range that ends now ends. Without it the
   * page offers 15 min, 1 hour and 24 hours (hrefs.range), as the sample, sized for those three, always does.
   */
  time?: { range: TimeRange; now: number; onChange: (t: TimeRange) => void };
  /** the top bar's freshness mark (TopBar's `fresh`): how long ago the figures were read, e.g. <Since ms={readAt} /> */
  fresh?: Fresh;
};

export function OverviewFlows({ data, sample = NO_SAMPLE, flow: initialFlow, copy, slots = {}, hrefs, onFlow, time: timeProp, fresh }: OverviewFlowsProps) {
  const router = useNav();
  const real = Boolean(data);
  const queues: Queue[] = data?.queues ?? sample.queues;
  /* why messages didn't finish, one row per cause: on own data, every unfinished message grouped by cause (lib/app/causes.ts) */
  const causes: Cause[] = data ? data.causes : sample.causes;
  const sends = data?.sends ?? sample.sends;
  const receives = data?.receives ?? sample.receives;
  const consumers: { name: string; runtime: string; queue: string; runsOn?: string }[] = data?.consumers ?? sample.consumers;
  const producerPlatform: Record<string, string> = data ? data.producerPlatform : sample.producerPlatform;

  const [picked, setRangeId] = useState<OverviewRangeId>("1h");
  /* a workspace's figures are counted on the server for the range in the URL */
  const rangeId = data ? ((RANGES.find((r) => r.id === data.range)?.id ?? "1h") as OverviewRangeId) : picked;
  const chooseRange = (id: OverviewRangeId) => (data ? router.push(hrefs.range(id)) : setRangeId(id));
  const preset = RANGES.find((r) => r.id === rangeId)!;
  const time = real ? timeProp : undefined;
  /* the range in a sentence: "the last hour", or the picker's "the past 7 days" and "this time range" */
  const range = time ? { id: preset.id, label: phrase(time.range), long: phrase(time.range) } : preset;
  const scale = sample.rangeScale[range.id] ?? { k: 1, c: 1 };
  const rate = (n: number) => (real ? n : Math.round(n * scale.k * 10) / 10);
  const count = (n: number) => (real ? n : Math.round(n * scale.c));

  /* what did not finish on each flow, by outcome */
  const failed = (name: string) => causes.filter((c) => c.queue === name).map((c) => ({ ...c, n: count(c.count) })).filter((c) => c.n > 0).sort((a, b) => ORDER.indexOf(a.outcome) - ORDER.indexOf(b.outcome) || b.n - a.n);
  /* a flow's not finished is the sum of its causes, so the row and its causes can't disagree */
  const flows = queues
    .map((q) => ({ q, bad: failed(q.name), n: failed(q.name).reduce((a, c) => a + c.n, 0) }))
    .sort((a, b) => b.n - a.n || b.q.sentPerMin - a.q.sentPerMin);
  const healthOf = (name: string) => (data ? data.info[name]?.health ?? null : queues.find((x) => x.name === name)?.health ?? null);
  const judged = queues.filter((q) => healthOf(q.name) !== null);
  const fromSpans = data ? queues.some((q) => data.info[q.name]?.healthBasis === "spans") : false;
  const troubled = queues.filter((q) => { const h = healthOf(q.name); return h !== null && h !== "good"; });

  const [name, setName] = useState(flows.find((f) => f.q.name === initialFlow)?.q.name ?? flows[0]?.q.name ?? "");
  const [part, setPart] = useState<Part>({ kind: "queue" });
  const pick = (n: string) => { setName(n); setPart({ kind: "queue" }); onFlow?.(n); };
  const seg = (active: boolean) => `rounded-md px-3 py-1 ${active ? "bg-sunken font-medium text-ink" : "text-ink-2 hover:text-ink"}`;

  /* a workspace nothing has reported to: one message, with its two ways forward. No empty flows
     table above it saying the same thing (QV-272). */
  if (flows.length === 0) {
    return (
      <>
        <TopBar title="Overview" pad="px-6" plain />
        <div className="px-6 pt-5 pb-10">
          <section
            aria-label="No data"
            className="rounded-card border border-line bg-canvas/60"
            style={{ backgroundImage: "radial-gradient(color-mix(in srgb, var(--color-ink-3) 26%, transparent) 1px, transparent 1px)", backgroundSize: "16px 16px", backgroundPosition: "8px 8px" }}
          >
            <div className="mx-auto flex min-h-[400px] max-w-lg flex-col items-center justify-center px-6 py-12 text-center">
              <Watching title="Waiting to observe your first message">
                No data yet. Nothing is reporting to {data?.workspace ?? "this workspace"}. This page draws each queue with the services that send to it and receive from it, once a service of yours is connected.
              </Watching>
              {slots.emptyActions && <div className="mt-5 flex flex-wrap justify-center gap-3">{slots.emptyActions}</div>}
            </div>
          </section>
        </div>
      </>
    );
  }

  const flow = flows.find((f) => f.q.name === name) ?? flows[0];
  const q: Queue = flow.q;
  const seenDlq = data?.dlq[q.name];
  const place = data?.places?.[q.name];
  // the queue's own page, at the Overview's range
  const queuePage = hrefs.queue({ name: q.name, range: rangeId, real });
  const info = data?.info[q.name];
  const lv = info?.live;
  const aws = lv?.status === "ok";
  const cfg: { visibilityTimeoutS: number | null; maxReceiveCount: number | null; dlq: string | null; retentionDays: number | null } =
    data ? { visibilityTimeoutS: lv?.visibilityTimeoutS ?? null, maxReceiveCount: lv?.maxReceiveCount ?? null, dlq: lv?.dlq ?? seenDlq?.name ?? null,
      retentionDays: lv?.retentionS ? Math.round((lv.retentionS / 86_400) * 10) / 10 : null } : sample.settings[q.name];
  /* the card's figures: the sample's, or SQS's own counts and CloudWatch's age of the oldest message */
  const st = data ? (aws ? { visible: lv!.visible, notVisible: lv!.inFlight, oldestS: lv!.oldestS === null ? null : Math.round(lv!.oldestS) } : undefined) : sample.stats[q.name];
  /* every consumer of the queue, each with its own receives */
  const found = consumers.filter((c) => c.queue === q.name);
  const conList = found.length ? found : [{ name: "No consumer reported", runtime: "", queue: q.name }];
  const con = (part.kind === "consumer" && conList.find((c) => c.name === part.name)) || conList[0];
  const cs = data ? undefined : sample.consumerStats[con.name];
  /* on own data: its deletes and processing errors in the range */
  const own = data?.consumerStats?.[q.name]?.[con.name];
  const p95 = cs ? cs.p95Ms : own?.p95Ms ?? null;
  const failPct = cs ? cs.failPct : own?.processed ? own.failPct : null;
  const fromCapped = data?.consumerStatsCapped ? " Read from the most recently active messages in the range, as the Event explorer reads them." : "";
  /* on own data: the function's timeouts and out-of-memory ends, from its log, when Trailbeam read it */
  const ends = data?.functionEnds?.[con.name];
  const ins = sends.filter((e) => e.to === q.name);
  const outOf = (name: string) => receives.find((e) => e.from === q.name && e.to === name) ?? { from: q.name, to: name, perMin: 0 };
  const out = outOf(con.name);
  const receivedPerMin = receives.filter((e) => e.from === q.name).reduce((a, e) => a + e.perMin, 0);
  /* moves to the dead-letter queue: on own data, those worked out from receive counts against maxReceiveCount
     (AWS logs no move). Receives on the dead-letter queue are receives, shown apart, never counted as moves */
  const dead = data ? (info?.moved ?? 0) : flow.bad.filter((c) => c.outcome === "dead_lettered").reduce((a, c) => a + c.n, 0);
  const dlqDepth = lv?.dlq ? lv.dlqDepth : null;
  const dlqBad = data ? dead > 0 || (info?.dlqChange ?? 0) > 0 : dead > 0;
  const dlqCount = dlqNodeCount({ real, queue: q.name, dlq: cfg.dlq ?? "", depth: dlqDepth, received: info?.dlqReceives ?? seenDlq?.received ?? 0, moved: dead, range: range.long });
  /* the Lambda triggers on the queue; the consumer's own, when its service name is its function's name */
  const triggers = info?.triggers ?? null;
  const mine = !!triggers?.some((t) => t.functionName === con.name);
  const conTone: Tone = failPct === null ? "ok" : failPct >= 5 ? "bad" : failPct >= 1 ? "warn" : "ok";
  const avg = data ? data.avgBytes[q.name] : sample.avgBytes[q.name];

  /* the drawing: nodes on each side sit level with the points their wires meet the card at */
  const PITCH = 76;
  const rights: Part[] = [...conList.map((c) => ({ kind: "consumer" as const, name: c.name })), ...(cfg.dlq ? [{ kind: "dlq" as const }] : [])];
  const H = Math.max(264, Math.max(ins.length, rights.length) * PITCH + 36);
  const y = (i: number, n: number) => H / 2 + (i - (n - 1) / 2) * PITCH;
  const port = (i: number, n: number) => H / 2 + (i - (n - 1) / 2) * 22;
  const is = (p: Part) => p.kind === part.kind && (p.kind === "producer" ? part.kind === "producer" && part.name === p.name : p.kind === "consumer" ? p.name === con.name : true);
  const node = (p: Part, tone: Tone = "ok") => ({
    type: "button" as const,
    "aria-pressed": is(p),
    onClick: () => setPart(p),
    className: `absolute inset-x-0 h-14 -translate-y-1/2 rounded-lg border bg-surface px-3 text-left transition-[border-color,box-shadow] duration-150 ${
      is(p) ? "border-accent shadow-[0_0_0_3px_color-mix(in_srgb,var(--color-accent)_18%,transparent)]" : tone === "bad" ? "border-critical/60 hover:border-critical" : "border-line-strong hover:border-ink-3"
    }`,
  });
  const face = (platform: string, role: string, name: string, color: string) => (
    <span className="flex h-full items-center gap-2.5">
      <Tooltip content={PLATFORM_LABEL[platform as keyof typeof PLATFORM_LABEL]} className="grid size-8 shrink-0 place-items-center rounded-md border border-line bg-canvas"><PlatformIcon platform={platform} size={18} color={color} /></Tooltip>
      <span className="min-w-0">
        <span className="block truncate text-xs text-ink-3"><Key color={color} />{role}</span>
        <Tooltip content={name} className="block text-sm font-medium text-ink"><MiddleTruncate text={name} /></Tooltip>
      </span>
    </span>
  );
  const flowRate = (perMin: number) => ({ n: fmtRate(rate(perMin)), bytes: avg === undefined ? undefined : fmtBytes(rate(perMin) * avg) });
  const Verdict = troubled.length ? CircleAlert : CircleCheck;

  return (
    <>
      <TopBar
        title="Overview"
        pad="px-6"
        plain={real && !fresh}
        search={real ? undefined : slots.search}
        fresh={fresh}
        actions={
          <>
            {real && slots.sampleSwitch}
            {real && slots.actions}
            {time
              ? <TimePicker time={time.range} setTime={time.onChange} now={time.now} />
              : <div role="group" aria-label="Time range" className="inline-flex rounded-lg border border-line bg-surface p-0.5 text-sm">
                  {RANGES.map((r) => <button key={r.id} aria-pressed={r.id === rangeId} onClick={() => chooseRange(r.id)} className={seg(r.id === rangeId)}>{r.label}</button>)}
                </div>}
          </>
        }
      />
      <div className="grid items-start gap-5 px-6 pt-5 pb-10 min-[1480px]:grid-cols-[22.5rem_minmax(0,1fr)]">
        {/* ── every flow, worst first ── */}
        <div className="grid gap-4">
          <section className={`${panel} overflow-hidden`} aria-label="Queues">
            <div className="flex items-center gap-2.5 px-4 pt-3.5 pb-3">
              {real && !judged.length && <span aria-hidden data-mark="neutral" className="size-2 shrink-0 rounded-[2px] bg-ink-3" />}
              {(!real || judged.length > 0) && <Verdict size={17} className={`shrink-0 ${troubled.length ? "text-critical" : "text-good"}`} aria-hidden />}
              <h2 className="text-sm font-medium text-ink">{real && !judged.length ? copy.noHealth(queues.length) : healthHeading(queues.length, judged.length, troubled.length)}{fromSpans && copy.spansOnly}</h2>
            </div>
            <div className="grid grid-cols-[minmax(0,1fr)_3.5rem_4.75rem_1rem] gap-x-3 border-y border-line bg-sunken/50 px-4 py-1.5 text-xs font-medium text-ink-3">
              <span>Queue</span><span className="text-right">Sent/min</span><span className="text-right">Not finished</span><span />
            </div>
            <ul className="divide-y divide-line">
              {flows.map(({ q: f, n }) => {
                const on = f.name === q.name;
                const worst = failed(f.name)[0]?.outcome;
                return (
                  <li key={f.name}>
                    <button type="button" aria-pressed={on} onClick={() => pick(f.name)} className={`grid w-full grid-cols-[minmax(0,1fr)_3.5rem_4.75rem_1rem] items-center gap-x-3 px-4 py-3 text-left ${on ? "bg-sunken" : "hover:bg-sunken/60"}`}>
                      <span className="min-w-0">
                        <span className="block truncate text-sm font-medium text-ink">{f.name}</span>
                        <span className="mt-0.5 block truncate text-xs text-ink-3">{[sends.filter((e) => e.to === f.name).map((e) => e.from).join(", ") || "No producer reported", consumers.find((c) => c.queue === f.name)?.name ?? "no consumer reported"].join(" to ")}</span>
                      </span>
                      <span className="text-right font-mono text-[13px] text-ink tabular-nums">{fmtRate(rate(f.sentPerMin))}</span>
                      <Tooltip content={n ? `Worst outcome: ${OUTCOME_LABEL[worst!]}` : undefined} className={`text-right font-mono text-[13px] tabular-nums ${n ? OUTCOME_TEXT[worst!] : "text-ink-3"}`}>{fmtNum(n)}</Tooltip>
                      <ChevronRight size={15} className={on ? "text-ink" : "text-ink-3"} aria-hidden />
                    </button>
                  </li>
                );
              })}
            </ul>
          </section>

          {/* ── why messages didn't finish on the selected flow: one row per cause ── */}
          <section aria-labelledby="causes-h" className={panel}>
            <div className="flex items-baseline gap-3 px-4 pt-3.5 pb-2">
              <h2 id="causes-h" className="min-w-0 text-sm font-medium text-ink [overflow-wrap:anywhere]">Why messages didn&apos;t finish on {q.name}</h2>
              <UiLink href={hrefs.issues(q.name)} className="ml-auto shrink-0 text-xs font-medium text-brand hover:underline">Issues on {q.name}</UiLink>
            </div>
            {flow.bad.length ? (
              <ul className="divide-y divide-line border-t border-line">
                {flow.bad.slice(0, 5).map((c) => (
                  <li key={`${c.outcome}:${c.title}`}>
                    <UiLink href={hrefs.cause(c)} className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-3 px-4 py-2.5 hover:bg-sunken/60">
                      <span className="min-w-0">
                        <span className="flex items-center gap-2">
                          <span className="size-2 shrink-0 rounded-full" style={{ background: OUTCOME_FILL[c.outcome] }} aria-label={OUTCOME_LABEL[c.outcome]} />
                          <Tooltip content={c.title} className="truncate text-[13px] text-ink">{c.title}</Tooltip>
                        </span>
                        <span className="mt-0.5 block truncate pl-4 text-xs text-ink-3">{[c.split, c.tenants, c.versions, `last ${c.last}`].filter(Boolean).join(". ")}{c.basis ? ` (${c.basis})` : ""}</span>
                      </span>
                      <span className={`font-mono text-[13px] tabular-nums ${OUTCOME_TEXT[c.outcome]}`}>{fmtNum(c.n)}</span>
                    </UiLink>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="border-t border-line px-4 py-3 text-xs text-ink-3">Every message first seen on {q.name} over 5 minutes ago has finished.</p>
            )}
            {flow.bad.length > 5 && <p className="border-t border-line px-4 py-2 text-xs text-ink-3">{flow.bad.length - 5} more {flow.bad.length - 5 === 1 ? "cause" : "causes"} on Issues.</p>}
            {data?.causesCapped && <p className="border-t border-line px-4 py-2 text-xs text-ink-3">At least 5,000 messages are unfinished. Causes are from the 5,000 most recently active.</p>}
          </section>


        </div>

        {/* ── the selected flow ── */}
        <div className="grid min-w-0 gap-4">
          <section
            aria-label={`Queue ${q.name}`}
            className="overflow-x-auto rounded-card border border-line bg-canvas/60"
            style={{ backgroundImage: "radial-gradient(color-mix(in srgb, var(--color-ink-3) 26%, transparent) 1px, transparent 1px)", backgroundSize: "16px 16px", backgroundPosition: "8px 8px" }}
          >
            <div className="min-w-[904px] px-6 pt-5">
              <h2 className="text-[15px] font-medium text-ink">Messages through {q.name}</h2>
              <p className="mt-0.5 text-sm text-ink-2">What sends to it, what receives from it, and where messages go when they fail. Select a part to see more.</p>
            </div>
            <div className="grid min-w-[904px] grid-cols-[minmax(160px,210px)_minmax(118px,1fr)_minmax(340px,440px)_minmax(118px,1fr)_minmax(160px,220px)] px-6 pt-2 pb-6">
              <div className="relative" style={{ height: H }}>
                {ins.length === 0 && <p className="absolute inset-x-0 top-1/2 -translate-y-1/2 text-sm leading-relaxed text-ink-3">No data. No service reported a send to this queue.</p>}
                {ins.map((e, i) => (
                  <button key={e.from} {...node({ kind: "producer", name: e.from })} style={{ top: y(i, ins.length) }}>
                    {face(producerPlatform[e.from] ?? (real ? "" : "EKS"), "Producer", e.from, SEND)}
                  </button>
                ))}
              </div>
              <Wires height={H} lines={ins.map((e, i) => ({ key: e.from, y1: y(i, ins.length), y2: port(i, ins.length), tone: "send" as Tone, rate: { ...flowRate(e.perMin), at: "start" as const } }))} />

              <div className="flex items-center" style={{ height: H }}>
                <div
                  role="button" tabIndex={0} aria-pressed={part.kind === "queue"} onClick={() => setPart({ kind: "queue" })}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setPart({ kind: "queue" }); } }}
                  className={`w-full cursor-pointer rounded-card border bg-surface shadow-raised transition-[border-color,box-shadow] duration-150 ${part.kind === "queue" ? "border-accent shadow-[0_0_0_3px_color-mix(in_srgb,var(--color-accent)_18%,transparent)]" : "border-line-strong hover:border-ink-3"}`}
                >
                  <div className="flex items-center gap-2.5 px-4 pt-3.5">
                    <PlatformIcon platform="SQS" size={18} />
                    <h3 className="min-w-0 truncate text-[15px] font-medium text-ink"><Tooltip content={q.name}><UiLink href={queuePage} onClick={(e) => e.stopPropagation()} className="hover:underline">{q.name}</UiLink></Tooltip></h3>
                    {place && (place.account || place.region) && <Tooltip content="Account and region, from the queue URL" className="shrink-0 font-mono text-micro text-ink-3">{[place.account, place.region].filter(Boolean).join(" / ")}</Tooltip>}
                    <span className="ml-auto shrink-0">{real ? (info?.health ? (info.healthBasis === "spans"
                      ? <span className="inline-flex items-center gap-1.5"><HealthPill health={info.health} label={info.why ?? undefined} /><Tooltip content={SPAN_HEALTH_NOTE} className="rounded border border-line px-1 text-micro text-ink-3">spans</Tooltip></span>
                      : <HealthPill health={info.health} label={info.why ?? undefined} />) : <span className="text-xs text-ink-3">No health data</span>) : <HealthPill health={q.health} />}</span>
                  </div>
                  <div className="mt-3.5 grid grid-cols-3 gap-x-4 px-4">
                    <div>
                      <p className="text-xs text-ink-3"><Key color={SEND} />Sent</p>
                      <p className="mt-0.5 font-mono text-[17px] font-medium text-ink tabular-nums">{fmtRate(rate(q.sentPerMin))}<span className="text-xs font-normal text-ink-3">/min</span></p>
                    </div>
                    <div>
                      <p className="text-xs text-ink-3"><Key color={RECEIVE} />Received</p>
                      <p className="mt-0.5 font-mono text-[17px] font-medium text-ink tabular-nums">{fmtRate(rate(receivedPerMin))}<span className="text-xs font-normal text-ink-3">/min</span></p>
                    </div>
                    <div>
                      <p className="text-xs text-ink-3">Not finished</p>
                      <p className={`mt-0.5 font-mono text-[17px] font-medium tabular-nums ${flow.n ? OUTCOME_TEXT[flow.bad[0].outcome] : "text-ink"}`}>{real
                        ? <Tooltip content="Messages from your services first seen over 5 minutes ago with no final delete, including those moved to the dead-letter queue: the sum of the causes listed for this queue" className="cursor-default">{fmtNum(flow.n)}</Tooltip>
                        : fmtNum(flow.n)}</p>
                    </div>
                  </div>
                  {st ? (
                  <dl className="mt-4 grid grid-cols-2 gap-x-5 gap-y-2 whitespace-nowrap border-t border-line px-4 py-3.5 text-xs">
                    {([["Visible", st.visible === null ? <NoData /> : fmtNum(st.visible)], ["In flight", st.notVisible === null ? <NoData /> : fmtNum(st.notVisible)],
                      ["Oldest message", st.oldestS === null ? (aws && lv!.oldestUnpublished ? <NotPublished what="this queue's ApproximateAgeOfOldestMessage" /> : <NoData />) : <>{st.oldestS < 60 ? `${st.oldestS} s` : `${Math.floor(st.oldestS / 60)}m ${st.oldestS % 60}s`}{aws && <AsOf ms={lv!.oldestAsOf} />}</>],
                      ["Visibility timeout", cfg.visibilityTimeoutS === null ? <NoData /> : `${cfg.visibilityTimeoutS} s`], ["maxReceiveCount", cfg.maxReceiveCount ?? "None"],
                      real ? ["Retention", cfg.retentionDays === null ? <NoData /> : `${cfg.retentionDays} ${cfg.retentionDays === 1 ? "day" : "days"}`] : ["Type", q.type]] as [string, ReactNode][]).map(([k, v]) => (
                      <div key={k} className="flex items-baseline justify-between gap-3">
                        <dt className="text-ink-3">{k}</dt>
                        <dd className="shrink-0 font-mono text-[12.5px] text-ink tabular-nums">{v}</dd>
                      </div>
                    ))}
                  </dl>
                  ) : (
                    <div className="mt-4 border-t border-line px-4 py-3.5 text-xs">
                      <p className="leading-relaxed text-ink-3">{copy.noFigures(lv)}</p>
                      <div className="mt-2 flex items-baseline justify-between gap-3">
                        <span className="text-ink-3">Type</span>
                        <span className="font-mono text-[12.5px] text-ink">{q.type}</span>
                      </div>
                    </div>
                  )}
                  {real && slots.queueFacts?.(q.name)}
                  <div className="border-t border-line px-4 py-2.5 text-right">
                    <UiLink href={queuePage} onClick={(e) => e.stopPropagation()} className="inline-flex items-center gap-1 text-xs font-medium text-brand hover:underline">Queue page</UiLink>
                  </div>
                </div>
              </div>

              <Wires height={H} lines={rights.map((k, i) => ({ key: k.kind === "consumer" ? `c:${k.name}` : k.kind, y1: port(i, rights.length), y2: y(i, rights.length), tone: k.kind === "dlq" ? (dlqBad ? "bad" : "ok") : conTone === "ok" ? "receive" : conTone, rate: k.kind === "consumer" ? { ...flowRate(outOf(k.name).perMin), at: "end" as const } : undefined }))} />
              <div className="relative" style={{ height: H }}>
                {rights.map((k, i) =>
                  k.kind === "consumer" ? (
                    <button key={`c:${k.name}`} {...node(k)} style={{ top: y(i, rights.length) }}>
                      {face(conList.find((c) => c.name === k.name)?.runtime ?? "", "Consumer", k.name, HANDLE)}
                    </button>
                  ) : (
                    <button key="dlq" {...node({ kind: "dlq" }, dlqBad ? "bad" : "ok")} style={{ top: y(i, rights.length) }}>
                      <span className="flex h-full items-center gap-2.5">
                        <span className="grid size-8 shrink-0 place-items-center rounded-md border border-line bg-canvas"><PlatformIcon platform="SQS" size={18} /></span>
                        <span className="min-w-0 flex-1">
                          <span className="block text-xs text-ink-3">DLQ</span>
                          <Tooltip content={cfg.dlq} className="block text-sm font-medium text-ink"><MiddleTruncate text={cfg.dlq ?? ""} /></Tooltip>
                        </span>
                        <Tooltip content={dlqCount.note} className={`shrink-0 font-mono text-[15px] font-medium tabular-nums ${dlqCount.n && dlqBad ? "text-critical-text" : "text-ink"}`}>
                          <span data-dlq-count="">{fmtNum(dlqCount.n)}</span>
                        </Tooltip>
                      </span>
                    </button>
                  ),
                )}
              </div>
            </div>
          </section>

          {/* ── more about the part that is selected ── */}
          {part.kind !== "queue" && <section className={`${panel} overflow-hidden`} aria-live="polite">
            {part.kind === "consumer" && (
              <div className="px-5 py-4">
                <div className="flex items-baseline gap-3">
                  <h2 className="text-sm font-medium text-ink">{con.name}, the consumer of {q.name}</h2>
                  {(!real || found.length > 0) && <UiLink href={hrefs.events(fieldQuery("consumer", con.name))} className="ml-auto inline-flex shrink-0 items-center gap-1 text-xs font-medium text-brand hover:underline">View its events</UiLink>}
                </div>
                <dl className="mt-4 grid grid-cols-2 gap-x-6 gap-y-4 sm:grid-cols-5">
                  <Fact k="Receives">{fmtRate(rate(out.perMin))}/min</Fact>
                  <Fact k="p95 processing time">{p95 !== null
                    ? <Tooltip content={own ? `From ${con.name}'s deletes and processing errors in the range: the handler's span for Lambda, and from the end of the receive to the start of the delete for other consumers.${fromCapped}` : undefined}>{fmtMs(p95)}</Tooltip>
                    : <NoData />}</Fact>
                  <Fact k="Processing failed">{failPct !== null
                    ? <Tooltip className={conTone === "bad" ? "text-critical-text" : conTone === "warn" ? "text-serious-text" : ""}
                        content={own ? `${fmtNum(own.failed)} processing ${own.failed === 1 ? "error" : "errors"} of ${fmtNum(own.processed)} ${own.processed === 1 ? "message" : "messages"} handled (deleted or failed) in the range.${fromCapped}` : undefined}>
                        {failPct}%{own && <span className="font-sans font-normal text-ink-3">, {fmtNum(own.failed)} of {fmtNum(own.processed)}</span>}
                      </Tooltip>
                    : <NoData />}</Fact>
                  <Fact k="Timed out">{cs ? fmtNum(count(cs.timeouts)) : ends ? <Tooltip content="From the function's own log: REPORT lines that say the invocation timed out or ran out of memory">{fmtNum(ends.timeouts)}{ends.oom > 0 && <span className="text-ink-3">, {fmtNum(ends.oom)} out of memory</span>}</Tooltip> : <NoData />}</Fact>
                  <Fact k="Runs on">{(con as { runsOn?: string }).runsOn || con.runtime || (mine ? "Lambda" : real ? <NoData /> : "Unknown")}</Fact>
                </dl>
                {real && triggers && triggers.length > 0 && (
                  <div className="mt-5">
                    <h3 className="text-xs font-medium text-ink-2">{triggers.length === 1 ? "The Lambda trigger" : `${triggers.length} Lambda triggers`} on {q.name}</h3>
                    <table className="mt-1.5 w-full border-collapse text-left text-[13px]">
                      <thead><tr className="text-xs text-ink-3">{["Function", "State", "Batch size", "Batching window", "Partial batch failures", "Maximum concurrency", "Event filters"].map((h) => <th key={h} className="border-b border-line py-1.5 pr-4 font-medium">{h}</th>)}</tr></thead>
                      <tbody className="font-mono text-caption text-ink tabular-nums">
                        {triggers.map((t) => (
                          <tr key={t.uuid ?? t.functionName} className={t.functionName === con.name ? "bg-sunken/60" : ""}>
                            <td className="border-b border-line py-1.5 pr-4">{t.functionName}</td>
                            <td className="border-b border-line py-1.5 pr-4 font-sans">{t.state ?? "Unknown"}</td>
                            <td className="border-b border-line py-1.5 pr-4">{t.batchSize ?? "Default"}</td>
                            <td className="border-b border-line py-1.5 pr-4">{t.batchingWindowS ? `${t.batchingWindowS} s` : "None"}</td>
                            <td className="border-b border-line py-1.5 pr-4 font-sans">{t.reportBatchItemFailures ? "Reported per message" : "Whole batch retried"}</td>
                            <td className="border-b border-line py-1.5 pr-4">{t.maxConcurrency ?? "Not set"}</td>
                            <td className="border-b border-line py-1.5 pr-4">{t.filters || "None"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    <p className="mt-2 text-xs text-ink-3">From lambda:ListEventSourceMappings on {q.name}.{!mine && " None of them is a function named " + con.name + ", so which one this consumer is can't be told from its name."}</p>
                  </div>
                )}
                {real && aws && triggers && triggers.length === 0 && <p className="mt-4 text-xs text-ink-3">No Lambda trigger reads from {q.name}: its consumers call ReceiveMessage themselves.</p>}
              </div>
            )}
            {/* the messages it received lately, one row each */}
            {part.kind === "consumer" && slots.partMessages?.({ queue: q.name, part: "consumer", consumer: found.length ? con.name : undefined, count: 0, perMin: out.perMin, max: cfg.maxReceiveCount, real })}

            {part.kind === "producer" && (
              <div className="px-5 py-4">
                <div className="flex items-baseline gap-3">
                  <h2 className="text-sm font-medium text-ink">{part.name}, a producer for {q.name}</h2>
                  <UiLink href={hrefs.events(fieldQuery("producer", part.name))} className="ml-auto inline-flex shrink-0 items-center gap-1 text-xs font-medium text-brand hover:underline">View its events</UiLink>
                </div>
                <p className="mt-1 text-xs text-ink-3">{real && !producerPlatform[part.name] ? "" : `${PLATFORM_LABEL[(producerPlatform[part.name] ?? "EKS") as keyof typeof PLATFORM_LABEL]}. `}It sends to {sends.filter((e) => e.from === part.name).length === 1 ? "this queue only" : "these queues"}.</p>
                <ul className="mt-3 divide-y divide-line border-y border-line">
                  {sends.filter((e) => e.from === part.name).map((e) => (
                    <li key={e.to}>
                      <button type="button" onClick={() => pick(e.to)} disabled={e.to === q.name} className="flex w-full items-center gap-4 py-2.5 text-left text-sm enabled:hover:bg-sunken">
                        <span className="min-w-0 flex-1 truncate font-medium text-ink">{e.to}</span>
                        <span className="font-mono text-[13px] text-ink-2 tabular-nums">{fmtRate(rate(e.perMin))}/min</span>
                        <span className="w-24 text-right text-xs text-ink-3">{e.to === q.name ? "Shown above" : "Show this queue"}</span>
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {/* the messages it sent to this queue lately, one row each */}
            {part.kind === "producer" && slots.partMessages?.({ queue: q.name, part: "producer", producer: part.name, count: 0, perMin: sends.find((e) => e.from === part.name && e.to === q.name)?.perMin ?? out.perMin, max: cfg.maxReceiveCount, real })}

            {part.kind === "dlq" && (
              <div className="px-5 py-4">
                <div className="flex items-baseline gap-3">
                  <h2 className="text-sm font-medium text-ink">{cfg.dlq}, the dead-letter queue of {q.name}</h2>
                  <UiLink href={hrefs.deadLettered({ queue: q.name, dlq: cfg.dlq, real })} className="ml-auto inline-flex shrink-0 items-center gap-1 text-xs font-medium text-brand hover:underline">View dead-lettered messages</UiLink>
                </div>
                {real ? (
                <dl className="mt-4 grid grid-cols-2 gap-x-6 gap-y-4 sm:grid-cols-4">
                  <Fact k="In the dead-letter queue now">{dlqDepth === null ? (aws && lv!.dlqUnpublished ? <NotPublished what={`${cfg.dlq}'s depth`} /> : <NoData />) : <><span className={dlqDepth ? "text-critical-text" : ""}>{fmtNum(dlqDepth)}</span><AsOf ms={lv?.dlqAsOf ?? null} /></>}</Fact>
                  <Fact k={`Change in ${range.long}`}>{info?.dlqChange === null || info?.dlqChange === undefined ? <NoData /> : <span className={info.dlqChange > 0 ? "text-critical-text" : ""}>{info.dlqChange > 0 ? "+" : ""}{fmtNum(info.dlqChange)}</span>}</Fact>
                  <Fact k={`Moved in ${range.long}`}><span className={dead ? "text-critical-text" : ""}>{fmtNum(dead)}</span></Fact>
                  <Fact k={`Received on it in ${range.long}`}>{fmtNum(info?.dlqReceives ?? seenDlq?.received ?? 0)}</Fact>
                  <Fact k="maxReceiveCount">{cfg.maxReceiveCount ?? <NoData />}</Fact>
                  <Fact k="Visibility timeout">{cfg.visibilityTimeoutS === null ? <NoData /> : `${cfg.visibilityTimeoutS} s`}</Fact>
                  <Fact k="Message retention">{cfg.retentionDays === null ? <NoData /> : `${cfg.retentionDays} ${cfg.retentionDays === 1 ? "day" : "days"}`}</Fact>
                </dl>
                ) : (
                <dl className="mt-4 grid grid-cols-2 gap-x-6 gap-y-4 sm:grid-cols-4">
                  <Fact k={`Dead-lettered in ${range.long}`}><span className={dead ? "text-critical-text" : ""}>{fmtNum(dead)}</span></Fact>
                  <Fact k="maxReceiveCount">{cfg.maxReceiveCount ?? <NoData />}</Fact>
                  <Fact k="Visibility timeout">{cfg.visibilityTimeoutS === null ? <NoData /> : `${cfg.visibilityTimeoutS} s`}</Fact>
                  <Fact k="Message retention">{cfg.retentionDays === null ? <NoData /> : `${cfg.retentionDays} ${cfg.retentionDays === 1 ? "day" : "days"}`}</Fact>
                </dl>
                )}
                <p className="mt-4 text-sm text-ink-2">
                  {real
                    ? <>
                        {lv?.dlq ? `${q.name}'s redrive policy names ${cfg.dlq}. Its depth is CloudWatch's ApproximateNumberOfMessagesVisible for ${cfg.dlq}.` : `Known because a receive on ${cfg.dlq} carried DeadLetterQueueSourceArn naming ${q.name}. ${copy.noFigures(lv)}`}
                        {" "}AWS does not log a move to a dead-letter queue. Moved counts the messages your services received {cfg.maxReceiveCount ?? "maxReceiveCount"} times without deleting them once the last visibility timeout ran out, since SQS moves such a message instead of delivering it again.
                        {(info?.dlqReceives ?? 0) > 0 && ` Received on it counts receives from ${cfg.dlq} by your services that SQS marked as coming from ${q.name}.`}
                      </>
                    : dead
                    ? `${flow.bad.filter((c) => c.outcome === "dead_lettered").map((c) => c.title).join(". ")}. SQS moves a message here on the receive after its receive count reaches maxReceiveCount.`
                    : `No message from ${q.name} reached its maxReceiveCount in ${range.long}.`}
                </p>
              </div>
            )}
            {/* the dead-lettered messages themselves, one row each */}
            {part.kind === "dlq" && dead > 0 && slots.partMessages?.({ queue: q.name, part: "dlq", count: dead, perMin: out.perMin, max: cfg.maxReceiveCount, real })}
          </section>}
        </div>

        {/* ── what is in the queue now: opens under the flow, across the page like the event explorer, when the queue itself is the selected part ── */}
        {part.kind === "queue" && (real ? (
          <div className="min-w-0 min-[1480px]:col-span-2">
            <QueueNowLive key={q.name} queue={q.name} view={info?.inQueue ?? null} live={lv ?? null} heldTotal={info?.heldTotal ?? 0} consumer={conList.length === 1 ? found[0]?.name ?? "" : ""} fifo={q.type === "FIFO"} triggers={info?.triggers ?? null} hrefs={hrefs} />
          </div>
        ) : slots.sampleNow ? <div className="min-w-0 min-[1480px]:col-span-2">{slots.sampleNow(q.name)}</div> : null)}
        {real && slots.foot && <footer className="min-w-0 text-xs leading-relaxed text-ink-3 min-[1480px]:col-span-2">{slots.foot}</footer>}
      </div>
    </>
  );
}
