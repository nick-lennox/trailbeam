"use client";

/*
  The queue page (QV-360 design, restyled on the design system in row 6 of the one-UI plan): is work arriving faster
  than it is finished, and where is the time going? One view for both products. It takes one QueuePageData
  (types.ts) and draws what it was given: `aws` null is self-hosted Trailbeam (no AWS calls), an `aws` with a status
  is the hosted app. Links come from `href` and go through the host's Nav (UiLink) unless the host passes `Link`;
  the time picker and the top bar's search are the host's (`picker`, `search`), and the URL state (sel=, split=) is
  the host's too: the view reports changes through onSelect/onSplit and the host writes them to the URL.

  The page is drawn the way the hosted pages are: the design system's TopBar, a breadcrumb, a serif heading, the
  figures in one grid, and each part in a panel with a 13px serif title. Values are in the mono face, labels and
  words never are. Hover notes are the design system's Tooltip, never `title`. Colored with role tokens, so it takes
  the theme it is drawn in. A client module, reached through @trailbeam/ui/design, never the main entry
  (test/server-import.test.ts).
*/
import { useState, type ComponentType, type ReactNode } from "react";
import { TopBar, type Fresh } from "../design/TopBar.tsx";
import { UiLink, type LinkProps } from "../design/nav.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { Badge } from "../design/Badge.tsx";
import { Chart, ChartSkeleton, Key, Tag, type ChartLine, type ChartMarker, type LegendItem } from "./Chart.tsx";
import {
  NO_VERDICT, SPAN_HEALTH_NOTE, collapseMarkers, consoleUrl, coverageText, fmtMs, fmtNum, fmtRate, fmtS, grid, onGrid, perMin,
  spansDays, totalsIn, utc,
} from "./model.ts";
import type {
  Bucket, CauseCounts, CauseRow, MessagesView, QueueConsumer as Consumer, QueueAwsView, QueueHrefs, QueuePageData, Tone, Trigger,
} from "./types.ts";

export type { MessagesView, QueueHrefs };

export type QueuePageProps = {
  /** the queue's name, shown while loading */
  name: string;
  /** null while loading */
  data: QueuePageData | null;
  /** the span query failed */
  error?: string | null;
  onRetry?: () => void;
  /** no span ever named this queue */
  unknown?: boolean;
  href: QueueHrefs;
  /** the host's time picker: the page's one range control */
  picker?: ReactNode;
  /** under the picker: "Kept for 7 days on this server", a clipped range */
  rangeNote?: ReactNode;
  /** the host's message search, drawn in the top bar */
  search?: ReactNode;
  /** the top bar's freshness mark (TopBar's); off by default */
  fresh?: Fresh;
  sel: [number, number] | null;
  onSelect: (sel: [number, number] | null) => void;
  split: boolean;
  onSplit: (on: boolean) => void;
  /** a link component; the host's Nav link (UiLink) by default */
  Link?: ComponentType<LinkProps>;
  /** why AWS wasn't read for this queue, in the host's words (self-hosted: the server's sentence and its AWS settings);
      the hosted sentences, with Connect its account, when unset */
  awsWhy?: (aws: QueueAwsView) => ReactNode;
  /** a host's own section, drawn right under the CloudWatch band (hosted: "In ‹queue› now"); nothing when unset */
  afterCloudWatch?: ReactNode;
  /** under the page's parts: the host's footnote (times are in UTC, the JSON link) */
  footer?: ReactNode;
};

const SEND = "var(--color-flow-send)";
const WAIT = "var(--color-flow-wait)";
const HANDLER = "var(--color-flow-handler)";
const LOST = "var(--color-flow-lost)";
const TONE: Record<Tone, string> = { good: "var(--color-good)", warning: "var(--color-warning)", critical: "var(--color-critical)" };

const link = "text-brand hover:underline";
const mono = "font-mono tabular-nums";
const prose = "max-w-[78ch] text-sm leading-relaxed text-ink-2";
/* prose inside a full-width panel: the rule spans the panel, the lines keep their measure */
const BAND = "text-sm leading-relaxed text-ink-2 [&>*]:max-w-[78ch]";

const AWS_REASON: Record<Exclude<QueueAwsView["status"], "ok">, (d: string | null) => string> = {
  no_account: () => "No connected AWS account names this queue.",
  not_configured: () => "This server has no AWS identity to read with.",
  failed: (d) => `AWS answered: ${d ?? "an error"}`,
};

/** "14:30 to 14:32", with dates when the window crosses midnight. */
const fromTo = (fromMs: number, toMs: number) => {
  const d = spansDays(fromMs, toMs);
  return `${utc(fromMs, { date: d })} to ${utc(toMs, { date: d })}`;
};

/* ── small parts ─────────────────────────────────────────────────────── */

/** A part of the page in the hosted panels' frame: a 13px serif title on a 36px header, then the body. */
function Panel({ title, right, children, className = "", id, label }: { title: ReactNode; right?: ReactNode; children: ReactNode; className?: string; id?: string; label?: string }) {
  return (
    <section id={id} aria-label={label} className={`flex scroll-mt-16 flex-col overflow-hidden rounded-card border border-line bg-surface shadow-card ${className}`}>
      <header className="flex min-h-9 shrink-0 flex-wrap items-center gap-x-3 gap-y-1 px-4 py-2">
        <h2 className="font-serif text-[13px] font-semibold text-ink">{title}</h2>
        {right && <span className="ml-auto flex flex-wrap items-center gap-3 text-xs text-ink-3">{right}</span>}
      </header>
      {children}
    </section>
  );
}

/** A heading over a group of panels, in the same face as the panels' titles. */
const GroupTitle = ({ children }: { children: ReactNode }) => <h2 className="font-serif text-[13px] font-semibold text-ink">{children}</h2>;

/** Words where a figure would be: "Not known", "No data". Sans, quieter than a value. */
const Words = ({ children }: { children: ReactNode }) => <span className="font-sans text-sm font-normal text-ink-3">{children}</span>;

type Cell = { label: string; value: ReactNode; note?: ReactNode; tip?: string; href?: string | null };

/** The figures someone reads first, in one grid of cells, as the hosted issue page draws them. */
function Figures({ cells, Link }: { cells: Cell[]; Link: ComponentType<LinkProps> }) {
  return (
    // each cell draws its right and bottom rule; the panel clips the outer ones, so a short last row leaves no gap
    <dl className="-mr-px -mb-px grid grid-cols-[repeat(auto-fill,minmax(9.5rem,1fr))] border-t border-line">
      {cells.map((c) => (
        <div key={c.label} className="border-r border-b border-line px-4 py-3">
          <dt className="text-xs text-ink-3">{c.label}</dt>
          <dd className={`mt-1 text-[17px] text-ink ${mono}`}>
            <Tooltip content={c.tip} tabIndex={c.tip ? 0 : undefined}>
              {c.href ? <Link href={c.href} className="hover:text-brand hover:underline">{c.value}</Link> : c.value}
            </Tooltip>
          </dd>
          {c.note && <dd className="mt-0.5 text-xs text-ink-3">{c.note}</dd>}
        </div>
      ))}
    </dl>
  );
}

/** The queue's health: a dot in its color and the reason, never color alone. */
function HealthMark({ tone, why, tip }: { tone: Tone; why: string; tip?: string }) {
  return (
    <Tooltip content={tip} tabIndex={tip ? 0 : undefined} className="inline-flex items-center gap-1.5 rounded-full border border-line bg-surface px-2.5 py-0.5 text-sm text-ink">
      <span aria-hidden className="size-2 rounded-full" style={{ background: TONE[tone] }} />{why}
    </Tooltip>
  );
}

/* ── the parts of the page ───────────────────────────────────────────── */

function Header({ p, d }: { p: QueuePageProps; d: QueuePageData | null }) {
  const Link = p.Link ?? UiLink;
  const q = d?.queue ?? { name: p.name, account: null, region: null, fifo: p.name.endsWith(".fifo"), key: p.name };
  const console = consoleUrl(q);
  const h = d?.health;
  const spanTip = h?.basis === "spans" ? SPAN_HEALTH_NOTE : undefined;
  return (
    <header>
      <nav aria-label="Breadcrumb" className="flex flex-wrap items-center gap-1 text-sm text-ink-3">
        <Link href={p.href.queues} className="hover:text-ink">Overview</Link>
        <span aria-hidden>/</span>
        <span className="font-mono text-caption text-ink-2">{q.name}</span>
      </nav>
      <div className="mt-3 flex flex-wrap items-start gap-4">
        <div className="min-w-0">
          <h1 className="break-all font-serif text-title-sm text-ink">{q.name}</h1>
          <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1.5 text-sm text-ink-3">
            {d && (h ? (
              <>
                <HealthMark tone={h.tone} why={h.why} tip={spanTip} />
                <Tooltip content={spanTip} tabIndex={spanTip ? 0 : undefined} className="text-xs">
                  {h.basis === "aws" ? "from SQS and CloudWatch" : "from your services' spans only"}
                </Tooltip>
              </>
            ) : <span className="text-xs">{d.noHealth ?? NO_VERDICT}</span>)}
            {d && <span>{q.fifo ? "FIFO queue" : "Standard queue"}</span>}
            {q.account && <span>Account <span className={`${mono} text-caption text-ink-2`}>{q.account}</span></span>}
            {q.region && <span>Region <span className={`${mono} text-caption text-ink-2`}>{q.region}</span></span>}
            {d?.sample && <Badge tone="brand">Sample data</Badge>}
            {console && <a className={link} href={console} target="_blank" rel="noreferrer">Open in the SQS console</a>}
          </div>
        </div>
        {(p.picker || p.rangeNote) && (
          <div className="ml-auto flex flex-col items-end gap-1">
            {p.picker}
            {p.rangeNote && <span className="text-xs text-ink-3">{p.rangeNote}</span>}
          </div>
        )}
      </div>
    </header>
  );
}

const NOT_KNOWN = "Trailbeam makes no AWS calls, so it doesn't read the queue's settings. maxReceiveCount is set in the server's config (max_receive_count).";

function SettingsPanel({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? UiLink;
  const aws = d.aws;
  const read = aws?.status === "ok" && aws.settings ? aws.settings : null;
  const dlqName = read?.dlqName ?? d.dlqSeen?.name ?? null;
  const tip = aws ? undefined : NOT_KNOWN;
  const cells: Cell[] = [
    {
      label: "Visibility timeout", tip,
      value: read ? (read.visibilityTimeoutS !== null ? fmtS(read.visibilityTimeoutS) : <Words>Not set</Words>) : <Words>{aws ? "Not read" : "Not known"}</Words>,
    },
    {
      label: "maxReceiveCount", tip,
      value: read ? (read.maxReceiveCount ?? <Words>None</Words>)
        : aws ? <Words>Not read</Words>
          : d.configured?.maxReceiveCount != null ? d.configured.maxReceiveCount
            : <Words>Not set on this server</Words>,
      note: !read && !aws && d.configured?.maxReceiveCount != null ? (d.configured.isDefault ? "server default" : "set on this server") : undefined,
    },
    {
      label: "Dead-letter queue",
      value: dlqName ? <Link href={p.href.queue(dlqName)} className="break-all text-caption hover:text-brand hover:underline">{dlqName}</Link>
        : <Words>{aws ? (read ? "None" : "Not read") : "None seen"}</Words>,
      note: dlqName && !read?.dlqName ? <Tag>seen on a move</Tag> : undefined,
    },
    ...(read?.retentionS != null ? [{ label: "Retention", value: fmtS(read.retentionS) }] : []),
    ...(read && (read.delayS ?? 0) > 0 ? [{ label: "Delivery delay", value: fmtS(read.delayS!) }] : []),
  ];
  const right = read && aws?.readAt ? <Tag>Read from SQS at {utc(aws.readAt, { seconds: true })} UTC</Tag>
    : aws && aws.status !== "ok" ? (p.awsWhy ? p.awsWhy(aws) : <>{AWS_REASON[aws.status](aws.detail)}{aws.status === "no_account" && p.href.connect && <Link href={p.href.connect} className={link}>Connect its account</Link>}</>)
      : null;
  return <Panel title="Settings" right={right}><Figures cells={cells} Link={Link} /></Panel>;
}

function NowPanel({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? UiLink;
  const { series, range } = d;
  const stepS = range.stepS;
  // sends and handled per minute over the last 5 minutes (the last bucket, when a bucket is longer)
  // complete buckets only: the open one would pull the rate down
  const done = (b: Bucket) => b.t + stepS * 1000 <= range.toMs;
  const recent = stepS >= 300
    ? series.filter((b) => done(b) && b.t + 2 * stepS * 1000 > range.toMs)
    : series.filter((b) => done(b) && b.t + stepS * 1000 > range.toMs - 5 * 60_000);
  const span = stepS >= 300 ? stepS : 300;
  const none = <Words>No data</Words>;
  const rate = (k: "sends" | "handled") => (series.length ? fmtRate(perMin(recent.reduce((n, b) => n + b[k], 0), span)) : none);
  const worst = (k: "p95QueueMs" | "p95HandlerMs") => {
    const vs = series.map((b) => b[k]).filter((v): v is number => v !== null);
    return vs.length ? fmtMs(Math.max(...vs)) : none;
  };
  const now = d.aws?.status === "ok" ? d.aws.now : null;
  const asOf = (t: number | null) => (t ? `as of ${utc(t)} UTC` : undefined);
  const np = <Words>Not in CloudWatch yet</Words>;
  const cells: Cell[] = [
    ...(now ? [
      { label: "Visible", value: now.visible === null ? np : fmtNum(now.visible) },
      { label: "In flight", value: now.inFlight === null ? np : fmtNum(now.inFlight) },
      { label: "Oldest message", value: now.oldestS === null ? np : fmtS(now.oldestS), note: asOf(now.oldestAt) },
      ...(d.aws?.settings?.dlqName ? [{
        label: "In the dead-letter queue",
        value: now.dlqDepth === null ? np : <>{fmtNum(now.dlqDepth)}{now.dlqChange ? <span className="text-ink-3"> ({now.dlqChange > 0 ? "+" : ""}{fmtNum(now.dlqChange)})</span> : null}</>,
        note: asOf(now.dlqAt),
      }] : []),
    ] : []),
    { label: "Sends per minute", value: rate("sends"), note: "last 5 minutes" },
    { label: "Handled per minute", value: rate("handled"), note: "last 5 minutes" },
    { label: "p95 in queue", value: worst("p95QueueMs"), note: "highest minute in the range" },
    { label: "p95 in handler", value: worst("p95HandlerMs"), note: "highest minute in the range" },
    ...(d.stuck != null ? [{ label: "Stuck", value: fmtNum(d.stuck), href: p.href.stuck(d.queue.name) }] : []),
  ];
  return <Panel title="Now" right={now ? "from CloudWatch and your services' spans" : "from your services' spans"}><Figures cells={cells} Link={Link} /></Panel>;
}

/** By-version lines for one measure: newest full strength, older at 45%, labelled at the right; dots under 20 samples. */
function versionLines(series: (Bucket | undefined)[], measure: "handled" | "p95QueueMs" | "p95HandlerMs", color: string, toValue: (v: number) => number): ChartLine[] {
  const first = new Map<string, number>();
  series.forEach((b, i) => b?.byVersion?.forEach((v) => { const k = v.version ?? ""; if (!first.has(k)) first.set(k, i); }));
  const order = [...first.keys()].sort((a, b) => first.get(a)! - first.get(b)!);
  const newest = order[order.length - 1];
  const samples = measure === "p95QueueMs" ? "queueSamples" : "handlerSamples";
  return order.map((k) => {
    const at = (b: Bucket | undefined) => b?.byVersion?.find((v) => (v.version ?? "") === k);
    return {
      key: `v:${k}`,
      label: k || "no version",
      color,
      endLabel: k || "no version",
      opacity: k === newest ? 1 : 0.45,
      values: series.map((b) => { const v = at(b)?.[measure]; return v === undefined || v === null ? null : toValue(v); }),
      dots: measure === "handled" ? undefined : series.map((b) => { const v = at(b); return !!v && (v[samples] ?? 20) < 20; }),
    };
  });
}

function SpanCharts({ d, p, hover, setHover }: { d: QueuePageData; p: QueuePageProps; hover: number | null; setHover: (i: number | null) => void }) {
  const { range } = d;
  const times = grid(range.fromMs, range.toMs, range.stepS);
  const by = new Map(d.series.map((b) => [b.t, b]));
  const at = times.map((t) => by.get(t));
  const rate = (k: "sends" | "receives" | "handled") => at.map((b) => (b ? perMin(b[k], range.stepS) : null));
  const secs = (k: "p95QueueMs" | "p95HandlerMs") => at.map((b) => (b && b[k] !== null ? b[k]! / 1000 : null));
  const rangeWords = `${fromTo(range.fromMs, range.toMs)} UTC`;
  const inProgress = range.endsNow;
  const fmtR = (n: number) => fmtRate(n);
  const fmtSec = (n: number) => (n === 0 ? "0" : fmtS(n));
  const vt = d.aws?.status === "ok" ? d.aws.settings?.visibilityTimeoutS ?? null : null;
  const rules = vt ? [{ value: vt, label: `visibility timeout ${fmtS(vt)}` }] : [];
  const hasVersions = d.series.some((b) => b.byVersion?.some((v) => v.version !== null));
  const split = p.split && hasVersions;
  const markers: ChartMarker[] = collapseMarkers(d.versions, range.fromMs, range.toMs).map((g) => ({
    t: g.t,
    label: g.items.length === 1 ? `${g.items[0].service} ${g.items[0].version}` : `${g.items.length} deploys`,
    title: g.items.map((m) => `${m.service} ${m.version} at ${utc(m.firstMs)} UTC`).join("\n"),
  }));
  const markerLegend: LegendItem[] = markers.length ? [{ label: "new version", color: "var(--color-ink-3)", shape: "marker" }] : [];
  const ruleLegend: LegendItem[] = rules.length ? [{ label: "visibility timeout", color: "var(--color-line-strong)", shape: "rule" }] : [];
  const common = { times, stepS: range.stepS, hover, onHover: setHover, sel: p.sel, onSelect: p.onSelect, inProgress, markers, summary: rangeWords };
  const raw = (i: number, ks: [string, "sends" | "receives" | "handled"][]) => {
    const b = at[i];
    if (!b) return [];
    return [`In this ${range.stepS >= 3600 ? `${range.stepS / 3600} h` : `${range.stepS / 60} min`}: ${ks.map(([l, k]) => `${fmtNum(b[k])} ${l}`).join(", ")}`];
  };

  const handled: ChartLine = { key: "handled", label: "handled", color: HANDLER, values: rate("handled") };
  const sendsLines: ChartLine[] = split
    ? [{ key: "sends", label: "sends", color: SEND, values: rate("sends") }, { ...handled, opacity: 0, quiet: true }, ...versionLines(at, "handled", HANDLER, (v) => perMin(v, range.stepS))]
    : [{ key: "sends", label: "sends", color: SEND, values: rate("sends") }, handled];
  const noSend = d.series.reduce((n, b) => n + b.noSendReceives, 0);
  const marks = at.flatMap((b, i) => [
    ...(b && b.dlqSeen > 0 ? [{ i, kind: "seen" as const, n: b.dlqSeen }] : []),
    ...(b && b.dlqInferred > 0 ? [{ i, kind: "inferred" as const, n: b.dlqInferred }] : []),
  ]);
  const queueLines: ChartLine[] = split ? versionLines(at, "p95QueueMs", WAIT, (v) => v / 1000) : [{ key: "p95q", label: "p95 in queue", color: WAIT, values: secs("p95QueueMs") }];
  const handlerLines: ChartLine[] = split ? versionLines(at, "p95HandlerMs", HANDLER, (v) => v / 1000) : [{ key: "p95h", label: "p95 in handler", color: HANDLER, values: secs("p95HandlerMs") }];
  const legendOf = (lines: ChartLine[]) => lines.filter((l) => !l.quiet && !l.key.startsWith("v:")).map((l) => ({ label: l.label, color: l.color }));
  const versionLegend = (color: string, word: string): LegendItem[] => (split ? [{ label: `${word}, one line per version (older fainter)`, color }] : []);

  return (
    <div className="grid gap-3 lg:grid-cols-2">
      <Chart {...common} title="Sends and handled per minute" source="spans" fmt={fmtR} lines={sendsLines}
        band={{ upper: "sends", lower: "handled", color: SEND }}
        legend={[...legendOf(sendsLines), ...versionLegend(HANDLER, "handled"), ...markerLegend]}
        caption="Sends above handled means work is arriving faster than it's finished."
        tip={(i) => raw(i, [["sends", "sends"], ["handled", "handled"]])} />
      <Chart {...common} title="Receives and handled per minute" source="spans" fmt={fmtR}
        lines={[{ key: "receives", label: "receives", color: WAIT, values: rate("receives") }, handled]} marks={marks}
        legend={[{ label: "receives", color: WAIT }, { label: "handled", color: HANDLER }, { label: "moved to the DLQ (seen)", color: LOST, shape: "diamond" }, { label: "reached maxReceiveCount (inferred)", color: LOST, shape: "diamond-dashed" }, ...markerLegend]}
        caption="The gap between the lines is receives that were never deleted."
        tip={(i) => {
          const b = at[i];
          return b ? [...raw(i, [["receives", "receives"], ["handled", "handled"]]), `Of which re-received: ${fmtNum(b.reReceives)}`, ...(b.dlqSeen ? [`Moved to the DLQ (seen): ${b.dlqSeen}`] : []), ...(b.dlqInferred ? [`Reached maxReceiveCount (inferred): ${b.dlqInferred}`] : [])] : [];
        }} />
      <Chart {...common} title="p95 time in queue (s)" source="spans" fmt={fmtSec} lines={queueLines} rules={rules}
        legend={[...legendOf(queueLines), ...versionLegend(WAIT, "p95 in queue"), ...ruleLegend, ...markerLegend]}
        caption={<>From send to first receive. Waits after a failed receive show as re-receives in the chart above.{noSend > 0 && <> {fmtNum(noSend)} {noSend === 1 ? "receive had no recorded send and isn’t" : "receives had no recorded send and aren’t"} counted.</>}</>} />
      <Chart {...common} title="p95 time in handler (s)" source="spans" fmt={fmtSec} lines={handlerLines} rules={rules}
        legend={[...legendOf(handlerLines), ...versionLegend(HANDLER, "p95 in handler"), ...ruleLegend, ...markerLegend]}
        caption="From receive to delete or processing error." />
    </div>
  );
}

function CloudWatchBand({ d, p, hover, setHover }: { d: QueuePageData; p: QueuePageProps; hover: number | null; setHover: (i: number | null) => void }) {
  const Link = p.Link ?? UiLink;
  const aws = d.aws;
  const console = consoleUrl(d.queue);
  const roleLink = p.href.connections && <> <Link href={p.href.connections} className={link}>Check the role in Connections</Link></>;
  if (!aws) {
    return (
      <Panel title="What Trailbeam can’t see here">
        <div className={`border-t border-line px-4 py-3 ${BAND}`}>
          <p>Trailbeam makes no AWS calls. It records what your services report, so it can&rsquo;t see:</p>
          <ul className="mt-1 list-disc pl-5">
            <li>how many messages are waiting or in flight;</li>
            <li>the oldest message&rsquo;s age;</li>
            <li>how many are in the dead-letter queue;</li>
            <li>messages sent or received by services without Trailbeam.</li>
          </ul>
          <p className="mt-2">The SQS console&rsquo;s Monitoring tab shows these.{console && <> <a className={link} href={console} target="_blank" rel="noreferrer">Open in the SQS console</a></>}</p>
        </div>
      </Panel>
    );
  }
  if (aws.status !== "ok") {
    return (
      <Panel title="From CloudWatch">
        <div className={`border-t border-line px-4 py-3 ${BAND}`}>
          <p><strong className="font-medium text-ink">Depth, in flight, the oldest message&rsquo;s age and the DLQ&rsquo;s depth come from CloudWatch.</strong> {p.awsWhy ? p.awsWhy(aws) : <>{AWS_REASON[aws.status](aws.detail)}{aws.status === "no_account" && p.href.connect && <> <Link href={p.href.connect} className={link}>Connect an AWS account</Link></>}</>}</p>
          <p className="mt-1">Until then, this page shows what your services reported.</p>
        </div>
      </Panel>
    );
  }
  const { range } = d;
  const times = grid(range.fromMs, range.toMs, range.stepS);
  const s = aws.series;
  const common = { times, stepS: range.stepS, hover, onHover: setHover, sel: p.sel, summary: `${fromTo(range.fromMs, range.toMs)} UTC` };
  const quiet = d.series.length === 0 ? " Your services reported nothing in this range." : "";
  const caption = `Each point is CloudWatch's minute. The newest is about a minute behind.${quiet}`;
  const retention = aws.settings?.retentionS ?? null;
  const spanSends = d.series.reduce((n, b) => n + b.sends, 0);
  const cover = s ? coverageText(s.sent.length ? s.sent.reduce((n, [, v]) => n + v, 0) : null, spanSends) : null;
  const fmtN = (n: number) => fmtNum(Math.round(n));
  const failed = (what: string) => aws.errors.find((e) => e.toLowerCase().includes(what));
  return (
    <section className="grid gap-3" aria-label="From CloudWatch">
      <GroupTitle>From CloudWatch, read with your role</GroupTitle>
      {!s ? (
        <p className={prose}>{aws.errors[0] ?? aws.detail ?? "CloudWatch returned nothing for this queue."}{roleLink}</p>
      ) : (
        <div className="grid gap-3 lg:grid-cols-3">
          <Chart {...common} title="Visible and in flight" source="CloudWatch" fmt={fmtN}
            lines={[{ key: "visible", label: "visible", color: WAIT, values: onGrid(s.visible, times, range.stepS) }, { key: "inflight", label: "in flight", color: HANDLER, values: onGrid(s.inFlight, times, range.stepS) }]}
            legend={[{ label: "visible", color: WAIT }, { label: "in flight", color: HANDLER }]} caption={caption} />
          <Chart {...common} title="Oldest message" source="CloudWatch" fmt={(n) => (n === 0 ? "0" : fmtS(n))}
            lines={[{ key: "oldest", label: "oldest", color: WAIT, values: onGrid(s.oldestS, times, range.stepS) }]}
            rules={retention ? [{ value: retention * 0.9, label: "90% of retention" }] : []}
            legend={[{ label: "age of the oldest message", color: WAIT }, ...(retention ? [{ label: "90% of retention", color: "var(--color-line-strong)", shape: "rule" as const }] : [])]} caption={caption} />
          {aws.settings?.dlqName ? (
            failed("dlq") ? <p className="text-sm text-ink-2">{failed("dlq")}</p> : (
              <Chart {...common} title="Dead-letter queue depth" source="CloudWatch" fmt={fmtN}
                lines={[{ key: "dlq", label: aws.settings.dlqName, color: LOST, values: onGrid(s.dlq, times, range.stepS) }]}
                legend={[{ label: `visible in ${aws.settings.dlqName}`, color: LOST }]} caption={caption} />
            )
          ) : (
            <p className="self-center text-sm text-ink-3">No dead-letter queue in this queue&rsquo;s redrive policy.</p>
          )}
        </div>
      )}
      {cover && <p className={prose}>{cover}</p>}
      {s && aws.errors.length > 0 && (
        <ul className="text-sm text-ink-3">{aws.errors.map((e) => <li key={e}>{e}{roleLink}</li>)}</ul>
      )}
    </section>
  );
}

function Spike({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? UiLink;
  if (!p.sel) return null;
  const [fromMs, toMs] = p.sel;
  const t = totalsIn(d.series, fromMs, toMs);
  const items: [MessagesView, string, number | null][] = [
    ["sent", "Sent", t.sends], ["waited", "Waited longest", null], ["slowest", "Slowest in handler", null],
    ["failed", "Failed or received again", t.failedOrRetried], ["dead_lettered", "Dead-lettered", t.dlq],
  ];
  const links = items.map(([view, label, n]) => ({ label, n, href: p.href.messages({ queue: d.queue.name, fromMs, toMs, view }) })).filter((x) => x.href);
  return (
    <Panel label="Messages behind the selected minutes"
      title={<>Messages from <span className={`${mono} font-normal`}>{fromTo(fromMs, toMs)}</span> UTC</>}
      right={<button type="button" onClick={() => p.onSelect(null)} className="rounded-md px-1.5 text-sm text-ink-3 hover:bg-sunken hover:text-ink">Clear the selection</button>}>
      <div className="border-t border-line px-4 py-3">
        <ul className="flex flex-wrap gap-2 text-sm">
          {links.map((x) => (
            <li key={x.label}>
              <Link href={x.href!} className="inline-flex items-center gap-1.5 rounded-control border border-line bg-surface px-3 py-1 text-ink shadow-card hover:border-line-strong">
                {x.label}{x.n !== null && <span className={`${mono} text-caption text-ink-2`}>{fmtNum(x.n)}</span>}
              </Link>
            </li>
          ))}
        </ul>
        <p className="mt-2 text-xs text-ink-3">Messages Trailbeam recorded in this window.</p>
      </div>
    </Panel>
  );
}

/* the design system's table cells, as the hosted tables draw them: sans labels, mono values only */
const TH = "border-b border-line px-3 py-2 text-left text-xs font-medium whitespace-nowrap text-ink-3";
const TD = "border-b border-line px-3 py-2.5 align-top";
const NUM = `${TD} ${mono} text-caption text-ink-2 text-right`;
const ROW = "[&:last-child>td]:border-b-0 hover:bg-sunken";

function triggerLine(t: Trigger) {
  return [
    t.batchSize !== null && `batch size ${t.batchSize}`,
    t.batchingWindowS !== null && `batching window ${t.batchingWindowS} s`,
    t.state,
    t.maxConcurrency !== null && `max concurrency ${t.maxConcurrency}`,
    t.reportBatchItemFailures && "reports batch item failures",
    t.filters > 0 && `${t.filters} filter${t.filters === 1 ? "" : "s"}`,
  ].filter(Boolean) as string[];
}

function Tables({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? UiLink;
  const [moreS, setMoreS] = useState(false);
  const [moreC, setMoreC] = useState(false);
  const mins = Math.max(1, (d.range.toMs - d.range.fromMs) / 60_000);
  const senders = [...d.senders].sort((a, b) => b.sends - a.sends);
  const consumers = [...d.consumers].sort((a, b) => b.receives - a.receives);
  const name = (svc: string) => {
    const h = p.href.service?.(svc, d.queue.name);
    return h ? <Link href={h} className="text-ink hover:text-brand hover:underline">{svc}</Link> : <span className="text-ink">{svc}</span>;
  };
  const none = <span className="font-sans text-ink-3">none</span>;
  const vers = (vs: string[]) => (vs.length ? <span className="font-mono text-caption text-ink-2">{vs.slice(-3).join(" ")}{vs.length > 3 ? " …" : ""}</span> : none);
  const when = (ms: number | null) => (ms ? <span className={`${mono} text-caption text-ink-2`}>{utc(ms, { seconds: true })}</span> : none);
  const more = (n: number, on: boolean, set: (b: boolean) => void) => (n > 8 && !on
    ? <div className="border-t border-line px-4 py-2"><button type="button" onClick={() => set(true)} className="text-sm text-brand hover:underline">Show {n - 8} more</button></div>
    : null);
  const enabled = (t: Trigger) => (t.state ?? "Enabled") === "Enabled";
  const trig = (t: Trigger) => {
    const ws = triggerLine(t);
    return (
      <p className="mt-0.5 text-xs text-ink-3">
        Trigger: {ws.map((w, i) => (
          <span key={w}><span className={w === t.state && !enabled(t) ? "text-critical-text" : undefined}>{w}</span>{i < ws.length - 1 && ", "}</span>
        ))}
      </p>
    );
  };
  // columns no row has a value for are left out: self-hosted Trailbeam counts only receives per consumer
  const cCols = {
    handled: consumers.some((c) => c.handled !== null), failed: consumers.some((c) => c.failed !== null), p95: consumers.some((c) => c.p95HandlerMs !== null),
    versions: consumers.some((c) => c.versions.length > 0), last: consumers.some((c) => c.lastMs !== null),
  };
  const sCols = { versions: senders.some((s) => s.versions.length > 0), last: senders.some((s) => s.lastMs !== null) };
  const cSpan = 2 + Object.values(cCols).filter(Boolean).length;
  const sSpan = 2 + Object.values(sCols).filter(Boolean).length;
  const cRow = (c: Consumer) => (
    <tr key={c.name} className={ROW}>
      <td className={`${TD} pl-4`}>
        {name(c.name)} <span className="text-xs text-ink-3">{c.kind === "lambda" ? "Lambda" : "service"}</span>
        {c.trigger && trig(c.trigger)}
      </td>
      <td className={NUM}>{fmtNum(c.receives)}</td>
      {cCols.handled && <td className={NUM}>{c.handled === null ? none : fmtNum(c.handled)}</td>}
      {cCols.failed && <td className={NUM}>{c.failed === null ? none : fmtNum(c.failed)}</td>}
      {cCols.p95 && <td className={NUM}>{c.p95HandlerMs === null ? none : fmtMs(c.p95HandlerMs)}</td>}
      {cCols.versions && <td className={TD}>{vers(c.versions)}</td>}
      {cCols.last && <td className={`${TD} pr-4 text-right`}>{when(c.lastMs)}</td>}
    </tr>
  );
  return (
    <div className="grid items-start gap-3 xl:grid-cols-[minmax(0,1fr)_minmax(0,1.6fr)]">
      <Panel title="Senders" id="senders">
        <div className="overflow-x-auto border-t border-line">
          <table className="w-full text-sm">
            <thead><tr>
              <th scope="col" className={`${TH} pl-4`}>Service</th><th scope="col" className={`${TH} text-right`}>Sends per minute</th>
              {sCols.versions && <th scope="col" className={TH}>Versions</th>}{sCols.last && <th scope="col" className={`${TH} pr-4 text-right`}>Last send</th>}
            </tr></thead>
            <tbody>
              {senders.length === 0 && <tr><td colSpan={sSpan} className={`${TD} border-b-0 pl-4 text-ink-3`}>No service reported a send on this queue in the range.</td></tr>}
              {(moreS ? senders : senders.slice(0, 8)).map((s) => (
                <tr key={s.service} className={ROW}>
                  <td className={`${TD} pl-4`}>{name(s.service)}</td>
                  <td className={NUM}>{fmtRate(s.sends / mins)}</td>
                  {sCols.versions && <td className={TD}>{vers(s.versions)}</td>}
                  {sCols.last && <td className={`${TD} pr-4 text-right`}>{when(s.lastMs)}</td>}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {more(senders.length, moreS, setMoreS)}
      </Panel>
      <Panel title="Consumers" id="consumers">
        <div className="overflow-x-auto border-t border-line">
          <table className={`w-full text-sm ${cSpan > 4 ? "min-w-[720px]" : ""}`}>
            <thead><tr>
              <th scope="col" className={`${TH} pl-4`}>Consumer</th><th scope="col" className={`${TH} text-right`}>Receives</th>
              {cCols.handled && <th scope="col" className={`${TH} text-right`}>Handled</th>}{cCols.failed && <th scope="col" className={`${TH} text-right`}>Failed</th>}
              {cCols.p95 && <th scope="col" className={`${TH} text-right`}>p95 in handler</th>}{cCols.versions && <th scope="col" className={TH}>Versions</th>}
              {cCols.last && <th scope="col" className={`${TH} pr-4 text-right`}>Last receive</th>}
            </tr></thead>
            <tbody>
              {consumers.length === 0 && !(d.silentTriggers ?? []).length && <tr><td colSpan={cSpan} className={`${TD} border-b-0 pl-4 text-ink-3`}>No service reported a receive on this queue in the range.</td></tr>}
              {(moreC ? consumers : consumers.slice(0, 8)).map(cRow)}
              {(d.silentTriggers ?? []).map((t) => (
                <tr key={`t:${t.functionName}`} className={`${ROW} [&>td]:border-dashed`}>
                  <td className={`${TD} pl-4`}>
                    <span className="text-ink">{t.functionName}</span> <span className="text-xs text-ink-3">Lambda</span> <Tag inferred>inferred</Tag>
                    {trig(t)}
                  </td>
                  <td colSpan={cSpan - 1} className={`${TD} text-sm text-ink-2`}>No spans from this function in the range</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {more(consumers.length, moreC, setMoreC)}
      </Panel>
    </div>
  );
}

const OUTCOME: Record<CauseRow["outcome"], { label: string; color: string; shape: "diamond" | "square" }> = {
  dead_lettered: { label: "Dead-lettered", color: LOST, shape: "diamond" },
  stuck: { label: "Stuck", color: WAIT, shape: "square" },
  filtered: { label: "Filtered", color: "var(--color-ink-3)", shape: "square" },
};
const RANK = ["dead_lettered", "stuck", "filtered"];
const messagesWord = (n: number) => (n === 1 ? "message" : "messages");

function Causes({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? UiLink;
  const rows = Array.isArray(d.causes) ? [...d.causes].sort((a, b) => RANK.indexOf(a.outcome) - RANK.indexOf(b.outcome)) : null;
  const counts = Array.isArray(d.causes) ? null : (d.causes as CauseCounts);
  const none = rows ? rows.length === 0 : counts!.stuck === 0 && counts!.dlq === 0;
  const dlqHref = p.href.messages({ queue: d.queue.name, fromMs: d.range.fromMs, toMs: d.range.toMs, view: "dead_lettered" });
  const LI = "border-t border-line";
  return (
    <Panel title="Why messages here didn’t finish"
      right={<Link href={p.href.stuck(d.queue.name)} className={`text-sm ${link}`}>See the stuck messages on this queue</Link>}>
      {none ? <p className="border-t border-line px-4 py-3 text-sm text-ink-3">Nothing unfinished on this queue in the range.</p> : (
        <ul>
          {rows?.map((r) => (
            <li key={r.id} className={LI}>
              <Link href={r.href} className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-baseline gap-3 px-4 py-2.5 text-sm hover:bg-sunken">
                <span className="inline-flex items-center gap-1.5 text-ink-2"><Key item={{ label: "", color: OUTCOME[r.outcome].color, shape: OUTCOME[r.outcome].shape }} />{OUTCOME[r.outcome].label}</span>
                <span className="min-w-0 truncate text-ink">{r.signature}{r.consumer && <span className="text-ink-3"> in {r.consumer}</span>}{r.versions?.length ? <span className="text-xs text-ink-3">, seen in {r.versions.join(", ")}</span> : null}</span>
                <span className="whitespace-nowrap text-xs text-ink-3"><span className={`${mono} text-caption text-ink-2`}>{fmtNum(r.messages)}</span> {messagesWord(r.messages)}, last at <span className={`${mono} text-caption text-ink-2`}>{utc(r.lastMs)}</span> UTC</span>
              </Link>
            </li>
          ))}
          {counts && counts.stuck > 0 && (
            <li className={`${LI} flex items-baseline gap-3 px-4 py-2.5 text-sm`}>
              <Key item={{ label: "", color: WAIT }} />
              <Link href={p.href.stuck(d.queue.name)} className={link}><span className={mono}>{fmtNum(counts.stuck)}{counts.stuckMore ? "+" : ""}</span> stuck {messagesWord(counts.stuck)}</Link>
            </li>
          )}
          {counts && counts.dlq > 0 && (
            <li className={`${LI} flex items-baseline gap-3 px-4 py-2.5 text-sm`}>
              <Key item={{ label: "", color: LOST, shape: "diamond" }} />
              {dlqHref ? <Link href={dlqHref} className={link}><span className={mono}>{fmtNum(counts.dlq)}</span> moved to the DLQ</Link>
                : <span className="text-ink-2"><span className={mono}>{fmtNum(counts.dlq)}</span> moved to the DLQ in the range</span>}
            </li>
          )}
        </ul>
      )}
    </Panel>
  );
}

const SPAN_TITLES = ["Sends and handled per minute", "Receives and handled per minute", "p95 time in queue (s)", "p95 time in handler (s)"];

/** The page's frame: the design system's top bar with the host's search, and the page's padding. */
function Frame({ p, children }: { p: QueuePageProps; children: ReactNode }) {
  return (
    <>
      <TopBar search={p.search} fresh={p.fresh} />
      <div className="grid gap-5 px-8 pt-5 pb-12">
        {children}
        {p.footer && <div className="text-xs leading-relaxed text-ink-3">{p.footer}</div>}
      </div>
    </>
  );
}

/** The queue page. */
export function QueuePage(p: QueuePageProps) {
  const [hover, setHover] = useState<number | null>(null);
  const Link = p.Link ?? UiLink;
  const d = p.data;
  if (p.unknown) {
    return (
      <Frame p={p}>
        <Header p={p} d={null} />
        <div className={`rounded-card border border-line bg-surface px-4 py-3 shadow-card ${BAND}`}>
          <p>No service has reported a call on <span className={`${mono} text-caption text-ink`}>{p.name}</span>. A queue appears when a service reports a send or receive on it.</p>
          <p className="mt-2"><Link href={p.href.queues} className={link}>See every queue on the Overview</Link></p>
        </div>
      </Frame>
    );
  }
  const hasVersions = !!d?.series.some((b) => b.byVersion?.some((v) => v.version !== null));
  const empty = d && d.series.length === 0;
  return (
    <Frame p={p}>
      <Header p={p} d={d} />
      {d && <div className="grid gap-3 xl:grid-cols-[minmax(0,2fr)_minmax(0,3fr)]"><SettingsPanel d={d} p={p} /><NowPanel d={d} p={p} /></div>}
      <section className="grid gap-3" aria-label="From your services' spans">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <GroupTitle>From your services&rsquo; spans</GroupTitle>
          {hasVersions && (
            <label className="inline-flex items-center gap-2 text-sm text-ink-2">
              <input type="checkbox" className="accent-brand" checked={p.split} onChange={(e) => p.onSplit(e.target.checked)} /> By version
            </label>
          )}
        </div>
        {p.error ? (
          <p className={prose} role="alert">Couldn&rsquo;t load this queue&rsquo;s spans: {p.error}{p.onRetry && <> <button type="button" onClick={p.onRetry} className={link}>Try again</button></>}</p>
        ) : !d ? (
          <div className="grid gap-3 lg:grid-cols-2">{SPAN_TITLES.map((t) => <ChartSkeleton key={t} title={t} />)}</div>
        ) : empty ? (
          <p className={`rounded-card border border-line bg-surface px-4 py-3 shadow-card ${BAND}`}>
            No send or receive on <span className={`${mono} text-caption text-ink`}>{d.queue.name}</span> from {utc(d.range.fromMs, { date: true })} to {utc(d.range.toMs, { date: true })} UTC.
            {" "}Try the <Link href={p.href.range("1d")} className={link}>past 24 hours</Link> or the <Link href={p.href.range("7d")} className={link}>past 7 days</Link>.
          </p>
        ) : (
          <SpanCharts d={d} p={p} hover={hover} setHover={setHover} />
        )}
      </section>
      {d && <Spike d={d} p={p} />}
      {d && <CloudWatchBand d={d} p={p} hover={hover} setHover={setHover} />}
      {p.afterCloudWatch}
      {d && <Tables d={d} p={p} />}
      {d && <Causes d={d} p={p} />}
    </Frame>
  );
}
