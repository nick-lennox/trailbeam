/*
  The queue page (QV-360 design): is work arriving faster than it is finished, and where is the time going?
  One view for both products. It takes one QueuePageData (types.ts) and draws what it was given: `aws` null is
  self-hosted Trailbeam (no AWS calls), an `aws` with a status is the hosted app. Links come from `href`, the time
  picker is the host's (`picker`), and the URL state (sel=, split=) is the host's too: the view reports changes
  through onSelect/onSplit and the host writes them to the URL. Role tokens only (theme.css), so it takes either
  host's theme.
*/
import { useState, type ComponentType, type ReactNode } from "react";
import { Chart, ChartSkeleton, Key, Tag, type ChartLine, type ChartMarker, type LegendItem } from "./Chart.tsx";
import {
  NO_VERDICT, SPAN_HEALTH_NOTE, chartSummary, collapseMarkers, consoleUrl, coverageText, fmtMs, fmtNum, fmtRate, fmtS, grid, onGrid, perMin,
  totalsIn, utc, windowText, type QueuePreset,
} from "./model.ts";
import type { Bucket, CauseCounts, CauseRow, QueueConsumer as Consumer, QueueAwsView, QueuePageData, Tone, Trigger } from "./types.ts";

type LinkProps = { href: string; className?: string; children: ReactNode; title?: string };
const A = ({ href, className, children, title }: LinkProps) => <a href={href} className={className} title={title}>{children}</a>;

export type MessagesView = "sent" | "waited" | "slowest" | "failed" | "dead_lettered";

export type QueueHrefs = {
  /** the queue list: the hosted Overview, self-hosted /queues */
  queues: string;
  /** another queue's page (the DLQ's) */
  queue: (name: string) => string;
  /** messages Trailbeam recorded on the queue in [fromMs, toMs), for one of §6's links; null drops that link */
  messages: (o: { queue: string; fromMs: number; toMs: number; view: MessagesView }) => string | null;
  /** Needs attention / Stuck, for this queue */
  stuck: (queue: string) => string;
  /** messages filtered to a service on this queue */
  service?: (service: string, queue: string) => string | null;
  /** this page at another preset (the empty state's "Past 24 hours", "Past 7 days") */
  range: (preset: QueuePreset) => string;
  /** connect an AWS account (hosted, no_account) */
  connect?: string | null;
  /** the role's settings (hosted, a read failed) */
  connections?: string | null;
};

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
  /** the host's time picker */
  picker?: ReactNode;
  /** under the picker: "Kept for 7 days on this server", a clipped range */
  rangeNote?: ReactNode;
  sel: [number, number] | null;
  onSelect: (sel: [number, number] | null) => void;
  split: boolean;
  onSplit: (on: boolean) => void;
  Link?: ComponentType<LinkProps>;
};

const SEND = "var(--color-flow-send)";
const WAIT = "var(--color-flow-wait)";
const HANDLER = "var(--color-flow-handler)";
const LOST = "var(--color-flow-lost)";
const TONE: Record<Tone, string> = { good: "var(--color-good)", warning: "var(--color-warning)", critical: "var(--color-critical)" };

const link = "text-brand hover:underline";
const eyebrow = "font-mono text-caption uppercase tracking-[0.08em] text-ink-3";
const mono = "font-mono tabular-nums";

const AWS_REASON: Record<Exclude<QueueAwsView["status"], "ok">, (d: string | null) => string> = {
  no_account: () => "No connected AWS account names this queue.",
  not_configured: () => "This server has no AWS identity to read with.",
  failed: (d) => `AWS answered: ${d ?? "an error"}`,
};

/* ── small parts ─────────────────────────────────────────────────────── */

function Pill({ tone, why, title }: { tone: Tone; why: string; title?: string }) {
  return (
    <span title={title} className="inline-flex items-center gap-1.5 rounded-full border border-line px-2.5 py-0.5 text-sm text-ink">
      <span aria-hidden className="h-2 w-2 rounded-full" style={{ background: TONE[tone] }} />{why}
    </span>
  );
}

const Figure = ({ label, value, note, href, Link }: { label: string; value: ReactNode; note?: ReactNode; href?: string | null; Link: ComponentType<LinkProps> }) => (
  <div className="flex min-w-0 flex-col">
    <span className="text-caption text-ink-3">{label}</span>
    <span className={`${mono} text-[1.05rem] text-ink`}>{href ? <Link href={href} className="hover:underline">{value}</Link> : value}</span>
    {note && <span className="text-micro text-ink-3">{note}</span>}
  </div>
);

const Setting = ({ label, children, title }: { label: string; children: ReactNode; title?: string }) => (
  <span className="inline-flex items-baseline gap-1.5" title={title}>
    <span className="text-ink-3">{label}</span> <span className={`${mono} text-ink-2`}>{children}</span>
  </span>
);

/* ── the parts of the page ───────────────────────────────────────────── */

function Header({ p, d }: { p: QueuePageProps; d: QueuePageData | null }) {
  const Link = p.Link ?? A;
  const q = d?.queue ?? { name: p.name, account: null, region: null, fifo: p.name.endsWith(".fifo"), key: p.name };
  const console = consoleUrl(q);
  const h = d?.health;
  return (
    <header>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <nav aria-label="Breadcrumb" className="text-sm text-ink-3">
          <Link href={p.href.queues} className="hover:text-ink">Queues</Link> <span aria-hidden>/</span> <span className="font-mono text-ink-2">{q.name}</span>
        </nav>
        <div className="flex flex-col items-end gap-1">
          {p.picker}
          {p.rangeNote && <span className="text-caption text-ink-3">{p.rangeNote}</span>}
        </div>
      </div>
      <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-2">
        <h1 className="break-all font-mono text-[1.875rem] leading-tight text-ink">{q.name}</h1>
        {q.fifo && <Tag>fifo</Tag>}
        {d && (h ? (
          <span className="inline-flex items-center gap-2">
            <Pill tone={h.tone} why={h.why} title={h.basis === "spans" ? SPAN_HEALTH_NOTE : undefined} />
            <span className="text-caption text-ink-3" title={h.basis === "spans" ? SPAN_HEALTH_NOTE : undefined}>
              {h.basis === "aws" ? "from SQS and CloudWatch" : "from your services' spans only"}
            </span>
          </span>
        ) : <span className="text-caption text-ink-3">{d.noHealth ?? NO_VERDICT}</span>)}
      </div>
      {d?.sample && <p className="mt-1 text-caption text-ink-3">Sample data</p>}
      <p className="mt-1 flex flex-wrap gap-x-3 text-caption text-ink-3">
        {(q.account || q.region) && <span className="font-mono">{[q.account, q.region, q.fifo ? "FIFO" : "Standard"].filter(Boolean).join(" · ")}</span>}
        {console && <a className={link} href={console} target="_blank" rel="noreferrer">Open in the SQS console ↗</a>}
      </p>
    </header>
  );
}

function SettingsRow({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? A;
  const aws = d.aws;
  const read = aws?.status === "ok" && aws.settings ? aws.settings : null;
  const dlqName = read?.dlqName ?? d.dlqSeen?.name ?? null;
  const dlq = dlqName
    ? <><Link href={p.href.queue(dlqName)} className="hover:underline">{dlqName} →</Link>{!read?.dlqName && <> <Tag>seen on a move</Tag></>}</>
    : aws ? (read ? "None" : "Not read") : "None seen";
  const NOT_KNOWN = "Trailbeam makes no AWS calls, so it doesn't read the queue's settings. maxReceiveCount is set in the server's config (max_receive_count).";
  return (
    <div className="flex flex-wrap items-baseline gap-x-5 gap-y-1 border-b border-line py-3 text-sm">
      <Setting label="Visibility timeout" title={aws ? undefined : NOT_KNOWN}>
        {read ? (read.visibilityTimeoutS !== null ? fmtS(read.visibilityTimeoutS) : "Not set") : aws ? "Not read" : "Not known"}
      </Setting>
      <Setting label="maxReceiveCount" title={aws ? undefined : NOT_KNOWN}>
        {read ? (read.maxReceiveCount ?? "None")
          : aws ? "Not read"
            : d.configured?.maxReceiveCount != null
              ? <>{d.configured.maxReceiveCount} <span className="font-serif text-ink-3">{d.configured.isDefault ? "server default" : "set on this server"}</span></>
              : "Not set on this server"}
      </Setting>
      <Setting label="DLQ">{dlq}</Setting>
      {read?.retentionS != null && <Setting label="Retention">{fmtS(read.retentionS)}</Setting>}
      {read && (read.delayS ?? 0) > 0 && <Setting label="Delay">{fmtS(read.delayS!)}</Setting>}
      <span className="ml-auto text-caption text-ink-3">
        {read && aws?.readAt ? <Tag>SQS · read {utc(aws.readAt, { seconds: true })}</Tag>
          : aws && aws.status !== "ok" ? <>{AWS_REASON[aws.status](aws.detail)}{aws.status === "no_account" && p.href.connect && <> <Link href={p.href.connect} className={link}>Connect its account →</Link></>}</>
            : null}
      </span>
    </div>
  );
}

function NowRow({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? A;
  const { series, range } = d;
  const stepS = range.stepS;
  // sends and handled per minute over the last 5 minutes (the last bucket, when a bucket is longer)
  // complete buckets only: the open one would pull the rate down
  const done = (b: Bucket) => b.t + stepS * 1000 <= range.toMs;
  const recent = stepS >= 300
    ? series.filter((b) => done(b) && b.t + 2 * stepS * 1000 > range.toMs)
    : series.filter((b) => done(b) && b.t + stepS * 1000 > range.toMs - 5 * 60_000);
  const span = stepS >= 300 ? stepS : 300;
  const rate = (k: "sends" | "handled") => (series.length ? fmtRate(perMin(recent.reduce((n, b) => n + b[k], 0), span)) : "-");
  const worst = (k: "p95QueueMs" | "p95HandlerMs") => {
    const vs = series.map((b) => b[k]).filter((v): v is number => v !== null);
    return vs.length ? fmtMs(Math.max(...vs)) : "-";
  };
  const now = d.aws?.status === "ok" ? d.aws.now : null;
  const asOf = (t: number | null) => (t ? `as of ${utc(t)}` : undefined);
  const NP = "Not in CloudWatch yet";
  return (
    <div className="flex flex-wrap gap-x-8 gap-y-3 border-b border-line py-4">
      <span className={`${eyebrow} self-center`}>Now</span>
      {now && (
        <div className="flex flex-wrap gap-x-6 gap-y-3 border-r border-line pr-8">
          <Figure Link={Link} label="Visible" value={now.visible === null ? NP : fmtNum(now.visible)} />
          <Figure Link={Link} label="In flight" value={now.inFlight === null ? NP : fmtNum(now.inFlight)} />
          <Figure Link={Link} label="Oldest" value={now.oldestS === null ? NP : fmtS(now.oldestS)} note={asOf(now.oldestAt)} />
          {d.aws?.settings?.dlqName && (
            <Figure Link={Link} label="DLQ" value={now.dlqDepth === null ? NP : <>{fmtNum(now.dlqDepth)}{now.dlqChange ? <span className="text-ink-3"> ({now.dlqChange > 0 ? "+" : ""}{fmtNum(now.dlqChange)})</span> : null}</>} note={asOf(now.dlqAt)} />
          )}
        </div>
      )}
      <div className="flex flex-wrap gap-x-6 gap-y-3">
        <Figure Link={Link} label="Sends/min" value={rate("sends")} note="last 5 min" />
        <Figure Link={Link} label="Handled/min" value={rate("handled")} note="last 5 min" />
        <Figure Link={Link} label="p95 in queue" value={worst("p95QueueMs")} note="highest minute" />
        <Figure Link={Link} label="p95 in handler" value={worst("p95HandlerMs")} note="highest minute" />
        {d.stuck != null && <Figure Link={Link} label="Stuck" value={fmtNum(d.stuck)} href={p.href.stuck(d.queue.name)} />}
      </div>
    </div>
  );
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
  const rangeWords = `${windowText(range.fromMs, range.toMs)} UTC`;
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
    <div className="grid gap-4 md:grid-cols-2">
      <Chart {...common} title="Sends vs handled (/min)" source="spans" fmt={fmtR} lines={sendsLines}
        band={{ upper: "sends", lower: "handled", color: SEND }}
        legend={[...legendOf(sendsLines), ...versionLegend(HANDLER, "handled"), ...markerLegend]}
        caption="Sends above handled means work is arriving faster than it's finished."
        tip={(i) => raw(i, [["sends", "sends"], ["handled", "handled"]])}
        summary={rangeWords} />
      <Chart {...common} title="Receives vs handled (/min)" source="spans" fmt={fmtR}
        lines={[{ key: "receives", label: "receives", color: WAIT, values: rate("receives") }, handled]} marks={marks}
        legend={[{ label: "receives", color: WAIT }, { label: "handled", color: HANDLER }, { label: "moved to DLQ (seen)", color: LOST, shape: "diamond" }, { label: "reached maxReceiveCount (inferred)", color: LOST, shape: "diamond-dashed" }, ...markerLegend]}
        caption="The gap between the lines is failed or abandoned attempts."
        tip={(i) => {
          const b = at[i];
          return b ? [...raw(i, [["receives", "receives"], ["handled", "handled"]]), `of which re-received: ${fmtNum(b.reReceives)}`, ...(b.dlqSeen ? [`moved to DLQ (seen): ${b.dlqSeen}`] : []), ...(b.dlqInferred ? [`reached maxReceiveCount (inferred): ${b.dlqInferred}`] : [])] : [];
        }} />
      <Chart {...common} title="p95 time in queue (s)" source="spans" fmt={fmtSec} lines={queueLines} rules={rules}
        legend={[...legendOf(queueLines), ...versionLegend(WAIT, "p95 in queue"), ...ruleLegend, ...markerLegend]}
        caption={<>From send to first receive. Waits after a failed attempt show as re-receives in the chart above.{noSend > 0 && <> {fmtNum(noSend)} {noSend === 1 ? "receive had no recorded send and isn’t" : "receives had no recorded send and aren’t"} counted.</>}</>} />
      <Chart {...common} title="p95 time in handler (s)" source="spans" fmt={fmtSec} lines={handlerLines} rules={rules}
        legend={[...legendOf(handlerLines), ...versionLegend(HANDLER, "p95 in handler"), ...ruleLegend, ...markerLegend]}
        caption="From receive to delete or processing error." />
    </div>
  );
}

function CloudWatchBand({ d, p, hover, setHover }: { d: QueuePageData; p: QueuePageProps; hover: number | null; setHover: (i: number | null) => void }) {
  const Link = p.Link ?? A;
  const aws = d.aws;
  const console = consoleUrl(d.queue);
  if (!aws) {
    return (
      <section className="border-t border-line py-5">
        <h2 className={eyebrow}>What Trailbeam can&rsquo;t see here</h2>
        <div className="mt-2 max-w-[72ch] text-[15px] leading-[1.55] text-ink-2">
          <p>Trailbeam makes no AWS calls. It records what your services report, so it can&rsquo;t see:</p>
          <ul className="mt-1 list-disc pl-5">
            <li>how many messages are waiting or in flight;</li>
            <li>the oldest message&rsquo;s age;</li>
            <li>how many are in the dead-letter queue;</li>
            <li>messages sent or received by services without Trailbeam.</li>
          </ul>
          <p className="mt-2">The SQS console&rsquo;s Monitoring tab shows these.{console && <> <a className={link} href={console} target="_blank" rel="noreferrer">Open in the SQS console ↗</a></>}</p>
        </div>
      </section>
    );
  }
  if (aws.status !== "ok") {
    return (
      <section className="border-t border-line py-5">
        <div className="max-w-[72ch] text-[15px] leading-[1.55] text-ink-2">
          <p><strong className="font-medium text-ink">Depth, in flight, the oldest message&rsquo;s age and the DLQ&rsquo;s depth come from CloudWatch.</strong> {AWS_REASON[aws.status](aws.detail)}{aws.status === "no_account" && p.href.connect && <> <Link href={p.href.connect} className={link}>Connect an AWS account →</Link></>}</p>
          <p className="mt-1">Until then, this page shows what your services reported.</p>
        </div>
      </section>
    );
  }
  const { range } = d;
  const times = grid(range.fromMs, range.toMs, range.stepS);
  const s = aws.series;
  const common = { times, stepS: range.stepS, hover, onHover: setHover, sel: p.sel, summary: `${windowText(range.fromMs, range.toMs)} UTC` };
  const quiet = d.series.length === 0 ? " Your services reported nothing in this range." : "";
  const caption = `Each point is CloudWatch's minute. The newest is about a minute behind.${quiet}`;
  const retention = aws.settings?.retentionS ?? null;
  const spanSends = d.series.reduce((n, b) => n + b.sends, 0);
  const cover = s ? coverageText(s.sent.length ? s.sent.reduce((n, [, v]) => n + v, 0) : null, spanSends) : null;
  const fmtN = (n: number) => fmtNum(Math.round(n));
  const failed = (what: string) => aws.errors.find((e) => e.toLowerCase().includes(what));
  return (
    <section className="border-t border-line py-5">
      <h2 className={eyebrow}>From CloudWatch, read with your role</h2>
      {!s ? (
        <p className="mt-2 text-[15px] text-ink-2">{aws.errors[0] ?? aws.detail ?? "CloudWatch returned nothing for this queue."}{p.href.connections && <> <Link href={p.href.connections} className={link}>Check the role in Connections →</Link></>}</p>
      ) : (
        <div className="mt-3 grid gap-4 md:grid-cols-3">
          <div className="md:col-span-1">
            <Chart {...common} title="Visible and in flight" source="CloudWatch" fmt={fmtN}
              lines={[{ key: "visible", label: "visible", color: WAIT, values: onGrid(s.visible, times, range.stepS) }, { key: "inflight", label: "in flight", color: HANDLER, values: onGrid(s.inFlight, times, range.stepS) }]}
              legend={[{ label: "visible", color: WAIT }, { label: "in flight", color: HANDLER }]} caption={caption} />
          </div>
          <Chart {...common} title="Oldest message" source="CloudWatch" fmt={(n) => (n === 0 ? "0" : fmtS(n))}
            lines={[{ key: "oldest", label: "oldest", color: WAIT, values: onGrid(s.oldestS, times, range.stepS) }]}
            rules={retention ? [{ value: retention * 0.9, label: "90% of retention" }] : []}
            legend={[{ label: "age of the oldest message", color: WAIT }, ...(retention ? [{ label: "90% of retention", color: "var(--color-line-strong)", shape: "rule" as const }] : [])]} caption={caption} />
          {aws.settings?.dlqName ? (
            failed("dlq") ? <p className="text-sm text-ink-2">{failed("dlq")}</p> : (
              <Chart {...common} title="DLQ depth" source="CloudWatch" fmt={fmtN}
                lines={[{ key: "dlq", label: aws.settings.dlqName, color: LOST, values: onGrid(s.dlq, times, range.stepS) }]}
                legend={[{ label: `visible in ${aws.settings.dlqName}`, color: LOST }]} caption={caption} />
            )
          ) : (
            <p className="self-center text-[15px] text-ink-3">No dead-letter queue in this queue&rsquo;s redrive policy.</p>
          )}
        </div>
      )}
      {cover && <p className="mt-3 max-w-[80ch] text-[15px] text-ink-2">{cover}</p>}
      {s && aws.errors.length > 0 && (
        <ul className="mt-2 text-sm text-ink-3">{aws.errors.map((e) => <li key={e}>{e}{p.href.connections && <> <Link href={p.href.connections} className={link}>Check the role in Connections →</Link></>}</li>)}</ul>
      )}
    </section>
  );
}

function Spike({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? A;
  if (!p.sel) return null;
  const [fromMs, toMs] = p.sel;
  const t = totalsIn(d.series, fromMs, toMs);
  const items: [MessagesView, string, number | null][] = [
    ["sent", "Sent", t.sends], ["waited", "Waited longest", null], ["slowest", "Slowest in handler", null],
    ["failed", "Failed or retried", t.failedOrRetried], ["dead_lettered", "Dead-lettered", t.dlq],
  ];
  const links = items.map(([view, label, n]) => ({ label, n, href: p.href.messages({ queue: d.queue.name, fromMs, toMs, view }) })).filter((x) => x.href);
  return (
    <section className="border-t border-line py-4" aria-label="Messages behind the selected minutes">
      <div className="flex items-baseline justify-between gap-3">
        <h2 className="text-[1.1rem] text-ink">Messages in <span className="font-mono">{windowText(fromMs, toMs)}</span> <span className="text-caption text-ink-3">UTC</span></h2>
        <button type="button" onClick={() => p.onSelect(null)} className="text-sm text-ink-3 hover:text-ink">Clear ×</button>
      </div>
      <p className="mt-2 flex flex-wrap gap-x-2 gap-y-1 text-[15px]">
        {links.map((x, i) => (
          <span key={x.label}>{i > 0 && <span aria-hidden className="mr-2 text-ink-3">·</span>}
            <Link href={x.href!} className={link}>{x.label}{x.n !== null && <> <span className={mono}>{fmtNum(x.n)}</span></>} →</Link>
          </span>
        ))}
      </p>
      <p className="mt-1 text-caption text-ink-3">Messages Trailbeam recorded in this window.</p>
    </section>
  );
}

const th = "py-2 pr-4 text-left font-mono text-caption font-medium text-ink-3";
const td = "py-2 pr-4 align-top";
const num = `${td} ${mono} text-caption text-ink-2 text-right`;

function triggerLine(t: Trigger) {
  return [
    t.batchSize !== null && `batch ${t.batchSize}`,
    t.batchingWindowS !== null && `window ${t.batchingWindowS} s`,
    t.state,
    t.maxConcurrency !== null && `max concurrency ${t.maxConcurrency}`,
    t.reportBatchItemFailures && "reports batch item failures",
    t.filters > 0 && `${t.filters} filter${t.filters === 1 ? "" : "s"}`,
  ].filter(Boolean) as string[];
}

function Tables({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? A;
  const [moreS, setMoreS] = useState(false);
  const [moreC, setMoreC] = useState(false);
  const mins = Math.max(1, (d.range.toMs - d.range.fromMs) / 60_000);
  const senders = [...d.senders].sort((a, b) => b.sends - a.sends);
  const consumers = [...d.consumers].sort((a, b) => b.receives - a.receives);
  const name = (svc: string, children: ReactNode) => {
    const h = p.href.service?.(svc, d.queue.name);
    return h ? <Link href={h} className="hover:underline">{children}</Link> : children;
  };
  const dash = <span className="text-ink-3">-</span>;
  const vers = (vs: string[]) => (vs.length ? <span className="font-mono text-caption">{vs.slice(-3).join(" ")}{vs.length > 3 ? " …" : ""}</span> : dash);
  const when = (ms: number | null) => (ms ? <span className={`${mono} text-caption`}>{utc(ms, { seconds: true })}</span> : dash);
  const more = (n: number, on: boolean, set: (b: boolean) => void) => (n > 8 && !on ? <button type="button" onClick={() => set(true)} className="mt-1 text-sm text-brand hover:underline">Show {n - 8} more</button> : null);
  const enabled = (t: Trigger) => (t.state ?? "Enabled") === "Enabled";
  const trig = (t: Trigger) => (
    <p className="text-caption text-ink-3">
      trigger: {triggerLine(t).map((w, i) => (
        <span key={w}>{i > 0 && " · "}<span className={w === t.state && !enabled(t) ? "text-critical-text" : undefined}>{w}</span></span>
      ))}
    </p>
  );
  // columns no row has a value for are left out: self-hosted Trailbeam counts only receives per consumer
  const cCols = {
    handled: consumers.some((c) => c.handled !== null), failed: consumers.some((c) => c.failed !== null), p95: consumers.some((c) => c.p95HandlerMs !== null),
    versions: consumers.some((c) => c.versions.length > 0), last: consumers.some((c) => c.lastMs !== null),
  };
  const sCols = { versions: senders.some((s) => s.versions.length > 0), last: senders.some((s) => s.lastMs !== null) };
  const cSpan = 2 + Object.values(cCols).filter(Boolean).length;
  const sSpan = 2 + Object.values(sCols).filter(Boolean).length;
  const cRow = (c: Consumer) => (
    <tr key={c.name} className="border-t border-line/60">
      <td className={td}>
        <span className="text-ink">{name(c.name, c.name)}</span> <span className="text-caption text-ink-3">{c.kind === "lambda" ? "Lambda" : "service"}</span>
        {c.trigger && trig(c.trigger)}
      </td>
      <td className={num}>{fmtNum(c.receives)}</td>
      {cCols.handled && <td className={num}>{c.handled === null ? dash : fmtNum(c.handled)}</td>}
      {cCols.failed && <td className={num}>{c.failed === null ? dash : fmtNum(c.failed)}</td>}
      {cCols.p95 && <td className={num}>{c.p95HandlerMs === null ? dash : fmtMs(c.p95HandlerMs)}</td>}
      {cCols.versions && <td className={td}>{vers(c.versions)}</td>}
      {cCols.last && <td className={`${td} text-right`}>{when(c.lastMs)}</td>}
    </tr>
  );
  return (
    <section className="border-t border-line py-5">
      <div className="overflow-x-auto">
        <table className="w-full text-[15px]">
          <thead><tr>
            <th className={`${th} w-1/3`}>Senders</th><th className={`${th} text-right`}>Sends/min</th>
            {sCols.versions && <th className={th}>Versions</th>}{sCols.last && <th className={`${th} text-right`}>Last send</th>}
          </tr></thead>
          <tbody>
            {senders.length === 0 && <tr><td colSpan={sSpan} className={`${td} text-ink-3`}>No service reported a send on this queue in the range.</td></tr>}
            {(moreS ? senders : senders.slice(0, 8)).map((s) => (
              <tr key={s.service} className="border-t border-line/60">
                <td className={`${td} text-ink`}>{name(s.service, s.service)}</td>
                <td className={num}>{fmtRate(s.sends / mins)}</td>
                {sCols.versions && <td className={td}>{vers(s.versions)}</td>}
                {sCols.last && <td className={`${td} text-right`}>{when(s.lastMs)}</td>}
              </tr>
            ))}
          </tbody>
        </table>
        {more(senders.length, moreS, setMoreS)}
      </div>
      <div className="mt-5 overflow-x-auto">
        <table className={`w-full text-[15px] ${cSpan > 4 ? "min-w-[720px]" : ""}`}>
          <thead><tr>
            <th className={`${th} w-1/3`}>Consumers</th><th className={`${th} text-right`}>Receives</th>
            {cCols.handled && <th className={`${th} text-right`}>Handled</th>}{cCols.failed && <th className={`${th} text-right`}>Failed</th>}
            {cCols.p95 && <th className={`${th} text-right`}>p95 handler</th>}{cCols.versions && <th className={th}>Versions</th>}
            {cCols.last && <th className={`${th} text-right`}>Last receive</th>}
          </tr></thead>
          <tbody>
            {consumers.length === 0 && !(d.silentTriggers ?? []).length && <tr><td colSpan={cSpan} className={`${td} text-ink-3`}>No service reported a receive on this queue in the range.</td></tr>}
            {(moreC ? consumers : consumers.slice(0, 8)).map(cRow)}
            {(d.silentTriggers ?? []).map((t) => (
              <tr key={`t:${t.functionName}`} className="border-t border-dashed border-line">
                <td className={td}>
                  <span className="text-ink">{t.functionName}</span> <span className="text-caption text-ink-3">Lambda</span> <Tag inferred>inferred</Tag>
                  {trig(t)}
                </td>
                <td colSpan={cSpan - 1} className={`${td} text-sm text-ink-2`}>No spans from this function in the range</td>
              </tr>
            ))}
          </tbody>
        </table>
        {more(consumers.length, moreC, setMoreC)}
      </div>
    </section>
  );
}

const OUTCOME: Record<CauseRow["outcome"], { label: string; color: string; shape: "diamond" | "square" }> = {
  dead_lettered: { label: "Dead-lettered", color: LOST, shape: "diamond" },
  stuck: { label: "Stuck", color: WAIT, shape: "square" },
  filtered: { label: "Filtered", color: "var(--color-ink-3)", shape: "square" },
};
const RANK = ["dead_lettered", "stuck", "filtered"];

function Causes({ d, p }: { d: QueuePageData; p: QueuePageProps }) {
  const Link = p.Link ?? A;
  const rows = Array.isArray(d.causes) ? [...d.causes].sort((a, b) => RANK.indexOf(a.outcome) - RANK.indexOf(b.outcome)) : null;
  const counts = Array.isArray(d.causes) ? null : (d.causes as CauseCounts);
  const none = rows ? rows.length === 0 : counts!.stuck === 0 && counts!.dlq === 0;
  const dlqHref = p.href.messages({ queue: d.queue.name, fromMs: d.range.fromMs, toMs: d.range.toMs, view: "dead_lettered" });
  return (
    <section className="border-t border-line py-5">
      <div className="flex items-baseline justify-between gap-3">
        <h2 className="text-[1.1rem] text-ink">Why messages here didn&rsquo;t finish</h2>
        <Link href={p.href.stuck(d.queue.name)} className={`text-sm ${link}`}>All in Needs attention →</Link>
      </div>
      {none ? <p className="mt-2 text-[15px] text-ink-3">Nothing unfinished on this queue in the range.</p> : (
        <ul className="mt-2">
          {rows?.map((r) => (
            <li key={r.id} className="border-t border-line/60 first:border-0">
              <Link href={r.href} className="grid grid-cols-[auto_1fr_auto] items-baseline gap-3 py-2 hover:bg-surface/40">
                <span className="inline-flex items-center gap-1.5 text-sm text-ink-2"><Key item={{ label: "", color: OUTCOME[r.outcome].color, shape: OUTCOME[r.outcome].shape }} />{OUTCOME[r.outcome].label}</span>
                <span className="min-w-0 truncate text-ink">{r.signature}{r.consumer && <span className="text-ink-3"> in {r.consumer}</span>}{r.versions?.length ? <span className="text-caption text-ink-3"> · seen in {r.versions.join(", ")}</span> : null}</span>
                <span className={`${mono} text-caption text-ink-2`}>{fmtNum(r.messages)} msgs · last {utc(r.lastMs)} →</span>
              </Link>
            </li>
          ))}
          {counts && counts.stuck > 0 && (
            <li className="flex items-baseline gap-3 py-2">
              <Key item={{ label: "", color: WAIT }} />
              <Link href={p.href.stuck(d.queue.name)} className={link}><span className={mono}>{fmtNum(counts.stuck)}{counts.stuckMore ? "+" : ""}</span> stuck messages →</Link>
            </li>
          )}
          {counts && counts.dlq > 0 && (
            <li className="flex items-baseline gap-3 border-t border-line/60 py-2">
              <Key item={{ label: "", color: LOST, shape: "diamond" }} />
              {dlqHref ? <Link href={dlqHref} className={link}><span className={mono}>{fmtNum(counts.dlq)}</span> moved to the DLQ →</Link>
                : <span className="text-ink-2"><span className={mono}>{fmtNum(counts.dlq)}</span> moved to the DLQ in the range</span>}
            </li>
          )}
        </ul>
      )}
    </section>
  );
}

const SPAN_TITLES = ["Sends vs handled (/min)", "Receives vs handled (/min)", "p95 time in queue (s)", "p95 time in handler (s)"];

/** The queue page. */
export function QueuePage(p: QueuePageProps) {
  const [hover, setHover] = useState<number | null>(null);
  const Link = p.Link ?? A;
  const d = p.data;
  if (p.unknown) {
    return (
      <div className="mx-auto max-w-[1200px]">
        <Header p={p} d={null} />
        <div className="mt-6 border-t border-line pt-5 text-[15px] text-ink-2">
          <p>No service has reported a call on <span className="font-mono text-ink">{p.name}</span>. A queue appears when a service reports a send or receive on it.</p>
          <p className="mt-2"><Link href={p.href.queues} className={link}>All queues →</Link></p>
        </div>
      </div>
    );
  }
  const hasVersions = !!d?.series.some((b) => b.byVersion?.some((v) => v.version !== null));
  const empty = d && d.series.length === 0;
  return (
    <div className="mx-auto max-w-[1200px]">
      <Header p={p} d={d} />
      {d && <><div className="mt-4 border-t border-line" /><SettingsRow d={d} p={p} /><NowRow d={d} p={p} /></>}
      <section className="py-5">
        <div className="flex items-center justify-between gap-3">
          <h2 className={eyebrow}>From your services&rsquo; spans</h2>
          {hasVersions && (
            <label className="inline-flex items-center gap-2 text-sm text-ink-2">
              <input type="checkbox" checked={p.split} onChange={(e) => p.onSplit(e.target.checked)} /> By version
            </label>
          )}
        </div>
        <div className="mt-3">
          {p.error ? (
            <p className="text-[15px] text-ink-2" role="alert">Couldn&rsquo;t load this queue&rsquo;s spans: {p.error}{p.onRetry && <> <button type="button" onClick={p.onRetry} className={link}>Try again</button></>}</p>
          ) : !d ? (
            <div className="grid gap-4 md:grid-cols-2">{SPAN_TITLES.map((t) => <ChartSkeleton key={t} title={t} />)}</div>
          ) : empty ? (
            <p className="text-[15px] text-ink-2">
              No send or receive on <span className="font-mono text-ink">{d.queue.name}</span> from {utc(d.range.fromMs, { date: true })} to {utc(d.range.toMs, { date: true })} UTC.{" "}
              <Link href={p.href.range("1d")} className={link}>Past 24 hours</Link> · <Link href={p.href.range("7d")} className={link}>Past 7 days</Link>
            </p>
          ) : (
            <SpanCharts d={d} p={p} hover={hover} setHover={setHover} />
          )}
        </div>
      </section>
      {d && <Spike d={d} p={p} />}
      {d && <CloudWatchBand d={d} p={p} hover={hover} setHover={setHover} />}
      {d && <Tables d={d} p={p} />}
      {d && <Causes d={d} p={p} />}
    </div>
  );
}
