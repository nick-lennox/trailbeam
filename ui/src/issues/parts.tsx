import { CheckCircle2, Copy, Filter, PauseCircle, RotateCw, Skull } from "lucide-react";
import type { ReactNode } from "react";
import { Tooltip } from "../design/Tooltip.tsx";
import { utc, type Breakdown, type Issue, type Lifespan, type Severity, type Step, type Timing } from "../model/issues.ts";

/*
  The parts the Issues list and an issue's page share. Color says one thing, as in the Event
  explorer: how bad. Red is a message in a dead-letter queue, orange one that has not finished,
  amber one that finished after a retry. Moved from the hosted app (QV-583).
*/

/* An issue's triage status: the host's triage says it (open when the host has no triage). */
export type IssueStatus = "open" | "resolved" | "ignored";

export const TONE: Record<Severity, string> = { critical: "var(--color-critical)", serious: "var(--color-serious)", warning: "var(--color-warning)" };
export const TONE_TEXT: Record<Severity, string> = { critical: "text-critical-text", serious: "text-serious-text", warning: "text-warning-text" };

const CRITICAL = "bg-critical-soft text-critical-text ring-critical-ring";
const SERIOUS = "bg-serious-soft text-serious-text ring-serious-ring";
const WARNING = "bg-warning-soft text-warning-text ring-warning-ring";

type Standing = Pick<Issue, "kind" | "count" | "inDlq" | "notFinished" | "handledTwice">;

/* How an issue's messages stand now: the worst of them names it. Always an icon and a label. */
function look(i: Omit<Standing, "count">) {
  if (i.inDlq > 0) return { label: "Dead-lettered", icon: Skull, cls: CRITICAL };
  if (i.notFinished > 0) return { label: "Not finished", icon: PauseCircle, cls: SERIOUS };
  if (i.kind === "filter") return { label: "Filtered by trigger", icon: Filter, cls: WARNING };
  if (i.handledTwice > 0) return { label: "Handled twice", icon: Copy, cls: WARNING };
  return { label: "Received again", icon: RotateCw, cls: WARNING };
}

export function IssuePill({ issue, status = "open" }: { issue: Omit<Standing, "count">; status?: IssueStatus }) {
  const l = status === "resolved" ? { label: "Resolved", icon: CheckCircle2, cls: "bg-good-soft text-good-text ring-good-ring" } : look(issue);
  const Icon = l.icon;
  return (
    <span className={`inline-flex shrink-0 items-center gap-1 whitespace-nowrap rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${l.cls}`}>
      <Icon size={12} strokeWidth={2.25} aria-hidden />
      {l.label}
    </span>
  );
}

/* The same dashed tag the timeline uses for a step nothing recorded. */
export function InferredTag() {
  return (
    <Tooltip content="No span recorded this. Trailbeam worked it out from the steps around it, such as a receive count that went up with no delete in between." className="inline-flex shrink-0 cursor-pointer">
      <span className="inline-flex items-center rounded-md border border-dashed border-line bg-surface px-1.5 py-px text-micro whitespace-nowrap text-ink-3 italic">inferred</span>
    </Tooltip>
  );
}

/* Messages per interval, oldest on the left: the line the dashboard's queue table draws in a cell. */
export function Trend({ issue }: { issue: Pick<Issue, "trend" | "trendLabel"> }) {
  const pts = issue.trend, max = Math.max(...pts, 1);
  return (
    <svg width="72" height="16" viewBox="0 0 72 16" role="img" aria-label={`Messages over the ${issue.trendLabel}, oldest first: ${pts.join(", ")}`} className="block">
      <path d={pts.map((v, i) => `${i ? "L" : "M"}${((72 * i) / Math.max(pts.length - 1, 1)).toFixed(1)},${(14 - 12 * (v / max)).toFixed(1)}`).join(" ")} fill="none" stroke="var(--color-series-1)" strokeWidth={1.25} strokeLinejoin="round" />
    </svg>
  );
}

/* How a message stands, in the order the charts stack them: the worst at the bottom. */
export type Tally = { dlq: number; open: number; done: number };
export const STATES: { id: keyof Tally; label: string; color: string }[] = [
  { id: "dlq", label: "In a dead-letter queue", color: "var(--color-critical)" },
  { id: "open", label: "Not finished", color: "var(--color-serious)" },
  { id: "done", label: "No longer in the queue", color: "var(--color-ink-3)" },
];

/* One issue's failing messages along the page's time range: the same axis in every row, so
   issues that start together line up. `at` is the moment under the pointer on any chart, as a
   share of the range. Each row has its own scale. */
export function RowBars({ cells, at, onAt, label }: { cells: Tally[]; at: number | null; onAt: (f: number | null) => void; label: (i: number) => string }) {
  const sum = (c: Tally) => c.dlq + c.open + c.done;
  const top = Math.max(...cells.map(sum), 1);
  return (
    <span
      className="relative flex h-7 items-stretch border-b border-line-strong"
      onPointerMove={(e) => { const r = e.currentTarget.getBoundingClientRect(); onAt(Math.max(0, Math.min(1, (e.clientX - r.left) / r.width))); }}
      onPointerLeave={() => onAt(null)}
    >
      {cells.map((c, i) => {
        const n = sum(c);
        if (!n) return <span key={i} className="flex-1" />;
        return (
          <Tooltip
            key={i} delay={0} className="flex min-w-0 flex-1 cursor-pointer flex-col-reverse px-[0.5px]"
            content={<>
              <span className="block font-mono text-ink-3">{label(i)}</span>
              {STATES.filter((s) => c[s.id] > 0).map((s) => (
                <span key={s.id} className="mt-0.5 flex items-center gap-1.5"><span className="size-2 shrink-0 rounded-[2px]" style={{ background: s.color }} /><span className="font-mono font-medium text-ink">{c[s.id]}</span> {s.label.toLowerCase()}</span>
              ))}
            </>}
          >
            {STATES.filter((s) => c[s.id] > 0).map((s) => <i key={s.id} className="block rounded-[1px]" style={{ height: `${Math.max(8, (c[s.id] / top) * 100)}%`, background: s.color }} />)}
            <span className="sr-only">{n} messages, {label(i)}</span>
          </Tooltip>
        );
      })}
      {at !== null && <i aria-hidden className="pointer-events-none absolute inset-y-0 w-px bg-ink-3" style={{ left: `${at * 100}%` }} />}
    </span>
  );
}

/* Status as the dashboard's queue table shows it: a small square in the color of how bad. */
export function Mark({ tone }: { tone: Severity | "good" }) {
  return <span aria-hidden className="size-2 shrink-0 rounded-[2px]" style={{ background: tone === "good" ? "var(--color-good)" : TONE[tone] }} />;
}

/* A panel with the dashboard widget's frame: a 13px title on a 36px header, then the body. */
export function Panel({ title, right, children, className = "", label }: { title: ReactNode; right?: ReactNode; children: ReactNode; className?: string; label?: string }) {
  return (
    <section aria-label={label} className={`flex flex-col overflow-hidden rounded-card border border-line bg-surface shadow-card ${className}`}>
      <header className="flex h-9 shrink-0 items-center gap-2 pr-3 pl-3">
        <h2 className="truncate font-serif text-[13px] font-semibold text-ink">{title}</h2>
        {right && <span className="ml-auto flex shrink-0 items-center gap-3 text-xs text-ink-3">{right}</span>}
      </header>
      {children}
    </section>
  );
}

/* A figure with its units: digits in the mono face, the words around them in the text face. */
export function Figure({ children, className = "" }: { children: string; className?: string }) {
  return (
    <span className={`whitespace-nowrap ${className}`}>
      {children.split(/(\d[\d,.]*)/).filter(Boolean).map((t, i) => (/^\d/.test(t) ? <span key={i} className="font-mono font-medium tabular-nums">{t}</span> : <span key={i} className="font-sans text-xs font-normal text-ink-2">{t}</span>))}
    </span>
  );
}

/* Where an issue's messages are now. */
export function parts(i: Pick<Issue, "count" | "inDlq" | "notFinished">) {
  const rest = Math.max(0, i.count - i.inDlq - i.notFinished);
  return [
    { key: "dlq", n: i.inDlq, label: "in a dead-letter queue", short: "in a DLQ", color: TONE.critical, text: TONE_TEXT.critical },
    { key: "open", n: i.notFinished, label: "not finished", short: "not finished", color: TONE.serious, text: TONE_TEXT.serious },
    { key: "done", n: rest, label: "no longer in the queue", short: "no longer in the queue", color: "var(--color-ink-3)", text: "text-ink-2" },
  ].filter((p) => p.n > 0);
}
const secs = (s: number) => (s < 1 ? `${Math.round(s * 1000)} ms` : s % 1 === 0 ? `${s} s` : `${s.toFixed(1)} s`);

/* One receive laid out along the visibility timeout: how long the consumer held the message
   before it failed, and how long the message then sat in flight with nothing working on it. */
export function TimingRuler({ t }: { t: Timing }) {
  const vt = t.visibilityTimeoutS;
  const end = vt !== null && vt > t.atS ? vt : t.atS;
  const at = Math.max((t.atS / end) * 100, 1.5);
  const idle = vt !== null && vt > t.atS ? vt - t.atS : null;
  return (
    <div>
      <div className="relative h-5">
        <span className="absolute inset-y-0 left-0 rounded-l-[3px]" style={{ width: `${at}%`, background: "var(--color-series-3)" }} />
        {idle !== null && (
          <span className="absolute inset-y-0 right-0 rounded-r-[3px] border border-l-0 border-line-strong"
            style={{ left: `${at}%`, backgroundImage: "repeating-linear-gradient(135deg, transparent 0 5px, color-mix(in srgb, var(--color-ink-3) 38%, transparent) 5px 6px)" }} />
        )}
        <span className="absolute -inset-y-1 w-[2px] -translate-x-1/2 rounded-full bg-critical" style={{ left: `${at}%` }} />
      </div>
      <div className="relative mt-1.5 h-4 font-mono text-micro tabular-nums text-ink-3">
        <span className="absolute left-0">0 s</span>
        {at > 12 && at < 88 && <span className="absolute -translate-x-1/2 text-critical-text" style={{ left: `${at}%` }}>{secs(t.atS)}</span>}
        <span className="absolute right-0">{idle !== null ? <>{secs(vt!)}<span className="font-sans">, visibility timeout</span></> : secs(t.atS)}</span>
      </div>
      <p className="mt-2 text-[13px] leading-relaxed text-ink-2">
        {t.label} {secs(t.atS)} into the receive.{" "}
        {idle !== null
          ? `The message then stays in flight for another ${secs(idle)} with nothing working on it, until the visibility timeout expires and SQS makes it visible to receive again.`
          : vt === null ? "No data on this queue's visibility timeout, so how long the message then waits to be received again is not known." : "That is at or past the visibility timeout, so the message may already have been received again."}
      </p>
    </div>
  );
}

const EDGE: Record<Step["tone"], string> = { plain: "transparent", warn: "var(--color-warning)", bad: "var(--color-critical)" };

/* The steps an issue's messages took, in order. The left edge is the Event explorer's:
   amber for a step to look at, red for a failure. */
export function Steps({ steps }: { steps: Step[] }) {
  return (
    <ol className="grid">
      {steps.map((s, i) => (
        <li key={i} className="flex items-center gap-3 border-b border-line py-1.5 pr-4 pl-4 last:border-0" style={{ boxShadow: `inset 3px 0 0 ${EDGE[s.tone]}` }}>
          <span className="w-5 shrink-0 text-right font-mono text-caption tabular-nums text-ink-3">{i + 1}</span>
          <span className={`text-[13px] ${s.tone === "bad" ? "font-medium text-ink" : "text-ink-2"}`}>{s.label}</span>
          {s.inferred && <span className="ml-auto"><InferredTag /></span>}
        </li>
      ))}
    </ol>
  );
}

const LIFE = { dlq: { color: TONE.critical, label: "ended in a dead-letter queue" }, open: { color: TONE.serious, label: "not finished" }, done: { color: "var(--color-ink-3)", label: "no longer in the queue" } } as const;
const LIFE_ROWS = 60;

/* Every message in the issue as one line: from when it was first seen to when it was last seen,
   with a mark at the end for how it stands. Reads like a trace list: when the trouble began,
   whether it is still arriving, and how long each message lasted. */
export function Lifespans({ spans, now }: { spans: Lifespan[]; now: number }) {
  const shown = spans.slice(-LIFE_ROWS);
  const start = Math.min(...shown.map((s) => s.from));
  const width = Math.max(now - start, 1000) * 1.02;
  const x = (t: number) => ((t - start) / width) * 100;
  const day = now - start > 86400_000;
  const tick = (t: number) => (day ? utc(t).slice(5, 16) : utc(t).slice(11, 19));
  const rowH = shown.length > 40 ? 4 : shown.length > 24 ? 8 : 10;
  const counts = (["dlq", "open", "done"] as const).map((k) => ({ k, n: spans.filter((s) => s.state === k).length })).filter((c) => c.n > 0);
  return (
    <div>
      <div className="relative border-b border-l border-line" style={{ height: Math.max(shown.length * rowH + 8, 48) }} role="img"
        aria-label={`${spans.length} messages from ${utc(start)} to ${utc(now)} UTC: ${counts.map((c) => `${c.n} ${LIFE[c.k].label}`).join(", ")}`}>
        {[0.25, 0.5, 0.75].map((f) => <span key={f} aria-hidden className="absolute inset-y-0 w-px bg-line opacity-55" style={{ left: `${f * 100}%` }} />)}
        {shown.map((s, i) => {
          const c = LIFE[s.state].color;
          return (
            <span key={i} aria-hidden className="absolute flex items-center" style={{ top: 4 + i * rowH, height: rowH - 1, left: `${x(s.from)}%`, width: `max(${x(s.to) - x(s.from)}%, 5px)` }}>
              <span className="h-px min-w-0 flex-1" style={{ background: c, opacity: 0.55 }} />
              <span className="shrink-0 rounded-full" style={{ width: Math.min(rowH - 1, 5), height: Math.min(rowH - 1, 5), background: s.state === "open" ? "transparent" : c, boxShadow: s.state === "open" ? `inset 0 0 0 1.25px ${c}` : undefined }} />
            </span>
          );
        })}
      </div>
      <div className="mt-1.5 flex justify-between font-mono text-micro tabular-nums text-ink-3">
        <span>{tick(start)}</span><span>{tick(start + width / 2)}</span><span>{tick(now)}<span className="font-sans"> UTC, now</span></span>
      </div>
      <div className="mt-2.5 flex flex-wrap gap-x-5 gap-y-1 text-xs text-ink-2">
        {counts.map((c) => (
          <span key={c.k} className="inline-flex items-center gap-1.5">
            <i aria-hidden className="block size-[7px] rounded-full" style={c.k === "open" ? { boxShadow: `inset 0 0 0 1.25px ${LIFE[c.k].color}` } : { background: LIFE[c.k].color }} />
            <span className="font-mono font-medium tabular-nums text-ink">{c.n}</span>{LIFE[c.k].label}
          </span>
        ))}
        {spans.length > shown.length && <span className="text-ink-3">The latest {shown.length} of {spans.length} are drawn.</span>}
      </div>
    </div>
  );
}

/* What sets an issue's messages apart: one row per field and value, with its share of this
   issue's messages and, where known, its share of all messages on the queue, on one track.
   Sorted so the widest gap is on top. */
export function Apart({ breakdowns, total }: { breakdowns: Breakdown[]; total: number }) {
  const rows = breakdowns
    .flatMap((b) => b.rows.map((r) => ({ field: b.label, mono: b.mono, value: r.value, n: r.n, share: total ? r.n / total : 0, base: r.base })))
    .sort((a, b) => (b.share - (b.base ?? b.share)) - (a.share - (a.base ?? a.share)) || b.share - a.share);
  const compared = rows.some((r) => r.base !== undefined);
  const pct = (f: number) => `${Math.round(f * 100)}%`;
  const cols = compared ? "grid-cols-[9rem_minmax(0,1fr)_minmax(8rem,16rem)_3.25rem_4.75rem_3.5rem]" : "grid-cols-[9rem_minmax(0,1fr)_minmax(8rem,16rem)_3.25rem]";
  return (
    <div className="overflow-x-auto rounded-card border border-line bg-surface shadow-card">
      <div className="flex h-9 min-w-[640px] items-center gap-x-4 px-3">
        <h2 className="font-serif text-[13px] font-semibold text-ink">What sets these messages apart</h2>
        <span className="ml-auto flex items-center gap-x-3.5 text-xs text-ink-2">
          <span className="inline-flex items-center gap-1.5"><i aria-hidden className="block size-[9px] rounded-full bg-series-1" />Share of this issue&apos;s messages</span>
          {compared && <span className="inline-flex items-center gap-1.5"><i aria-hidden className="block size-[9px] rounded-full" style={{ boxShadow: "inset 0 0 0 1.5px var(--color-ink-3)" }} />Share of all messages on the queue</span>}
        </span>
      </div>
      <div className={`grid min-w-[640px] ${cols} items-center gap-x-4 border-y border-line px-3 py-[7px] text-xs font-medium text-ink-3`}>
        <span>Field</span><span>Value</span>
        <span className="flex justify-between font-mono text-[10.5px] font-normal tabular-nums"><span>0%</span><span>50%</span><span>100%</span></span>
        <span className="text-right">This issue</span>
        {compared && <><span className="text-right">Whole queue</span><span className="text-right">Times</span></>}
      </div>
      <ul className="min-w-[640px] divide-y divide-line">
        {rows.map((r) => {
          const lift = r.base !== undefined && r.base > 0 ? r.share / r.base : null;
          const lead = r.base !== undefined && r.share - r.base >= 0.25;
          const lo = Math.min(r.share, r.base ?? r.share) * 100, hi = Math.max(r.share, r.base ?? r.share) * 100;
          return (
            <li key={`${r.field}:${r.value}`} className={`grid ${cols} items-center gap-x-4 px-3 py-2 text-[13px]`}>
              <span className={`truncate text-ink-3 ${r.field.startsWith("body.") ? "font-mono text-[12.5px]" : ""}`}>{r.field}</span>
              <span className={`truncate ${lead ? "font-medium text-ink" : "text-ink-2"} ${r.mono ? "font-mono text-[12.5px]" : ""}`}>{r.value}</span>
              <span className="relative block h-4" aria-hidden>
                <span className="absolute inset-x-0 top-1/2 h-px bg-line-strong" />
                {r.base !== undefined && <span className="absolute top-1/2 h-[3px] -translate-y-1/2 rounded-full" style={{ left: `${lo}%`, width: `${hi - lo}%`, background: lead ? "var(--color-series-1)" : "var(--color-ink-3)", opacity: 0.55 }} />}
                {r.base !== undefined && <span className="absolute top-1/2 size-[9px] -translate-x-1/2 -translate-y-1/2 rounded-full bg-surface" style={{ left: `${r.base * 100}%`, boxShadow: "inset 0 0 0 1.5px var(--color-ink-3)" }} />}
                <span className="absolute top-1/2 size-[9px] -translate-x-1/2 -translate-y-1/2 rounded-full" style={{ left: `${r.share * 100}%`, background: lead || r.base === undefined ? "var(--color-series-1)" : "var(--color-ink-2)" }} />
              </span>
              <span className="text-right font-mono text-[12.5px] tabular-nums text-ink">{pct(r.share)}</span>
              {compared && <span className="text-right font-mono text-[12.5px] tabular-nums text-ink-3">{r.base === undefined ? "" : pct(r.base)}</span>}
              {compared && <span className={`text-right font-mono text-[12.5px] tabular-nums ${lead ? "font-medium text-ink" : "text-ink-3"}`}>{lift === null ? (r.base === 0 ? <span className="font-sans">new</span> : "") : lift >= 10 ? `${Math.round(lift)}x` : `${lift.toFixed(1)}x`}</span>}
            </li>
          );
        })}
      </ul>
    </div>
  );
}
