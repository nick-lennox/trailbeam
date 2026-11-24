/*
  The Overview (QV-392 design): "what is talking to what, how much, and which queue should I open?". One ruled
  table with a row per queue, from GET /api/flows; a row opens in place to show its flow drawing. The same view
  in both products: the host passes its range control, its links, and (hosted) slots for what only it knows
  (health, CloudWatch depth and age, the Needs attention count, alert rules). A slot left out draws nothing: no
  column, no cell, no space. Role tokens (theme.css), so the view takes the host page's theme.
*/
import { Fragment, useRef, useState, type ComponentType, type ReactNode } from "react";
import { OUTCOME_LABELS } from "../causes/text.ts";
import {
  FILTER_FROM, LAMBDA_DELETES_HINT, NOTE_HINT, PAGE, filterFlows, fmtNum, fmtRate, fmtSeen, idleToggleText, isFifo, lambdaDeletes, notesOf,
  openKey, orderFlows, perMinute, servicesText, splitIdle, summaryOf, summaryText, type Flow, type Note,
} from "./derive.ts";
import { A, FlowDrawing, type LinkProps, type OverviewHrefs } from "./FlowDrawing.tsx";

export type OverviewSlots = {
  /** §7.1: the host's verdict line, above the summary sentence */
  summary?: ReactNode;
  /** §7.1: one ruled line under it (AWS not connected) */
  banner?: ReactNode;
  /** left of the range control ("See sample data") */
  actions?: ReactNode;
  /** §7.3: a pill on the line above the queue name */
  health?: (f: Flow) => ReactNode;
  /** §7.3: after Deletes, before DLQ */
  columns?: { id: string; header: string; hint?: string; cell: (f: Flow) => ReactNode }[];
  /** §7.3: the Needs attention count, which links to Needs attention for the queue */
  attention?: (f: Flow) => { count: number; tone: "dead_lettered" | "stuck" | "filtered" } | null;
  /** §7.3: a bell after the attention count */
  alert?: (f: Flow) => ReactNode;
  /** §7.2: under Sent and Received in the drawing */
  queueCard?: (f: Flow) => ReactNode;
  /** the host says the queue's dead-letter queue is growing: its wire is drawn critical */
  dlqBad?: (f: Flow) => boolean;
  /** §7.3: the host's row order */
  sort?: (a: Flow, b: Flow) => number;
  /** §8: the whole empty-store block, in place of self-hosted Trailbeam's wording */
  empty?: ReactNode;
};

export type OverviewState =
  | { kind: "ok"; updatedAt: number }
  | { kind: "loading" }
  | { kind: "error"; message: string; staleAt?: number; at?: number }
  | { kind: "empty-store" }
  | { kind: "unsupported" };

export type OverviewProps = {
  /** exactly the QV-363 Flow (= the hosted app's lib/app/flows.ts Flow) */
  flows: Flow[];
  /** `during` is the range as a phrase with its preposition: "in the last hour", "between 13:05 and 14:05 UTC" */
  range: { fromMs: number; toMs: number; during: string; endsNow: boolean };
  rangeControl: ReactNode;
  /** under the range control: a clipped start ("Spans before … aren't kept on this server.") */
  rangeNote?: ReactNode;
  copyLink?: ReactNode;
  /** beside the title: "Sample data" */
  titleTag?: ReactNode;
  /** flow= as the URL has it: undefined (absent) opens the first row, "" none, else a key or an old link's label */
  open: string | null | undefined;
  onOpen: (key: string | null) => void;
  showIdle: boolean;
  onShowIdle: () => void;
  /** the filter box's text, kept in q= by the host */
  query?: string;
  onQuery?: (q: string) => void;
  state: OverviewState;
  /** null for a fixed range */
  refresh: { on: boolean; toggle: () => void; now: () => void } | null;
  onRetry?: () => void;
  /** for "Spans stored, none in range": the presets longer than the current one */
  widen?: { label: string; href: string }[];
  /** the page's foot (self-hosted: the coverage sentence and the JSON link) */
  foot?: ReactNode;
  href: OverviewHrefs;
  /** what the host's nav calls Needs attention: "Stuck" in self-hosted Trailbeam, "Needs attention" hosted */
  stuckWord?: string;
  /** a doc link for the 501 state */
  unsupportedHref?: string;
  slots?: OverviewSlots;
  Link?: ComponentType<LinkProps>;
};

const mono = "font-mono tabular-nums";
const link = "text-brand hover:underline";
const th = "px-3 py-2 text-caption font-medium text-ink-3";
const thL = `${th} text-left`, thR = `${th} text-right`;
const OUTCOME_CLS = { dead_lettered: "text-critical-text", stuck: "text-warning-text", filtered: "text-ink-2" } as const;
const DLQ_HINT = "Known when a receive on a dead-letter queue names this queue as its source.";
const clock = (ms: number) => new Date(ms).toISOString().slice(11, 19);

const NoteText = ({ note }: { note: Note }) => <span className="text-ink-3" title={NOTE_HINT[note]}>{note}<span className="sr-only">: {NOTE_HINT[note]}</span></span>;

function Services({ f }: { f: Flow }) {
  if (f.sent === 0 && f.received === 0 && f.deleted === 0 && f.visibilityChanges === 0) return <span className="text-ink-3">last seen {fmtSeen(f.lastSeen)}</span>;
  const n = notesOf(f);
  const side = (s: "producers" | "consumers", note: Note | null) => {
    if (note) return <NoteText note={note} />;
    const t = servicesText(f[s], { side: s });
    return <span title={t.all}>{t.shown}{t.more > 0 && <span className="text-ink-3"> +{t.more}</span>}</span>;
  };
  return <>{side("producers", n.senders)} <span aria-label="to" className="text-ink-3">→</span> {side("consumers", n.receivers)}</>;
}

function RefreshStamp({ state, refresh }: Pick<OverviewProps, "state" | "refresh">) {
  if (!refresh) return null;
  const btn = "hover:text-ink";
  if (state.kind === "error" && state.staleAt) {
    return (
      <span className="text-caption text-warning-text" aria-live="polite">
        Couldn&rsquo;t update{state.at ? ` at ${clock(state.at)}` : ""}: {state.message}. Showing {clock(state.staleAt)}.{" "}
        <button type="button" onClick={refresh.now} className="underline">Try now</button>
      </span>
    );
  }
  return (
    <span className="flex items-baseline gap-3 text-caption text-ink-3">
      {state.kind === "ok" && <button type="button" onClick={refresh.now} className={`${mono} ${btn}`} title="Reload now">⟳ Updated {clock(state.updatedAt)}</button>}
      <button type="button" onClick={refresh.toggle} className={btn}>{refresh.on ? "Pause" : "Resume"}</button>
    </span>
  );
}

function Deletes({ f }: { f: Flow }) {
  const l = lambdaDeletes(f);
  if (l === "all") return <span className="font-sans text-ink-3" title={LAMBDA_DELETES_HINT}>By Lambda</span>;
  return <>{fmtNum(f.deleted)}{l === "mixed" && <span className="ml-1 font-sans text-micro text-ink-3" title={LAMBDA_DELETES_HINT}>+ Lambda</span>}</>;
}

/** Three ruled placeholder rows while the first load runs: no spinner, no shimmer. */
const Placeholder = ({ cols }: { cols: number }) => (
  <>{[0, 1, 2].map((i) => <tr key={i} aria-hidden className="border-t border-line"><td colSpan={cols} className="h-12" /></tr>)}</>
);

export function Overview(p: OverviewProps) {
  const { flows, range, href, slots = {}, Link = A, stuckWord = "Stuck" } = p;
  const [more, setMore] = useState(false);
  /* the order freeze (§3.2): fixed at the first load and at a range change; a refresh keeps it */
  const frozen = useRef<{ range: string; keys: string[] } | null>(null);
  const rangeKey = range.endsNow ? `now:${range.toMs - range.fromMs}` : `${range.fromMs}-${range.toMs}`;
  const keep = frozen.current && frozen.current.range === rangeKey ? frozen.current.keys : null;
  const ordered = orderFlows(flows, keep, slots.sort);
  if (p.state.kind === "ok" || p.state.kind === "error") frozen.current = { range: rangeKey, keys: ordered.map((f) => f.key) };

  const { active, idle } = splitIdle(ordered);
  const shown = filterFlows(active, p.query ?? "");
  const paged = more ? shown : shown.slice(0, PAGE);
  const idleShown = filterFlows(idle, p.query ?? "");
  const openK = p.open == null ? (paged[0] ?? idle[0])?.key ?? null : openKey(flows, p.open);
  const cols = slots.columns ?? [];
  const nCols = 6 + cols.length + (slots.attention ? 1 : 0) + 1;
  const rate = (n: number) => `${fmtRate(perMinute(n, range.fromMs, range.toMs))} a minute`;
  const s = p.state;

  const header = (
    <div className="flex flex-wrap items-baseline gap-x-4 gap-y-2">
      <h1 className="text-xl font-medium text-ink">Overview</h1>
      {p.titleTag}
      {s.kind !== "empty-store" && (
        <div className="ml-auto flex flex-wrap items-baseline gap-x-4 gap-y-2">
          {slots.actions}
          <div className="flex flex-col items-end gap-1">{p.rangeControl}{p.rangeNote && <span className="text-caption text-ink-3">{p.rangeNote}</span>}</div>
          {p.copyLink}
          <RefreshStamp state={s} refresh={range.endsNow ? p.refresh : null} />
        </div>
      )}
    </div>
  );

  if (s.kind === "empty-store") {
    return (
      <div>
        {header}
        {slots.empty ?? (
          <div className="mx-auto max-w-lg py-16 text-center">
            <p className="text-lg text-ink">No queue yet.</p>
            <p className="mt-2 text-sm text-ink-2">A queue appears here once one of your services sends or receives on it. Set up a service, or send a test message, from the Messages page.</p>
            {href.setUp && <p className="mt-4"><Link href={href.setUp} className={link}>Set up a service →</Link></p>}
          </div>
        )}
      </div>
    );
  }

  const row = (f: Flow, isIdle: boolean) => {
    const open = openK === f.key;
    const id = `flow-${f.key.replace(/[^A-Za-z0-9_-]/g, "_")}`;
    const att = slots.attention?.(f) ?? null;
    const health = slots.health?.(f);
    return (
      <Fragment key={f.key}>
        <tr className={`border-t border-line align-top ${open ? "bg-canvas" : ""}`}>
          <td className="w-8 py-3 pl-3">
            <button type="button" aria-expanded={open} aria-controls={id} aria-label={`${open ? "Close" : "Open"} ${f.queue}`} onClick={() => p.onOpen(open ? "" : f.key)} className="text-ink-3 hover:text-ink">
              {open ? "▾" : "▸"}
            </button>
          </td>
          <td className="px-3 py-3">
            {health && <div className="mb-1">{health}</div>}
            <span className="flex items-baseline gap-2">
              <Link href={href.queue(f)} className="font-mono text-ink hover:underline">{f.queue}</Link>
              {isFifo(f) && <span className="rounded border border-line px-1 text-micro text-ink-3">fifo</span>}
            </span>
            {(f.account || f.region) && <span className="block font-mono text-caption text-ink-3">{[f.account, f.region].filter(Boolean).join(" · ")}</span>}
            <span className="mt-1 block text-sm text-ink-2 md:hidden"><Services f={f} /></span>
          </td>
          <td className="hidden px-3 py-3 text-sm text-ink-2 md:table-cell"><Services f={f} /></td>
          <td className={`px-3 py-3 text-right ${mono} ${f.sent ? "text-ink" : "text-ink-3"}`} title={rate(f.sent)}>{fmtNum(f.sent)}</td>
          <td className={`px-3 py-3 text-right ${mono} ${f.received ? "text-ink" : "text-ink-3"}`} title={rate(f.received)}>{fmtNum(f.received)}</td>
          <td className={`px-3 py-3 text-right ${mono} ${f.deleted ? "text-ink" : "text-ink-3"}`} title={lambdaDeletes(f) === "none" ? rate(f.deleted) : undefined}><Deletes f={f} /></td>
          {cols.map((c) => <td key={c.id} className={`px-3 py-3 text-right ${mono}`}>{c.cell(f)}</td>)}
          {slots.attention && (
            <td className={`px-3 py-3 text-right ${mono}`}>
              <span className="inline-flex items-baseline gap-1.5">
                <Link href={href.stuck(f)} className={`hover:underline ${att && att.count > 0 ? OUTCOME_CLS[att.tone] : "text-ink-3"}`} title={att && att.count > 0 ? `Worst outcome: ${OUTCOME_LABELS[att.tone]}` : undefined}>
                  {att && att.count > 0 && <span aria-hidden className="mr-1">◆</span>}{fmtNum(att?.count ?? 0)}
                </Link>
                {slots.alert?.(f)}
              </span>
            </td>
          )}
          <td className="px-3 py-3 text-sm">
            {f.dlq ? (
              <span className="font-mono"><Link href={href.queue(flows.find((x) => x.queue === f.dlq!.name) ?? { queue: f.dlq.name, name: f.dlq.name, account: null, region: null })} className="text-ink-2 hover:underline">{f.dlq.name}</Link>
                <span className="text-ink-3"> · {fmtNum(f.dlq.received)}</span></span>
            ) : <span className="text-ink-3" title={DLQ_HINT}>—</span>}
          </td>
        </tr>
        {open && (
          <tr className="bg-canvas">
            <td colSpan={nCols} className="px-3 pb-4">
              <div className="rounded-lg border border-line">
                <FlowDrawing id={id} flow={f} flows={flows} range={range} href={href} Link={Link} queueCard={slots.queueCard?.(f)} dlqBad={slots.dlqBad?.(f)} stuckWord={stuckWord} />
              </div>
            </td>
          </tr>
        )}
      </Fragment>
    );
  };

  const table = (body: ReactNode) => (
    <table className="mt-4 w-full border-collapse border-b border-line text-sm">
      <thead>
        <tr className="border-t border-line">
          <th scope="col" className="w-8"><span className="sr-only">Open</span></th>
          <th scope="col" className={thL}>Queue</th>
          <th scope="col" className={`${thL} hidden md:table-cell`}>Senders → Receivers</th>
          <th scope="col" className={thR}>Sends</th>
          <th scope="col" className={thR}>Receives</th>
          <th scope="col" className={thR}>Deletes</th>
          {cols.map((c) => <th key={c.id} scope="col" className={thR} title={c.hint}>{c.header}</th>)}
          {slots.attention && <th scope="col" className={thR} title="Messages first seen over 5 minutes ago that didn't finish, including those moved to a dead-letter queue.">Needs attention</th>}
          <th scope="col" className={thL} title={DLQ_HINT}>DLQ</th>
        </tr>
      </thead>
      <tbody>{body}</tbody>
    </table>
  );

  let main: ReactNode;
  if (s.kind === "loading") main = table(<Placeholder cols={nCols} />);
  else if (s.kind === "unsupported") {
    main = (
      <p className="mt-6 border-t border-line pt-4 text-sm text-ink-2">
        This store doesn&rsquo;t keep the counts the Overview needs. The SQLite and Postgres stores do.
        {p.unsupportedHref && <> <a href={p.unsupportedHref} className={link}>About /api/flows →</a></>}
      </p>
    );
  } else if (s.kind === "error" && !s.staleAt) {
    main = (
      <p className="mt-6 border-t border-line pt-4 text-sm text-ink-2" role="alert">
        Couldn&rsquo;t load the flows: {s.message}.{p.onRetry && <> <button type="button" onClick={p.onRetry} className={link}>Try again</button></>}
      </p>
    );
  } else if (active.length === 0) {
    main = (
      <>
        <div className="mt-6 border-t border-line pt-4 text-sm text-ink-2">
          <p>No send or receive on any queue {range.during}.</p>
          {p.widen && p.widen.length > 0 && <p className="mt-2 flex gap-4">{p.widen.map((w) => <Link key={w.href} href={w.href} className={link}>{w.label}</Link>)}</p>}
        </div>
        {idle.length > 0 && table(idle.map((f) => row(f, true)))}
      </>
    );
  } else {
    main = (
      <>
        {active.length > FILTER_FROM && p.onQuery && (
          <input type="search" aria-label="Filter queues" placeholder="Filter queues" value={p.query ?? ""} onChange={(e) => p.onQuery!(e.target.value)}
            className="mt-4 w-64 rounded border border-line bg-surface px-3 py-1.5 text-sm text-ink placeholder:text-ink-3" />
        )}
        {table(
          <>
            {paged.map((f) => row(f, false))}
            {shown.length === 0 && <tr className="border-t border-line"><td colSpan={nCols} className="px-3 py-3 text-ink-3">No queue or service matches &ldquo;{p.query}&rdquo;.</td></tr>}
            {p.showIdle && idleShown.length > 0 && <tr aria-hidden className="border-t-2 border-line"><td colSpan={nCols} /></tr>}
            {p.showIdle && idleShown.map((f) => row(f, true))}
          </>,
        )}
        {shown.length > paged.length && (
          <button type="button" onClick={() => setMore(true)} className={`mt-3 text-sm ${link}`}>Show {fmtNum(shown.length - paged.length)} more</button>
        )}
        {!p.showIdle && idle.length > 0 && (
          <p className="mt-3"><button type="button" onClick={p.onShowIdle} className={`text-sm ${link}`}>{idleToggleText(idle.length, range.during)}</button></p>
        )}
      </>
    );
  }

  const live = s.kind === "ok" || (s.kind === "error" && !!s.staleAt);
  return (
    <div>
      {header}
      {slots.summary && <div className="mt-3 text-sm text-ink">{slots.summary}</div>}
      {live && active.length > 0 && <p className={`${slots.summary ? "mt-1" : "mt-3"} text-sm text-ink-2`}>{summaryText(summaryOf(flows), range.during)}</p>}
      {slots.banner && <div className="mt-3 border-t border-line pt-3 text-sm text-ink-2">{slots.banner}</div>}
      {main}
      {p.foot && <div className="mt-6 text-caption text-ink-3">{p.foot}</div>}
    </div>
  );
}
