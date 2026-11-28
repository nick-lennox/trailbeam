"use client";

import { useMemo, useState, type ReactNode } from "react";
import { Check, ChevronDown, ChevronRight, Search, X } from "lucide-react";
import { TimePicker } from "../explorer/TimePicker.tsx";
import { CAT, HoverCtx, OTHER, TimeSeries, type Line } from "../charts/TimeSeries.tsx";
import { Menu, MenuItem } from "../design/Menu.tsx";
import { UiLink } from "../design/nav.tsx";
import { STAGES, ago, byUrgency, severity, type Lifespan } from "../model/issues.ts";
import { MIN, PRESETS, bucket, custom, phrase, preset, rangeCovering, sameDay, stamp, type TimeRange } from "../model/timerange.ts";
import { InferredTag, Mark, RowBars, STATES, parts, type Tally } from "./parts.tsx";
import type { Row } from "./rows.ts";

/*
  The Issues list, as an explorer: the Event explorer's layout with issues in place of events.

  One time range drives the whole page. It opens wide enough to hold every message the page read.
  An issue is listed when one of its messages was last seen inside the range, and every count, chart and filter is of those messages only.

  1. A filter box, Open or Resolved, and the time range.
  2. Failing messages over the range, split by a field you choose. Dragging across it sets the
     time range to that span.
  3. Filters down the left, each value with the number of issues it would leave.
  4. One row per issue, with its messages drawn along the same time axis in every row, so
     issues that start together line up.

  Moved from the hosted app (QV-583). Rows come from issueRow (rows.ts).
*/

/* A row as the time range sees it. */
type View = Row & { n: number; here: Tally; cells: Tally[]; isNew: boolean };

const defaultHref = (id: string) => `/issues/${encodeURIComponent(id)}`;
const COLS = "grid-cols-[minmax(12.5rem,1fr)_8.5rem_4.5rem_minmax(8rem,0.55fr)_4.75rem_4.75rem]";
const ROW_BARS = 40; // the most bars a row draws
const zero = (): Tally => ({ dlq: 0, open: 0, done: 0 });
const total = (c: Tally) => c.dlq + c.open + c.done;
const plural = (n: number, one: string) => `${n.toLocaleString("en-US")} ${n === 1 ? one : `${one}s`}`;
const stageOf = (r: Row) => STAGES.find((s) => s.id === r.stage)!.label;

type Facet = { id: string; label: string; mono?: boolean; of: (v: View) => string[] };
const FACETS: Facet[] = [
  { id: "state", label: "Messages now", of: (v) => STATES.filter((s) => v.here[s.id] > 0).map((s) => s.label) },
  { id: "first", label: "First seen", of: (v) => [v.isNew ? "In this time range" : "Before this time range"] },
  { id: "stage", label: "Stops at", of: (v) => [stageOf(v)] },
  { id: "queue", label: "Queue", mono: true, of: (v) => [v.queue] },
  { id: "consumer", label: "Consumer", mono: true, of: (v) => v.consumers },
  { id: "producer", label: "Producer", mono: true, of: (v) => v.producers },
  { id: "tenant", label: "Tenant", mono: true, of: (v) => v.tenantNames },
];

/* What the chart can be split by. A split that is also a filter makes its legend a control. */
const GROUPS: { id: string; label: string; facet?: string; of: (v: View) => string }[] = [
  { id: "state", label: "Messages now", facet: "state", of: () => "" },
  { id: "issue", label: "Issue", of: (v) => v.title },
  { id: "queue", label: "Queue", facet: "queue", of: (v) => v.queue },
  { id: "stage", label: "Stops at", facet: "stage", of: stageOf },
];

const SORTS = {
  /* an issue that failed again after it was resolved or ignored comes first */
  worst: { name: "Worst first", by: (a: View, b: View) => Number(b.regressed) - Number(a.regressed) || byUrgency(a, b) },
  count: { name: "Most messages first", by: (a: View, b: View) => b.n - a.n },
  receives: { name: "Most receives first", by: (a: View, b: View) => b.maxReceives - a.maxReceives },
  first: { name: "Newest first", by: (a: View, b: View) => b.firstSeen - a.firstSeen },
  recent: { name: "Latest seen first", by: (a: View, b: View) => b.lastSeen - a.lastSeen },
};
type Sort = keyof typeof SORTS;
type Picked = Record<string, string[]>;

/*
  The page passes in what is the host's own, as slots: the status filters (triage's Open, Assigned to me and so on,
  kept in the link), and each row's triage tag and actions (triage, alert, copy link). An empty list says
  why from emptyText, which follows the status filter.
*/
export function IssueList({ rows, now, filters, filterName, emptyText, tags = {}, actions = {}, hrefs = {}, issueHref = defaultHref, defaultRange, initialRange = null, initialQueue = null, note }: {
  rows: Row[];
  now: number;
  filters?: ReactNode;
  /* the status filter's name, for the list's label: "Open", "Resolved" */
  filterName: string;
  emptyText: { title: string; body: string };
  tags?: Record<string, ReactNode>;
  actions?: Record<string, ReactNode>;
  /* each row's link, by issue ID, so it keeps the page's status, age and queue; issueHref's when not given */
  hrefs?: Record<string, string>;
  /* an issue's page, for a row hrefs leaves out: /issues/<id> when not given */
  issueHref?: (id: string) => string;
  defaultRange: string;
  /* the range the page's link named (?range=, or ?from= and ?to=), opened on in place of defaultRange */
  initialRange?: TimeRange | null;
  initialQueue?: string | null;
  note?: string;
}) {
  /* the time picker's ranges end at the sample data's clock; these end at this page's */
  const nowHere = now;
  const anchor = (t: TimeRange): TimeRange => (t.preset ? { ...t, from: nowHere - (t.to - t.from), to: nowHere } : t);
  /* it opens on defaultRange, widened to reach the oldest message, so every issue the counts above include is listed (QV-620) */
  const [time, setTimeState] = useState<TimeRange>(() => {
    if (initialRange) return anchor(initialRange);
    const oldest = rows.reduce<number | null>((o, r) => r.points.reduce((a, p) => (a === null || p.at < a ? p.at : a), o), null);
    return rangeCovering(defaultRange, oldest, nowHere);
  });
  const setTime = (t: TimeRange) => setTimeState(anchor(t));
  const [picked, setPicked] = useState<Picked>(initialQueue ? { queue: [initialQueue] } : {});
  const [text, setText] = useState("");
  const [sort, setSort] = useState<Sort>("worst");
  const [group, setGroup] = useState("state");
  const [closed, setClosed] = useState<string[]>([]);
  const [at, setAt] = useState<number | null>(null); // the moment under the pointer, on every chart

  const b = bucket(time);
  const merge = Math.ceil(b.n / ROW_BARS);

  /* each issue's messages inside the range, per interval */
  const inRange = useMemo(() => {
    const from = time.from, to = time.to;
    return rows.flatMap((r): View[] => {
      const cells = Array.from({ length: b.n }, zero), here = zero();
      for (const p of r.points) {
        if (p.at < from || p.at > to) continue;
        cells[Math.min(b.n - 1, Math.floor((p.at - from) / b.span))][p.state]++;
        here[p.state]++;
      }
      const n = total(here);
      return n ? [{ ...r, n, here, cells, isNew: r.firstSeen >= from && r.firstSeen <= to }] : [];
    });
  }, [rows, time.from, time.to, b.n, b.span]);

  const needle = text.trim().toLowerCase();
  const matches = (v: View, skip?: string) =>
    (!needle || [v.title, v.queue, ...v.consumers, ...v.producers, ...v.tenantNames].some((s) => s.toLowerCase().includes(needle))) &&
    FACETS.every((f) => f.id === skip || !picked[f.id]?.length || f.of(v).some((x) => picked[f.id].includes(x)));
  const shown = useMemo(() => inRange.filter((v) => matches(v)).sort(SORTS[sort].by), [inRange, needle, picked, sort]); // eslint-disable-line react-hooks/exhaustive-deps

  /* each filter's values, with the issues each would leave given the other filters */
  const facets = FACETS.map((f) => {
    const tally = new Map<string, number>((picked[f.id] ?? []).map((v) => [v, 0]));
    for (const v of inRange) if (matches(v, f.id)) for (const x of new Set(f.of(v))) tally.set(x, (tally.get(x) ?? 0) + 1);
    return { ...f, values: [...tally.entries()].sort((a, c) => c[1] - a[1] || a[0].localeCompare(c[0])) };
  }).filter((f) => f.values.length > 0);
  const on = Object.values(picked).reduce((a, v) => a + v.length, 0);
  const toggle = (facet: string, value: string) => {
    const cur = picked[facet] ?? [];
    setPicked({ ...picked, [facet]: cur.includes(value) ? cur.filter((x) => x !== value) : [...cur, value] });
  };

  /* the chart: failing messages per interval, one series per value of the chosen field */
  const g = GROUPS.find((x) => x.id === group)!;
  const lines: Line[] = useMemo(() => {
    const blank = Array.from({ length: b.n }, () => 0);
    if (g.id === "state") {
      const out = STATES.map((s) => ({ name: s.label, color: s.color, points: blank.map((_, i) => shown.reduce((a, v) => a + v.cells[i][s.id], 0)) })).filter((l) => l.points.some((p) => p > 0));
      return out.length ? out : [{ name: "Failing messages", color: OTHER, points: blank }];
    }
    const by = new Map<string, number[]>();
    for (const v of shown) {
      const pts = by.get(g.of(v)) ?? [...blank];
      v.cells.forEach((c, i) => { pts[i] += total(c); });
      by.set(g.of(v), pts);
    }
    const ranked = [...by.entries()].map(([name, points]) => ({ name, points, n: points.reduce((a, c) => a + c, 0) })).sort((a, c) => c.n - a.n);
    const top = ranked.slice(0, 4).map((l, i) => ({ name: l.name, color: CAT[i], points: l.points }));
    const rest = ranked.slice(4);
    if (rest.length) top.push({ name: `${rest.length} more`, color: OTHER, points: blank.map((_, i) => rest.reduce((a, l) => a + l.points[i], 0)) });
    return top.length ? top : [{ name: "Failing messages", color: OTHER, points: blank }];
  }, [shown, g, b.n]);
  const messages = shown.reduce((a, v) => a + v.n, 0);

  /* when the range holds nothing: the shortest common range that would */
  const latest = rows.length ? Math.max(...rows.flatMap((r) => r.points.map((p) => p.at))) : null;
  const wider = latest === null ? null : PRESETS.find((p) => p.ms > time.to - time.from && now - p.ms <= latest) ?? null;

  const dated = !sameDay(time.from, time.to);
  const clock = (ms: number) => stamp(ms, { date: dated, seconds: b.span * merge < MIN });
  const cellLabel = (i: number) => `${clock(time.from + i * merge * b.span)} to ${clock(Math.min(time.to, time.from + (i + 1) * merge * b.span))}`;
  const th = (id: Sort, name: string, cls = "") => (
    <button type="button" onClick={() => setSort(sort === id ? "worst" : id)} aria-pressed={sort === id} className={`inline-flex cursor-pointer items-center gap-1 whitespace-nowrap hover:text-ink ${sort === id ? "text-ink" : ""} ${cls}`}>
      {name}{sort === id && <ChevronDown size={12} aria-hidden />}
    </button>
  );

  return (
    <HoverCtx.Provider value={{ at, set: setAt }}>
      <div className="flex min-h-[calc(100vh-3.5rem)] flex-col [--row-actions:9.5rem]">
        {/* ── 1 and 2. filter, time range, and the chart ── */}
        <div className="grid gap-3 border-b border-line px-5 py-3">
          <div className="flex flex-wrap items-center gap-2">
            <label className="flex h-10 min-w-[240px] flex-1 items-center gap-2 rounded-lg border border-line-strong bg-surface px-3 text-[13px] focus-within:border-accent">
              <Search size={14} className="shrink-0 text-ink-3" aria-hidden />
              <input value={text} onChange={(e) => setText(e.target.value)} placeholder="Filter issues by cause, queue, service or tenant" aria-label="Filter issues" spellCheck={false} className="min-w-0 flex-1 bg-transparent text-ink !outline-none placeholder:text-ink-3" />
              {text && <button type="button" onClick={() => setText("")} aria-label="Clear" className="cursor-pointer text-ink-3 hover:text-ink"><X size={13} /></button>}
            </label>
            {filters}
            <TimePicker time={time} setTime={setTime} now={nowHere} />
          </div>

          <section aria-label="Failing messages over time">
            <div className="mb-1 flex h-6 items-center justify-between gap-3 text-xs text-ink-3">
              <span>Failing messages per {b.label}, by when each was last seen</span>
              <Menu width="w-44" button={(open, flip) => (
                <button type="button" onClick={flip} aria-expanded={open} aria-haspopup="menu" className="inline-flex cursor-pointer items-center gap-1.5 rounded-md border border-line-strong px-2 py-0.5 text-xs font-medium text-ink-2 hover:border-ink-3 hover:text-ink">
                  <span className="font-normal text-ink-3">Split by</span> {g.label} <ChevronDown size={12} className="text-ink-3" />
                </button>
              )}>
                {(close) => GROUPS.map((x) => (
                  <MenuItem key={x.id} onClick={() => { setGroup(x.id); close(); }}>
                    <span className="w-4">{x.id === group && <Check size={14} className="text-accent" />}</span>{x.label}
                  </MenuItem>
                ))}
              </Menu>
            </div>
            <div className="flex h-[150px]">
              <TimeSeries lines={lines} from={time.from} to={time.to} unit="count" kind="bars" legend={false} onZoom={(a, c) => setTime(custom(Math.floor(a / b.span) * b.span, Math.ceil(c / b.span) * b.span))} />
            </div>
            <div className="mt-1.5 flex min-h-5 flex-wrap items-center gap-x-4 gap-y-1 pl-10 text-xs text-ink-2">
              {messages > 0 && lines.map((l) => {
                const sum = l.points.reduce((a, c) => a + c, 0);
                const body = <><span className="size-2 shrink-0 rounded-[2px]" style={{ background: l.color }} /><span className={g.facet === "queue" ? "font-mono text-caption" : ""}>{l.name}</span><b className="font-mono font-medium text-ink">{sum}</b></>;
                const filters = g.facet && l.color !== OTHER;
                return filters
                  ? <button key={l.name} type="button" onClick={() => toggle(g.facet!, l.name)} aria-pressed={!!picked[g.facet!]?.includes(l.name)} className="inline-flex max-w-full cursor-pointer items-center gap-1.5 rounded-[5px] px-1 hover:bg-sunken aria-pressed:bg-sunken">{body}</button>
                  : <span key={l.name} className="inline-flex max-w-full items-center gap-1.5 px-1">{body}</span>;
              })}
            </div>
          </section>
        </div>

        <div className="grid flex-1 md:grid-cols-[236px_minmax(0,1fr)]">
          {/* ── 3. filters ── */}
          <aside className="border-b border-line text-[13px] md:border-r md:border-b-0" aria-label="Filters">
            <p className="flex h-[33px] items-center justify-between border-b border-line px-3 text-xs text-ink-3">
              <span>{on === 0 ? "No filters on" : <><b className="font-mono font-medium text-accent">{on}</b> {on === 1 ? "filter" : "filters"} on</>}</span>
              {on > 0 && <button type="button" onClick={() => setPicked({})} className="cursor-pointer text-brand hover:underline">Clear all</button>}
            </p>
            <div className="grid gap-0.5 px-3 py-2">
              {facets.map((f) => {
                const open = !closed.includes(f.id);
                const active = picked[f.id]?.length ?? 0;
                return (
                  <div key={f.id}>
                    <button type="button" onClick={() => setClosed(open ? [...closed, f.id] : closed.filter((x) => x !== f.id))} aria-expanded={open} className="-mx-1.5 flex w-[calc(100%+12px)] cursor-pointer items-center gap-1 rounded-[5px] px-1.5 py-1 text-left hover:bg-sunken">
                      {open ? <ChevronDown size={12} className="shrink-0 text-ink-3" /> : <ChevronRight size={12} className="shrink-0 text-ink-3" />}
                      <span className={`min-w-0 truncate text-xs font-medium ${active ? "text-ink" : "text-ink-2"}`}>{f.label}</span>
                      {active > 0 && <span className="size-1.5 shrink-0 rounded-full bg-accent" />}
                      <span className="ml-auto pl-1 font-mono text-micro text-ink-3">{f.values.length}</span>
                    </button>
                    {open && (
                      <div className="mb-1.5 pl-3.5">
                        {f.values.map(([v, n]) => (
                          <label key={v} className="-mx-1.5 flex cursor-pointer items-center gap-2 rounded-[5px] px-1.5 py-[3px] hover:bg-sunken">
                            <input type="checkbox" checked={!!picked[f.id]?.includes(v)} onChange={() => toggle(f.id, v)} className="size-3.5 shrink-0 accent-brand" />
                            <span className={`min-w-0 flex-1 truncate text-ink ${f.mono ? "font-mono text-caption" : "text-[13px]"}`}>{v}</span>
                            <span className="font-mono text-micro tabular-nums text-ink-3">{n}</span>
                          </label>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
              {facets.length === 0 && <p className="py-2 text-[13px] text-ink-2">Nothing to filter in {phrase(time)}.</p>}
            </div>
          </aside>

          {/* ── 4. one row per issue ── */}
          <section className="flex min-w-0 flex-col bg-canvas" aria-label={`${filterName} issues`}>
            <div className="min-w-0 flex-1 overflow-x-auto">
              <div className={`grid min-w-[800px] ${COLS} h-[33px] items-center gap-x-3 border-b border-line bg-surface pr-[var(--row-actions)] pl-4 text-xs font-medium text-ink-3`}>
                {th("worst", "Issue")}{th("count", "Messages")}{th("receives", "Receives")}
                <span className="flex justify-between font-mono text-micro font-normal tabular-nums"><span className="sr-only">Failing messages from </span><span className="whitespace-nowrap">{clock(time.from)}</span><span className="whitespace-nowrap">{time.preset ? <span className="font-sans">Now</span> : clock(time.to)}</span></span>
                {th("first", "First seen", "justify-self-end")}{th("recent", "Last seen", "justify-self-end")}
              </div>
              {shown.length ? (
                <ul className="min-w-[800px]">
                  {shown.map((v) => {
                    const stand = parts({ count: v.n, inDlq: v.here.dlq, notFinished: v.here.open });
                    const atMax = v.maxReceiveCount !== null && v.maxReceives >= v.maxReceiveCount;
                    const cells = Array.from({ length: Math.ceil(b.n / merge) }, (_, i) => v.cells.slice(i * merge, (i + 1) * merge).reduce((a, c) => ({ dlq: a.dlq + c.dlq, open: a.open + c.open, done: a.done + c.done }), zero()));
                    return (
                      <li key={v.id} className="group flex items-center border-b border-line hover:bg-sunken/60">
                        <UiLink href={hrefs[v.id] ?? issueHref(v.id)} className={`grid min-w-0 flex-1 ${COLS} items-center gap-x-3 py-2 pl-4`}>
                          <span className="min-w-0">
                            <span className="flex items-center gap-2">
                              <Mark tone={v.status === "resolved" ? "good" : severity(v)} />
                              <span className={`truncate text-[13px] font-medium ${v.status === "open" ? "text-ink" : "text-ink-2"}`}>{v.title}</span>
                            </span>
                            <span className="mt-1 flex items-center gap-1.5 pl-4 text-xs text-ink-3">
                              {tags[v.id]}
                              {v.inferred && <InferredTag />}
                              <span className="min-w-0 truncate">
                                <span className="font-mono">{v.queue}</span>
                                {v.consumers.length > 0 && <>, received by {v.consumers.join(", ")}</>}
                              </span>
                            </span>
                          </span>
                          <span className="min-w-0 truncate text-xs text-ink-3">
                            <span className="mr-2 font-mono text-[13px] font-medium tabular-nums text-ink">{v.n}</span>
                            {stand.map((p, i) => <span key={p.key}>{i > 0 && ", "}{stand.length > 1 ? `${p.n} ` : v.n > 1 ? "all " : ""}{p.short}</span>)}
                          </span>
                          <span className={`font-mono text-[13px] tabular-nums ${atMax ? "text-critical-text" : "text-ink-2"}`}>{v.maxReceiveCount !== null ? `${v.maxReceives} of ${v.maxReceiveCount}` : v.maxReceives}</span>
                          <RowBars cells={cells} at={at} onAt={setAt} label={cellLabel} />
                          <span className="text-right text-xs whitespace-nowrap text-ink-2">{ago(v.firstSeen, now)}</span>
                          <span className="text-right text-xs whitespace-nowrap text-ink-2">{ago(v.lastSeen, now)}</span>
                        </UiLink>
                        <div className="flex w-[var(--row-actions)] shrink-0 items-center justify-end pr-2">{actions[v.id]}</div>
                      </li>
                    );
                  })}
                </ul>
              ) : (
                <div className="px-5 py-14 text-center">
                  {rows.length === 0 ? (
                    <>
                      <div className="font-medium text-ink">{emptyText.title}</div>
                      <p className="mx-auto mt-1 max-w-[60ch] text-sm text-ink-2">{emptyText.body}</p>
                    </>
                  ) : inRange.length === 0 ? (
                    <>
                      <div className="font-medium text-ink">No failing message was seen in {phrase(time)}</div>
                      <p className="mx-auto mt-1 max-w-[60ch] text-sm text-ink-2">{plural(rows.length, `${filterName.toLowerCase()} issue`)} last had a failing message {ago(latest!, now)}.</p>
                      {wider && <button type="button" onClick={() => setTime(preset(wider.id, nowHere))} className="mt-3 h-8 cursor-pointer rounded-lg border border-line-strong bg-surface px-3 text-[13px] font-medium text-ink hover:border-ink-3">Show the {wider.label.toLowerCase()}</button>}
                    </>
                  ) : (
                    <>
                      <div className="font-medium text-ink">No issue matches these filters</div>
                      <button type="button" onClick={() => { setPicked({}); setText(""); }} className="mt-3 h-8 cursor-pointer rounded-lg border border-line-strong bg-surface px-3 text-[13px] font-medium text-ink hover:border-ink-3">Clear the filters</button>
                    </>
                  )}
                </div>
              )}
            </div>
            <div className="border-t border-line bg-surface px-4 py-2 text-[13px] text-ink-3">
              <p className="flex items-center justify-between gap-3">
                <span>
                  {shown.length !== inRange.length && <><b className="font-mono font-medium text-ink">{shown.length}</b> of </>}
                  <b className="font-mono font-medium text-ink">{inRange.length}</b> {inRange.length === 1 ? "issue" : "issues"} with <b className="font-mono font-medium text-ink">{messages.toLocaleString("en-US")}</b> failing {messages === 1 ? "message" : "messages"} in {phrase(time)}
                </span>
                <span>{SORTS[sort].name}</span>
              </p>
              {note && <p className="mt-1 text-xs">{note}</p>}
            </div>
          </section>
        </div>
      </div>
    </HoverCtx.Provider>
  );
}
