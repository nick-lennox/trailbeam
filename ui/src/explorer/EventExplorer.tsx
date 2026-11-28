"use client";

/*
  The Event explorer: a query bar, a chart of the events over the range, filters with their counts, and the events
  themselves, newest first. Both products draw it. The host hands it the range's events (Ev, model/events.ts: each
  message's timeline through eventsOfTimeline, so the inferred events are there too) and keeps the query and the time
  range, usually in the URL. The query string is the one source of truth: the filters and the chart edit it, and the
  list and the chart read it.

  The hosted app loads a workspace's events from its stored spans, or the sample stream; self-hosted Trailbeam reads
  GET /api/timeline-events for the chosen range. A column or filter that no event in the range fills is left out, and
  Columns and Export sit in the results bar over the list.
*/
import { Fragment, useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import { ChevronDown, ChevronRight, Clock, Search, X } from "lucide-react";
import { SOURCE } from "../design/pills.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { EVENT_NAME, fmtDate, type Ev } from "../model/events.ts";
import { bodyPathsOf, has, GROUPS, type FieldDef, type Group, type QueryLanguage } from "../model/query.ts";
import { bucket, custom, phrase, rangeLabel, type TimeRange } from "../model/timerange.ts";
import { EventChart, type Seg } from "./EventChart.tsx";
import { EventDetail } from "./EventDetail.tsx";
import { DEFAULT_COLUMNS, LEVEL_COLOR, RECORDED_LANGUAGE, SPECIAL, TD, TH, cellText, colLabel, columnChoices, filledColumns, level } from "./columns.ts";
import { download as browserDownload, toCsv, toPdf } from "./export.ts";
import { ColumnChoices, ColumnsButton, ColumnsPanel, ExportMenu, LevelMark, QueryInput, eventCell } from "./parts.tsx";
import { cutShort, type StepChart } from "./stepChart.ts";

/** The events the explorer reads, and where the ranges end. */
export type ExplorerData = {
  events: Ev[];
  /** where a range that ends now ends; a new value reloads the step chart */
  now: number;
  /** the events were recorded (a workspace's own, or self-hosted Trailbeam's), not the hosted sample: fields only the sample has are not offered */
  live: boolean;
  /** more events were in the range than were loaded, so the oldest are missing */
  capped: boolean;
  /** "shown": bodies came with the events (bodiesCapped: some were left out to keep the page small); "admins": the reader may not see them */
  bodies?: "shown" | "admins";
  bodiesCapped?: boolean;
};

export type EventExplorerProps = {
  query: string;
  setQuery: (q: string) => void;
  time: TimeRange;
  /** the page's time range control (its TimePicker's setTime): a span picked on the chart becomes the page's range.
      Without it, the chart's bars only filter to failed or ok events. */
  setTime?: (t: TimeRange) => void;
  data: ExplorerData;
  /** a message's timeline page */
  messageHref: (id: string) => string;
  /** the query language; by default over what the events recorded (attrs, body) */
  lang?: QueryLanguage;
  /** the body paths to suggest while typing; by default the paths in the events' bodies */
  bodyPaths?: string[];
  /** fill the screen with no card around it */
  page?: boolean;
  /** beside the query bar: the time picker, Copy link, live or refresh */
  controls?: ReactNode;
  /** an opened row; EventDetail by default */
  detail?: (e: Ev) => ReactNode;
  /** every recorded step in [from, to), counted by the server, for when the loaded events miss part of the range */
  loadStepChart?: (from: number, to: number) => Promise<StepChart | null>;
  /** how a file reaches the person; a browser download by default */
  download?: (fileName: string, mime: string, data: BlobPart) => void;
  /** the localStorage key the chosen columns are kept under */
  columnsKey?: string;
};

/* The events section of the Overview: a query bar, a histogram, facets and the
   event list. The query string is the one source of truth; facets and the map
   edit it, and the list and histogram read it. */

/* Every field with few enough values to tick is a facet, on recorded events only the fields they can have. The first
   group is open; the attribute groups start closed so the panel is short until someone goes looking. */
const facetsFor = (lang: QueryLanguage, live: boolean): FieldDef[] => lang.fieldsFor(live).filter((f) => f.facet);
const OPEN_BY_DEFAULT = ["status", "event", "queue", "service", "source", "body_size", "processing_time"];

const PAGE = 100;

export function EventExplorer({ query, setQuery, time, setTime, data, messageHref, lang = RECORDED_LANGUAGE, bodyPaths: givenPaths, page = false, controls, detail,
  loadStepChart, download = browserDownload, columnsKey = "trailbeam-event-columns-3" }: EventExplorerProps) {
  const { def, parse, matches, ranges, token, toggle, setOnly } = lang;
  const [open, setOpen] = useState<string | null>(null);
  const [columns, setColumnsState] = useState<string[]>(DEFAULT_COLUMNS);
  const [picker, setPicker] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [find, setFind] = useState("");
  const [openGroups, setOpenGroups] = useState<Group[]>(["Event"]);
  const [openFacets, setOpenFacets] = useState<string[]>(OPEN_BY_DEFAULT);
  /* filters whose full list of values is shown; the rest show the eight most common */
  const [allValues, setAllValues] = useState<string[]>([]);
  /* The chosen columns are remembered in this browser. */
  useEffect(() => {
    try {
      const saved = JSON.parse(localStorage.getItem(columnsKey) ?? "null");
      if (Array.isArray(saved) && saved.length && saved.every((c) => typeof c === "string" && (c in SPECIAL || def(c)))) setColumnsState(saved);
    } catch {}
  }, []);
  const setColumns = (c: string[]) => {
    setColumnsState(c);
    try { localStorage.setItem(columnsKey, JSON.stringify(c)); } catch {}
  };
  const closePicker = useCallback(() => setPicker(false), []);

  const { events, capped, live, bodies, bodiesCapped, now } = data;
  const bodyPaths = useMemo(() => givenPaths ?? bodyPathsOf(events.flatMap((e) => (e.body === undefined ? [] : [e.body]))), [events, givenPaths]);
  const terms = useMemo(() => parse(query, live), [parse, query, live]);

  const { from, to } = time;
  const { span, n: bars, label: per } = bucket(time);
  const inRange = useMemo(() => events.filter((e) => e.ms >= from && e.ms < to), [events, from, to]);
  const matched = useMemo(() => inRange.filter((e) => matches(e, terms)), [inRange, terms, matches]);

  /* The columns and filters some event in the range fills: the others are left out of the table, the column list and
     the filters (a filter a query names stays, so it can be cleared). */
  const filled = useMemo(() => filledColumns([...columnChoices(lang, live).flatMap((g) => g.cols.map((c) => c.id)), ...lang.fieldsFor(live).map((f) => f.name)], inRange, lang), [inRange, lang, live]);
  const shownCols = useMemo(() => { const c = columns.filter((x) => filled.has(x)); return c.length ? c : ["date", "event"]; }, [columns, filled]);
  const rows = matched;

  /* Export: every event that matches the query and the time range, not only the page on
     screen, with the columns that are shown. Values are written in full. */
  const exportAs = (kind: "csv" | "pdf") => {
    const text = (c: string, e: Ev) => cellText(c, e, lang, (s) => SOURCE[s]?.label ?? s);
    const table = { headers: shownCols.map((c) => colLabel(c, lang)), rows: rows.map((e) => shownCols.map((c) => text(c, e))) };
    const span = rangeLabel(from, to);
    const name = `trailbeam-events-${fmtDate(to).slice(0, 6).replace(" ", "-").toLowerCase()}`;
    if (kind === "csv") return download(`${name}.csv`, "text/csv;charset=utf-8", toCsv(table));
    const lines = [`Query: ${query.trim() || "none (all events)"}`, `Time range: ${span}`, `${rows.length} ${rows.length === 1 ? "event" : "events"}, newest first`];
    download(`${name}.pdf`, "application/pdf", toPdf(table, "Trailbeam events", lines));
  };

  const facets = useMemo(
    () =>
      facetsFor(lang, live).filter((f) => filled.has(f.name) || terms.some((t) => t.field === f.name)).map((f) => {
        const counts = new Map<string, number>();
        for (const e of inRange) {
          if (!matches(e, terms, f.name)) continue;
          const raw = f.get(e);
          if (raw === undefined) continue;
          /* a measured quantity is counted by the range it falls in */
          const v = f.buckets ? ranges(f).find((r) => (raw as number) >= r.lo && (raw as number) < r.hi)!.value : token(f.name, raw);
          counts.set(v, (counts.get(v) ?? 0) + 1);
        }
        for (const t of terms) if (!t.neg && t.field === f.name && t.value && !counts.has(t.value)) counts.set(t.value, 0);
        const numeric = f.kind !== "text";
        const order = f.buckets ? ranges(f).map((r) => r.value) : null;
        const values = [...counts].sort((a, b) => (order ? order.indexOf(a[0]) - order.indexOf(b[0]) : numeric ? parseFloat(a[0]) - parseFloat(b[0]) : b[1] - a[1]));
        return { ...f, values, active: terms.filter((t) => !t.neg && t.field === f.name && t.value).length };
      })
      /* a facet with no value to tick is not drawn, nor a group with none (QV-777); one the query names still is */
      .filter((f) => f.values.length > 0 || terms.some((t) => t.field === f.name)),
    [inRange, terms, live, lang, filled],
  );
  const activeTotal = terms.filter((t) => (t.field || t.value) && !t.error).length;
  const needle = find.trim().toLowerCase();
  /* While searching, show only facets whose name or values match, and open them all. */
  const shown = (f: (typeof facets)[number]) => !needle || f.label.toLowerCase().includes(needle) || f.name.toLowerCase().includes(needle) || f.values.some(([v]) => v.toLowerCase().includes(needle));
  const flip = <T,>(list: T[], x: T) => (list.includes(x) ? list.filter((y) => y !== x) : [...list, x]);

  /* One bar per minute, counted from the events that match the query. */
  const buckets = useMemo(() => {
    const n = bars;
    const b = Array.from({ length: n }, () => ({ ok: 0, err: 0 }));
    for (const e of matched) b[Math.min(n - 1, Math.floor((e.ms - from) / span))][e.status === "error" ? "err" : "ok"]++;
    return b;
  }, [matched, from, span, bars]);
  /* When the loaded events miss part of the range, an empty query's chart comes from every recorded step in it, counted
     by the server (stepChart.ts). The list and its count still come from the loaded events. */
  const wantSteps = !!loadStepChart && !activeTotal && cutShort(events, capped, from);
  const [steps, setSteps] = useState<StepChart | null>(null);
  useEffect(() => {
    if (!wantSteps || !loadStepChart) { setSteps(null); return; }
    let live = true;
    loadStepChart(from, to).then((c) => { if (live) setSteps(c); }, () => {});
    return () => { live = false; };
  }, [wantSteps, from, to, now]);
  const chart = wantSteps ? steps : null;
  /* A span picked on the chart is the page's range from then on: one range per page, the time picker's. */
  const pick = (w: [number, number], seg?: Seg) => {
    setOpen(null);
    if (setTime && (w[0] !== from || w[1] !== to)) setTime(custom(w[0], w[1]));
    if (seg) setQuery(setOnly(query, ["status"], "status", seg === "err" ? "error" : "ok"));
  };

  const th = TH;
  const td = TD;

  return (
    <div className={page ? "flex min-h-0 flex-1 flex-col" : "mt-8 overflow-hidden rounded-xl border border-line bg-surface"}>
      <div className={`grid gap-3 border-b border-line ${page ? "px-5 py-3" : "p-4"}`}>
        <div className="flex items-start gap-3">
        <QueryInput
          query={query}
          setQuery={setQuery}
          live={live}
          lang={lang}
          values={(f) => facets.find((x) => x.name === f)?.values}
          right={!page && (
            <span className="inline-flex h-7 items-center gap-1.5 border-l border-line pl-3 pr-1 font-sans text-[13px] whitespace-nowrap text-ink-2">
              <Clock size={14} /> {time.label}
            </span>
          )}
          bodyPaths={bodyPaths}
          note={terms.some((t) => t.field && /^body(\.|$)/.test(t.field)) && (
            <p className="mt-1.5 font-sans text-[13px] text-ink-3">
              {bodies === "admins"
                ? "Bodies are shown to owners and admins only, so body filters match nothing here."
                : <>Body filters match only messages whose queue has payload capture on, and search the body as stored, after redaction.{live && " A match is the event that carried the body: the send, or a receive when the producer runs no Trailbeam package."}{bodiesCapped && " Bodies of the oldest messages were left out to keep this page small."}</>}
            </p>
          )}
        />
        {controls}
        </div>

        {chart
          ? <EventChart steps buckets={chart.buckets} from={chart.from} to={to} span={chart.span} per={chart.per} onPick={(w) => pick(w)} />
          : <EventChart buckets={buckets} from={from} to={to} span={span} per={per} onPick={pick} />}
      </div>

      <div className={`grid md:grid-cols-[236px_minmax(0,1fr)] ${page ? "min-h-0 flex-1 md:grid-rows-[minmax(0,1fr)]" : ""}`}>
        <aside className={`grid grid-cols-[minmax(0,1fr)] content-start border-b border-line text-[13px] md:border-r md:border-b-0 ${page ? "[scrollbar-color:var(--color-line-strong)_transparent] [scrollbar-width:thin] md:overflow-x-hidden md:overflow-y-auto" : ""}`}>
          {/* Find a filter by name or value, and see at a glance how many are on. */}
          <div className="sticky top-0 z-[1] grid gap-2 border-b border-line bg-surface px-3 py-3">
            <label className="flex items-center gap-2 rounded-md border border-line-strong bg-canvas px-2 py-1 focus-within:border-accent">
              <Search size={13} className="shrink-0 text-ink-3" />
              <input value={find} onChange={(e) => setFind(e.target.value)} placeholder="Find a filter" aria-label="Find a filter" className="w-full bg-transparent text-[13px] text-ink !outline-none placeholder:text-ink-3" />
              {find && <button onClick={() => setFind("")} aria-label="Clear" className="text-ink-3 hover:text-ink"><X size={12} /></button>}
            </label>
            <p className="flex items-center justify-between text-xs text-ink-3">
              <span>{activeTotal === 0 ? "No filters on" : <><b className="font-mono font-medium text-accent">{activeTotal}</b> {activeTotal === 1 ? "filter" : "filters"} on</>}</span>
              {activeTotal > 0 && <button onClick={() => { setOpen(null); setQuery(""); }} className="text-brand hover:underline">Clear all</button>}
            </p>
          </div>

          {GROUPS.map((g) => {
            const inGroup = facets.filter((f) => f.group === g && shown(f));
            if (!inGroup.length) return null;
            const on = facets.filter((f) => f.group === g).reduce((a, f) => a + f.active, 0);
            const groupOpen = !!needle || openGroups.includes(g) || on > 0;
            return (
              <section key={g} className="border-b border-line last:border-b-0">
                <button onClick={() => setOpenGroups(flip(openGroups, g))} aria-expanded={groupOpen} className="flex w-full items-center gap-1.5 px-3 py-2.5 text-left hover:bg-sunken/60">
                  {groupOpen ? <ChevronDown size={14} className="text-ink-3" /> : <ChevronRight size={14} className="text-ink-3" />}
                  <span className="text-[13px] font-semibold text-ink">{g}</span>
                  {on > 0 && <span className="rounded-full bg-accent/15 px-1.5 font-mono text-micro font-medium text-accent">{on}</span>}
                  <span className="ml-auto font-mono text-micro text-ink-3">{inGroup.length}</span>
                </button>
                {groupOpen && (
                  <div className="grid grid-cols-[minmax(0,1fr)] gap-0.5 px-3 pb-3">
                    {inGroup.map((f) => {
                      const facetOpen = !!needle || openFacets.includes(f.name) || f.active > 0;
                      const attribute = g !== "Event";
                      return (
                        <div key={f.name}>
                          <button onClick={() => setOpenFacets(flip(openFacets, f.name))} aria-expanded={facetOpen} className="-mx-1.5 flex w-[calc(100%+12px)] items-center gap-1 rounded-[5px] px-1.5 py-1 text-left hover:bg-sunken">
                            {facetOpen ? <ChevronDown size={12} className="shrink-0 text-ink-3" /> : <ChevronRight size={12} className="shrink-0 text-ink-3" />}
                            <Tooltip content={attribute ? f.name : undefined} className={`min-w-0 truncate text-xs font-medium ${f.active ? "text-ink" : "text-ink-2"}`}>{f.label}</Tooltip>
                            {f.active > 0 && <span className="size-1.5 shrink-0 rounded-full bg-accent" />}
                            <span className="ml-auto pl-1 font-mono text-micro text-ink-3">{f.values.length}</span>
                          </button>
                          {facetOpen && (
                            <div className="mb-1.5 pl-3.5">
                              {(allValues.includes(f.name) ? f.values : f.values.slice(0, 8)).map(([v, n]) => {
                                const ticked = has(terms, f.name, v);
                                return (
                                  <label key={v} className="-mx-1.5 flex cursor-pointer items-center gap-2 rounded-[5px] px-1.5 py-[3px] hover:bg-sunken">
                                    <input type="checkbox" checked={ticked} onChange={() => { setOpen(null); setQuery(toggle(query, f.name, v)); }} className="size-3.5 shrink-0 accent-brand" />
                                    <Tooltip className={`min-w-0 flex-1 truncate text-ink ${f.name === "event" || f.name === "source" || f.buckets ? "font-sans text-[13px]" : "font-mono text-caption"}`} content={f.name === "event" || f.buckets ? `${f.name}:${v}` : undefined}>{f.name === "source" ? SOURCE[v as Ev["source"]]?.label ?? v : f.name === "event" ? EVENT_NAME[v as Ev["event"]] ?? v : f.buckets ? ranges(f).find((r) => r.value === v)?.label ?? v : v}</Tooltip>
                                    <span className="font-mono text-micro tabular-nums text-ink-3">{n}</span>
                                  </label>
                                );
                              })}
                              {f.values.length > 8 && (
                                <button type="button" onClick={() => setAllValues(flip(allValues, f.name))} aria-expanded={allValues.includes(f.name)} className="-mx-1.5 mt-0.5 rounded-[5px] px-1.5 py-1 text-xs font-medium text-brand hover:bg-sunken">
                                  {allValues.includes(f.name) ? "Show fewer" : `Show ${f.values.length - 8} more`}
                                </button>
                              )}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </section>
            );
          })}
          {needle && !facets.some(shown) && <p className="px-3 py-4 text-[13px] text-ink-2">No filter matches &ldquo;{find}&rdquo;.</p>}
        </aside>


        <div className="relative flex min-h-0 min-w-0 flex-col">
          {/* The results bar: how many events match, and what to do with them. */}
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 border-b border-line bg-surface px-4 py-2 text-[13px] text-ink-3" data-results-bar>
            <p className="min-w-0 flex-1">
              {rows.length > PAGE ? <>Showing the newest <b className="font-mono font-medium text-ink">{PAGE}</b> of </> : null}
              <b className="font-mono font-medium text-ink">{rows.length.toLocaleString("en-US")}</b> {rows.length === 1 ? "event" : "events"}, newest first
              {capped && <>. Only the most recent messages are loaded, so older events in this range are missing</>}
              {chart && <>. The chart counts all <b className="font-mono font-medium text-ink">{chart.total.toLocaleString("en-US")}</b> recorded steps in this range</>}
            </p>
            <div className="flex shrink-0 items-center gap-1.5">
              <ColumnsButton open={picker} onClick={() => { setExporting(false); setPicker(!picker); }} />
              <ExportMenu open={exporting} setOpen={(o) => { setPicker(false); setExporting(o); }} disabled={rows.length === 0} formats={["csv", "pdf"]} onExport={exportAs}
                what={<>{rows.length.toLocaleString("en-US")} {rows.length === 1 ? "event" : "events"} match, with the columns shown</>} />
            </div>
          </div>
          {picker && (
            <ColumnsPanel shown={shownCols.length} onReset={() => setColumns(DEFAULT_COLUMNS)} onClose={closePicker} note="Columns that no event in this range fills are left out.">
              <ColumnChoices columns={columns} setColumns={setColumns} live={live} lang={lang} only={filled} />
            </ColumnsPanel>
          )}
          <div className={`min-h-0 min-w-0 flex-1 overflow-auto [container-type:inline-size] ${page ? "bg-canvas" : "max-h-[640px]"}`}>
            <table className="w-full border-collapse font-mono text-caption" data-events>
              <thead className="sticky top-0 z-[1] bg-surface">
                <tr>
                  {shownCols.map((c) => <th key={c} className={th}><Tooltip content={c in SPECIAL ? undefined : c}>{colLabel(c, lang)}</Tooltip></th>)}
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, PAGE).map((e) => {
                  const sel = open === e.key;
                  const cell = `${td} ${sel ? "bg-brand-soft" : "group-hover:bg-sunken"}`;
                  return (
                    <Fragment key={e.key}>
                      <tr
                        className="ev-row group cursor-pointer"
                        style={{ ["--bar" as string]: LEVEL_COLOR[level(e)] }}
                        tabIndex={0}
                        aria-expanded={sel}
                        onClick={() => setOpen(sel ? null : e.key)}
                        onKeyDown={(k) => { if (k.key === "Enter" || k.key === " ") { k.preventDefault(); setOpen(sel ? null : e.key); } }}
                      >
                        {shownCols.map((c, i) => eventCell(c, e, cell, sel, i === 0 ? <LevelMark e={e} /> : undefined, lang))}
                      </tr>
                      {sel && (
                        <tr>
                          <td colSpan={shownCols.length} className={`border-b border-line p-0 ${page ? "bg-surface [--qs-ground:var(--color-surface)]" : "bg-canvas"}`}>
                            <div className="sticky left-0 w-[100cqw] px-5 py-4">
                              {detail ? detail(e) : <EventDetail event={e} messageHref={messageHref} />}
                            </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
                {rows.length === 0 && (
                  <tr><td colSpan={shownCols.length} className="px-6 py-10 text-center font-sans text-sm text-ink-2">No events match this query in {phrase(time)}.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
