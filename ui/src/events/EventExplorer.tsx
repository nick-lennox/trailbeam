/*
  The Event explorer: a query bar, a chart of the recorded steps over the range, the kinds with their counts, and the
  steps themselves, newest first, a page at a time. Everything comes from GET /api/events through `load`, so the view
  runs on self-hosted Trailbeam's API and on the hosted app's alike, and shows the whole range: nothing is loaded into
  the browser to be counted.

  The host owns the URL: it keeps the query and the time range there, works out the window for each ask (a range that
  ends now moves on every refresh), and puts its own controls (the time picker, Copy link, refresh) beside the query bar.
*/
import { Fragment, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { StepHistogram } from "./Histogram.tsx";
import {
  EXPORT_HEADERS, EXPORT_MAX, PAGE_LIMIT, barsOf, collectSteps, eventsQuery, exportRow, fmtStepTime, kindLabel, perLabel, pickedWindow,
  serviceText, stepFailed, stepKey, stepResult, toCsv, type Window,
} from "./model.ts";
import { kindsOf, onlyKind, parseQuery, setField } from "./query.ts";
import { STEP_KINDS, type EventHistogram, type EventPage, type EventStep } from "./types.ts";

/** What an export is handed: the table, and lines saying what it holds (for a PDF's heading). */
export type EventExport = { headers: string[]; rows: string[][]; lines: string[]; fileName: string };
export type ExportOption = { label: string; run: (e: EventExport) => void };

export type EventExplorerProps = {
  query: string;
  onQuery: (q: string) => void;
  /** the range's window now; a new object on each refresh */
  window: Window;
  /** names the chosen range (its preset, or its fixed times): a picked span is dropped when it changes, not on a refresh */
  rangeKey: string;
  /** the range in a sentence: "in the last 24 hours" */
  rangeText: string;
  /** GET /api/events with this query string */
  load: (q: Record<string, string>) => Promise<EventPage>;
  messageHref: (id: string) => string;
  /** beside the query bar */
  controls?: ReactNode;
  /** under the list */
  foot?: ReactNode;
  /** more export formats after CSV (the hosted app's PDF) */
  exports?: ExportOption[];
  /** how a CSV reaches the person; a browser download by default */
  download?: (fileName: string, mime: string, text: string) => void;
};

type Rows = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; steps: EventStep[]; cursor: string | null; more: boolean };

const browserDownload = (fileName: string, mime: string, text: string) => {
  const url = URL.createObjectURL(new Blob([text], { type: mime }));
  const a = Object.assign(document.createElement("a"), { href: url, download: fileName });
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
};

const n = (v: number) => v.toLocaleString("en-US");
const message = (e: unknown) => (e instanceof Error ? e.message : "The events couldn't be read.");
const TH = "whitespace-nowrap px-3 py-2 text-left font-mono text-caption font-medium text-ink-3";
const TD = "whitespace-nowrap px-3 py-1.5 align-top";

/** The query bar: typing waits half a second before asking, Enter asks at once. */
function QueryBar({ query, onQuery, error }: { query: string; onQuery: (q: string) => void; error?: string }) {
  const [draft, setDraft] = useState(query);
  useEffect(() => setDraft(query), [query]);
  useEffect(() => {
    if (draft === query) return;
    const t = setTimeout(() => onQuery(draft), 500);
    return () => clearTimeout(t);
  }, [draft]);
  return (
    <div className="min-w-0 flex-1">
      <label className="flex h-10 items-center gap-2 rounded-lg border border-line-strong bg-surface px-3 focus-within:border-brand">
        <span className="sr-only">Query</span>
        <input value={draft} onChange={(e) => setDraft(e.target.value)} onKeyDown={(e) => { if (e.key === "Enter") onQuery(draft); }}
          spellCheck={false} placeholder="queue:orders service:orders-worker kind:delete, or a message ID"
          aria-invalid={!!error} aria-describedby="event-query-help"
          className="w-full bg-transparent font-mono text-caption text-ink outline-none placeholder:text-ink-3" />
        {draft && <button type="button" onClick={() => { setDraft(""); onQuery(""); }} aria-label="Clear the query" className="text-ink-3 hover:text-ink">&times;</button>}
      </label>
      <p id="event-query-help" role={error ? "alert" : undefined} className={`mt-1.5 text-xs ${error ? "text-warning-text" : "text-ink-3"}`}>
        {error ?? <>Filter on <code className="font-mono">queue:</code>, <code className="font-mono">service:</code>, <code className="font-mono">kind:</code> (send, receive, delete, change_visibility, outcome; <code className="font-mono">-kind:</code> leaves one out) and <code className="font-mono">message_id:</code>.</>}
      </p>
    </div>
  );
}

export function EventExplorer({ query, onQuery, window: range, rangeKey, rangeText, load, messageHref, controls, foot, exports = [], download = browserDownload }: EventExplorerProps) {
  const parsed = useMemo(() => parseQuery(query), [query]);
  const paramsKey = JSON.stringify(parsed.params);
  const [chart, setChart] = useState<{ kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; h: EventHistogram | null }>({ kind: "loading" });
  /* a span picked on the chart, in ms: it stays put while a range that ends now moves on */
  const [span, setSpan] = useState<Window | null>(null);
  const [rows, setRows] = useState<Rows>({ kind: "loading" });
  const [open, setOpen] = useState<string | null>(null);
  const [exporting, setExporting] = useState<"menu" | "busy" | null>(null);
  const [exportNote, setExportNote] = useState("");
  const loadRef = useRef(load);
  loadRef.current = load;

  // a new query or range drops the picked span; a refresh of the same range keeps it
  useEffect(() => { setSpan(null); setOpen(null); }, [paramsKey, rangeKey]);

  const bars = useMemo(() => (chart.kind === "ok" && chart.h ? barsOf(chart.h) : []), [chart]);
  const inSpan = span ? bars.flatMap((b, i) => (b.end > span.from_ms && b.start <= span.to_ms ? [i] : [])) : [];
  const picked: [number, number] | null = inSpan.length ? [inSpan[0], inSpan[inSpan.length - 1]] : null;
  const pick = (p: [number, number] | null) => setSpan(p ? pickedWindow(range, bars[p[0]], bars[p[1]]) : null);
  const listWindow = span ?? range;

  // One ask per query string: the chart's ask and the list's first page are the same one when no span is picked.
  const asked = useRef(new Map<string, Promise<EventPage>>());
  const ask = (q: Record<string, string>) => {
    const k = new URLSearchParams(q).toString();
    let p = asked.current.get(k);
    if (!p) {
      if (asked.current.size > 8) asked.current.clear();
      p = loadRef.current(q);
      p.catch(() => asked.current.delete(k));
      asked.current.set(k, p);
    }
    return p;
  };

  // the chart: always the whole range
  useEffect(() => {
    if (parsed.error) return;
    let live = true;
    ask(eventsQuery(range, parsed))
      .then((page) => { if (live) setChart({ kind: "ok", h: page.histogram }); })
      .catch((e) => { if (live) setChart({ kind: "error", message: message(e) }); });
    return () => { live = false; };
  }, [range.from_ms, range.to_ms, paramsKey, Boolean(parsed.error)]);

  // the list's first page: the picked span's, else the range's. The rows on screen stay until the answer comes.
  useEffect(() => {
    if (parsed.error) return;
    let live = true;
    ask(eventsQuery(listWindow, parsed))
      .then((page) => { if (live) setRows({ kind: "ok", steps: page.events, cursor: page.next_cursor, more: !!page.next_cursor }); })
      .catch((e) => { if (live) setRows({ kind: "error", message: message(e) }); });
    return () => { live = false; };
  }, [listWindow.from_ms, listWindow.to_ms, paramsKey, Boolean(parsed.error)]);

  const more = () => {
    if (rows.kind !== "ok" || !rows.cursor) return;
    const had = rows.steps;
    loadRef.current(eventsQuery(listWindow, parsed, PAGE_LIMIT, rows.cursor))
      .then((page) => setRows({ kind: "ok", steps: [...had, ...page.events], cursor: page.next_cursor, more: !!page.next_cursor }))
      .catch((e) => setRows({ kind: "error", message: message(e) }));
  };

  const h = chart.kind === "ok" ? chart.h : null;
  const pickedBars = picked ? bars.slice(picked[0], picked[1] + 1) : bars;
  const total = h ? pickedBars.reduce((a, b) => a + b.total, 0) : null;
  const kinds = kindsOf(parsed);
  const utc = (ms: number) => new Date(ms).toISOString().slice(0, 19).replace("T", " ");
  const spanText = span ? `between ${utc(span.from_ms)} and ${utc(span.to_ms)} UTC` : rangeText;

  const runExport = async (run: (e: EventExport) => void) => {
    setExporting("busy");
    setExportNote("");
    try {
      const { steps, more: cut } = await collectSteps(loadRef.current, listWindow, parsed, EXPORT_MAX);
      const fileName = `trailbeam-events-${new Date(listWindow.to_ms).toISOString().slice(0, 10)}`;
      const lines = [`Query: ${query.trim() || "none (every step)"}`, `Time range: ${spanText}`,
        cut ? `The newest ${n(steps.length)} matching steps (there are more), newest first` : `${n(steps.length)} ${steps.length === 1 ? "step" : "steps"}, newest first`];
      run({ headers: EXPORT_HEADERS, rows: steps.map(exportRow), lines, fileName });
      setExportNote(cut ? `Exported the newest ${n(steps.length)} steps. Narrow the range or the query to export the rest.` : `Exported ${n(steps.length)} ${steps.length === 1 ? "step" : "steps"}.`);
    } catch (e) {
      setExportNote(`The export failed: ${message(e)}`);
    } finally {
      setExporting(null);
    }
  };
  const csv = (e: EventExport) => download(`${e.fileName}.csv`, "text/csv;charset=utf-8", toCsv(e.headers, e.rows));

  return (
    <div className="flex min-h-0 flex-1 flex-col" data-event-explorer>
      <div className="grid gap-3 border-b border-line px-4 py-3 md:px-5">
        <div className="flex flex-wrap items-start gap-3">
          <QueryBar query={query} onQuery={onQuery} error={parsed.error} />
          {controls}
        </div>
        {chart.kind === "loading" && !parsed.error && <p className="h-[110px] text-sm text-ink-3">Loading the chart</p>}
        {chart.kind === "error" && <p className="text-sm text-ink-2" role="alert">{chart.message}</p>}
        {h && <>
          <StepHistogram bars={bars} per={perLabel(h.bucket_ms)} picked={picked} onPick={pick} />
          <div className="flex flex-wrap items-center gap-1.5 text-xs" role="group" aria-label="Kinds of step">
            {STEP_KINDS.map((k) => {
              const kept = kinds.includes(k);
              const only = kinds.length === 1 && kept;
              return (
                <button key={k} type="button" onClick={() => onQuery(onlyKind(query, k))} aria-pressed={only}
                  title={only ? "Show every kind again" : `Show only ${kindLabel(k).toLowerCase()} steps`}
                  className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 ${only ? "border-brand text-ink" : "border-line text-ink-2 hover:border-line-strong hover:text-ink"} ${kept ? "" : "opacity-50"}`}>
                  {kindLabel(k)} <span className="font-mono tabular-nums text-ink-3">{kept ? n(h.totals[k]) : "-"}</span>
                </button>
              );
            })}
            {span && <button type="button" onClick={() => setSpan(null)} className="ml-auto text-brand hover:underline">Show the whole range</button>}
          </div>
        </>}
      </div>

      <div className="min-h-0 flex-1 overflow-auto">
        {parsed.error && <p className="px-5 py-8 text-sm text-ink-2">Fix the query to see steps.</p>}
        {!parsed.error && rows.kind === "loading" && <p className="px-5 py-8 text-sm text-ink-3">Loading steps</p>}
        {!parsed.error && rows.kind === "error" && <p className="px-5 py-8 text-sm text-ink-2" role="alert">{rows.message}</p>}
        {!parsed.error && rows.kind === "ok" && (
          <table className="w-full border-collapse text-[13px]" data-steps>
            <thead className="sticky top-0 z-[1] bg-surface">
              <tr className="border-b border-line">
                {["Time (UTC)", "Step", "Queue", "Service", "Message ID", "Receives", "Result"].map((c) => <th key={c} className={TH}>{c}</th>)}
                <th className={`${TH} text-right`}>
                  <span className="relative inline-block">
                    <button type="button" onClick={() => setExporting(exporting === "menu" ? null : "menu")} disabled={!rows.steps.length || exporting === "busy"}
                      aria-expanded={exporting === "menu"} aria-haspopup="menu"
                      className="rounded-md border border-line-strong px-2 py-0.5 font-sans text-xs text-ink-2 hover:text-ink disabled:opacity-40">
                      {exporting === "busy" ? "Exporting" : "Export"}
                    </button>
                    {exporting === "menu" && (
                      <div role="menu" className="absolute top-full right-0 z-30 mt-1.5 w-60 rounded-lg border border-line-strong bg-surface p-1.5 text-left font-sans shadow-lg">
                        <p className="px-2 pt-1 pb-1.5 text-xs font-normal text-ink-3">Every matching step {spanText}, up to the newest {n(EXPORT_MAX)}</p>
                        {[{ label: "CSV", run: csv }, ...exports].map((x) => (
                          <button key={x.label} role="menuitem" type="button" onClick={() => runExport(x.run)} className="block w-full rounded-md px-2 py-1.5 text-left text-[13px] text-ink hover:bg-canvas">{x.label}</button>
                        ))}
                      </div>
                    )}
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.steps.map((s) => {
                const key = stepKey(s);
                const sel = open === key;
                const failed = stepFailed(s);
                return (
                  <Fragment key={key}>
                    <tr className={`cursor-pointer border-b border-line/60 ${sel ? "bg-canvas" : "hover:bg-canvas/60"}`} tabIndex={0} aria-expanded={sel}
                      onClick={() => setOpen(sel ? null : key)} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setOpen(sel ? null : key); } }}>
                      <td className={`${TD} font-mono text-caption tabular-nums text-ink-2`}>{fmtStepTime(s)}</td>
                      <td className={`${TD} text-ink`}>{kindLabel(s.kind)}</td>
                      <td className={TD}><button type="button" onClick={(e) => { e.stopPropagation(); onQuery(setField(query, "queue", s.queue)); }} title="Show only this queue" className="font-mono text-caption text-ink hover:text-brand">{s.queue}</button></td>
                      <td className={TD}>{s.service_name
                        ? <button type="button" onClick={(e) => { e.stopPropagation(); onQuery(setField(query, "service", s.service_name!)); }} title="Show only this service" className="font-mono text-caption text-ink-2 hover:text-brand">{serviceText(s)}</button>
                        : <span className="text-ink-3">unknown</span>}</td>
                      <td className={TD}>{s.message_id
                        ? <a href={messageHref(s.message_id)} onClick={(e) => e.stopPropagation()} className="font-mono text-caption text-ink hover:text-brand">{s.message_id}</a>
                        : <span className="text-ink-3" title="The receive whose receipt handle this step used isn't stored">not known</span>}</td>
                      <td className={`${TD} font-mono text-caption tabular-nums text-ink-2`}>{s.receive_count ?? ""}</td>
                      <td className={`${TD} ${failed ? "text-critical-text" : "text-ink-2"}`} colSpan={2}>{stepResult(s)}</td>
                    </tr>
                    {sel && (
                      <tr className="border-b border-line bg-canvas">
                        <td colSpan={8} className="px-5 py-3">
                          <dl className="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1 text-[13px]">
                            <dt className="text-ink-3">Trace</dt><dd className="font-mono text-caption text-ink-2 break-all">{s.trace_id}</dd>
                            <dt className="text-ink-3">Span</dt><dd className="font-mono text-caption text-ink-2">{s.span_id}</dd>
                            <dt className="text-ink-3">Took</dt><dd className="font-mono text-caption text-ink-2">{s.start_unix_nano ? `${(Number(BigInt(s.time_unix_nano) - BigInt(s.start_unix_nano)) / 1e6).toFixed(1)} ms` : "not observed"}</dd>
                            {s.error_code && <><dt className="text-ink-3">SQS error</dt><dd className="font-mono text-caption text-ink-2">{s.error_code}</dd></>}
                            {s.error_type && <><dt className="text-ink-3">Exception</dt><dd className="font-mono text-caption text-ink-2">{s.error_type}</dd></>}
                            {s.message_id && <><dt className="text-ink-3">Timeline</dt><dd><a href={messageHref(s.message_id)} className="text-brand hover:underline">Open this message&rsquo;s timeline</a></dd></>}
                          </dl>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
              {rows.steps.length === 0 && (
                <tr><td colSpan={8} className="px-5 py-10 text-center text-sm text-ink-2">No recorded steps match {query.trim() ? "this query " : ""}{spanText}.</td></tr>
              )}
            </tbody>
          </table>
        )}
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-line px-4 py-2 text-[13px] text-ink-3 md:px-5" data-explorer-foot>
        <span>
          {rows.kind === "ok" && <>
            Showing <b className="font-mono font-medium text-ink">{n(rows.steps.length)}</b>
            {total !== null && <> of <b className="font-mono font-medium text-ink">{n(total)}</b></>} {total === 1 ? "step" : "steps"} {spanText}, newest first.
            {rows.more && <> <button type="button" onClick={more} className="text-brand hover:underline">Show {PAGE_LIMIT} more</button></>}
          </>}
          {exportNote && <span className="ml-2" aria-live="polite">{exportNote}</span>}
        </span>
        <span>{foot}</span>
      </div>
    </div>
  );
}
