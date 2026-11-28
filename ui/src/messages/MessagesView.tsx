"use client";

/*
  The Messages list (row 4 of the one-UI plan), built from the Event explorer's parts: its query bar (QueryInput, over the
  list API's filters: listQuery.ts), its results bar with Columns and Export, its column list, and its event-list rows
  with the level bar at the left edge. Both products draw it on their own list API: self-hosted Trailbeam on
  GET /api/messages, the hosted app on its messages page. A list is filtered and paged on the server, so the query is
  applied when it is sent (Enter), not on each key: `onQuery` gets the query, and the host asks its API again.

  The host keeps the query and the time range (usually in the URL), and hands in the page's rows, the controls beside
  the query bar (TimePicker, Copy link, Live), what the results bar says, the pager, and what stands in place of an
  empty list. A column no row fills is left out.
*/
import { useCallback, useEffect, useMemo, useState, type FormEvent, type ReactNode } from "react";
import { UiLink } from "../design/nav.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { LEVEL_COLOR, TD, TH } from "../explorer/columns.ts";
import { download as browserDownload, toCsv } from "../explorer/export.ts";
import { ColumnList, ColumnsButton, ColumnsPanel, ExportMenu, QueryInput } from "../explorer/parts.tsx";
import { DEFAULT_MESSAGE_COLUMNS, MESSAGE_COLUMNS, messageLevel, type MessageColumn, type MessageRow } from "./columns.tsx";
import { LIST_STATES, type ListField, type MessageQueryLanguage } from "./listQuery.ts";

export type MessagesViewProps = {
  /** the page's rows, in the API's order */
  messages: MessageRow[];
  /** the query the rows were asked with */
  query: string;
  /** a query sent with Enter, with no mistake in it: the host reads it with listQuery and asks again */
  onQuery: (q: string) => void;
  /** the fields the host's list API takes (messageQueryLanguage) */
  lang: MessageQueryLanguage;
  /** a field's known values to suggest while typing (the workspace's queues); state's are always offered */
  values?: (field: ListField) => string[] | undefined;
  /** the body paths to suggest after body. */
  bodyPaths?: string[];
  /** a message's timeline page */
  messageHref: (id: string) => string;
  /** beside the query bar: the time picker, Copy link, Live */
  controls?: ReactNode;
  /** under the query bar, when the query has no mistake (what a body search reads) */
  queryNote?: ReactNode;
  /** over the results bar (a search that found no message, a server that keeps no bodies) */
  notice?: ReactNode;
  /** the results bar's words; "N messages, most recently active first" by default */
  summary?: ReactNode;
  /** the results bar's links to more rows (Next page, Newest) */
  pager?: ReactNode;
  /** in place of the table when there are no rows: loading, a failed ask, or the empty state */
  empty?: ReactNode;
  /** the host's columns, after the list's own (Why, Age) */
  extra?: MessageColumn[];
  /** the columns shown until someone picks; every column by default */
  defaultColumns?: string[];
  /** the localStorage key the chosen columns are kept under */
  columnsKey?: string;
  /** the export's file name, without .csv */
  exportName?: string;
  /** how a file reaches the person; a browser download by default */
  download?: (fileName: string, mime: string, data: BlobPart) => void;
  /** fill the screen with no card around it */
  page?: boolean;
};

const PLACEHOLDER: Record<string, string> = {
  body: "Filter messages, for example queue:orders body.order_id:ord-1003",
  state: "Filter messages, for example queue:orders state:dlq",
};

export function MessagesView({ messages, query, onQuery, lang, values, bodyPaths = [], messageHref, controls, queryNote, notice, summary, pager, empty,
  extra = [], defaultColumns, columnsKey = "trailbeam-message-columns-1", exportName = "trailbeam-messages", download = browserDownload, page = false }: MessagesViewProps) {
  const all = useMemo(() => [...MESSAGE_COLUMNS, ...extra], [extra]);
  const defaults = useMemo(() => defaultColumns ?? all.map((c) => c.id), [defaultColumns, all]);
  const [columns, setColumnsState] = useState<string[]>(defaults);
  const [picker, setPicker] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [draft, setDraft] = useState(query);
  useEffect(() => setDraft(query), [query]);

  /* The chosen columns are remembered in this browser; a host's own columns join on the right when they are new. */
  useEffect(() => {
    try {
      const saved = JSON.parse(localStorage.getItem(columnsKey) ?? "null");
      if (Array.isArray(saved) && saved.length && saved.every((c) => typeof c === "string")) {
        const known = saved.filter((c: string) => all.some((x) => x.id === c));
        if (known.length) setColumnsState([...known, ...extra.map((c) => c.id).filter((id) => !saved.includes(id))]);
      }
    } catch {}
  }, [columnsKey]);
  const setColumns = (c: string[]) => {
    setColumnsState(c);
    try { localStorage.setItem(columnsKey, JSON.stringify(c)); } catch {}
  };
  const closePicker = useCallback(() => setPicker(false), []);

  const filled = useMemo(() => new Set(all.filter((c) => !c.fills || messages.some(c.fills)).map((c) => c.id)), [all, messages]);
  const shown = useMemo(() => {
    const c = columns.map((id) => all.find((x) => x.id === id)).filter((x): x is MessageColumn => !!x && filled.has(x.id));
    return c.length ? c : all.filter((x) => x.id === "message_id");
  }, [columns, all, filled]);
  const groups = useMemo(() => [
    { group: "Message", cols: MESSAGE_COLUMNS.map(({ id, label }) => ({ id, label })) },
    ...(extra.length ? [{ group: "This list", cols: extra.map(({ id, label }) => ({ id, label })) }] : []),
  ], [extra]);

  const terms = useMemo(() => lang.parse(draft), [lang, draft]);
  const mistake = terms.some((t) => t.error);
  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (!mistake) onQuery(draft.trim());
  };
  const suggest = (field: string): [string, number | null][] | undefined => {
    const known = field === "state" ? LIST_STATES.map((s) => s.key) : values?.(field as ListField);
    return known?.map((v) => [v, null]);
  };

  const exportCsv = () => {
    const table = { headers: shown.map((c) => c.label), rows: messages.map((m) => shown.map((c) => c.text(m))) };
    download(`${exportName}.csv`, "text/csv;charset=utf-8", toCsv(table));
  };

  const n = messages.length;
  return (
    <div className={page ? "flex min-h-0 flex-1 flex-col" : "overflow-hidden rounded-xl border border-line bg-surface"} data-messages-view>
      <div className={`border-b border-line ${page ? "px-5 py-3" : "p-4"}`}>
        <div className="flex items-start gap-3">
          <form onSubmit={submit} className="min-w-0 flex-1" role="search">
            <QueryInput query={draft} setQuery={setDraft} lang={lang} values={suggest} bodyPaths={bodyPaths} label="Filter messages"
              placeholder={PLACEHOLDER[lang.fields.includes("body") ? "body" : "state"]}
              note={draft.trim() !== query.trim() && !mistake
                ? <p className="mt-1.5 font-sans text-[13px] text-ink-3">Press Enter to filter the list.</p>
                : queryNote} />
          </form>
          {controls}
        </div>
      </div>
      {notice}
      <div className="relative flex min-h-0 min-w-0 flex-1 flex-col">
        {/* The results bar: how many messages are listed, the links to more, and what to do with them. */}
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 border-b border-line bg-surface px-4 py-2 text-[13px] text-ink-3" data-results-bar>
          <p className="min-w-0 flex-1">
            {summary ?? <><b className="font-mono font-medium text-ink">{n.toLocaleString("en-US")}</b> {n === 1 ? "message" : "messages"}, most recently active first</>}
          </p>
          {pager && <span className="flex shrink-0 items-center gap-3">{pager}</span>}
          <div className="flex shrink-0 items-center gap-1.5">
            <ColumnsButton open={picker} onClick={() => { setExporting(false); setPicker(!picker); }} />
            <ExportMenu open={exporting} setOpen={(o) => { setPicker(false); setExporting(o); }} disabled={n === 0} formats={["csv"]} onExport={exportCsv}
              what={<>The {n.toLocaleString("en-US")} {n === 1 ? "message" : "messages"} on this page, with the columns shown</>} />
          </div>
        </div>
        {picker && (
          <ColumnsPanel shown={shown.length} onReset={() => setColumns(defaults)} onClose={closePicker} note="Columns that no message on this page fills are left out.">
            <ColumnList groups={groups} columns={columns} setColumns={setColumns} only={filled} />
          </ColumnsPanel>
        )}
        <div className={`min-h-0 min-w-0 flex-1 overflow-auto ${page ? "bg-canvas" : ""}`}>
          {n === 0 ? (
            <div className="px-6 py-10 font-sans text-sm text-ink-2" data-empty-list>{empty ?? "No messages match."}</div>
          ) : (
            <table className="w-full border-collapse font-mono text-caption" data-messages>
              <thead className="sticky top-0 z-[1] bg-surface">
                <tr>
                  {shown.map((c) => (
                    <th key={c.id} className={`${TH} ${c.align === "right" ? "text-right" : ""}`}>
                      {c.tip ? <Tooltip content={c.tip} tabIndex={0}>{c.label}</Tooltip> : c.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {messages.map((m) => {
                  const lv = messageLevel(m);
                  /* the row's link is its message ID, or the first cell when Message ID is not shown */
                  const linked = (c: MessageColumn, i: number) => c.id === "message_id" || (i === 0 && !shown.some((x) => x.id === "message_id"));
                  return (
                    <tr key={m.message_id} className="ev-row group" style={{ ["--bar" as string]: LEVEL_COLOR[lv.level] }} data-level={lv.level}>
                      {shown.map((c, i) => (
                        <td key={c.id} className={`${TD} group-hover:bg-sunken ${c.align === "right" ? "text-right" : ""} ${i === 0 ? "relative" : ""}`}>
                          {i === 0 && lv.word && <Tooltip content={lv.word} className="absolute inset-y-0 left-0 w-2"><span className="sr-only">{lv.word}</span></Tooltip>}
                          {linked(c, i) ? <UiLink href={messageHref(m.message_id)} className="hover:text-brand">{c.cell(m)}</UiLink> : c.cell(m)}
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
