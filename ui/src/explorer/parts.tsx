"use client";

/*
  The parts of the Event explorer that other pages use too: how it draws a row's cell and the row's level mark, its
  column list, and its query bar. The explorer and the dashboards' widgets and editors both use these, so a row, a
  column or a filter looks and behaves the same wherever it appears. Each takes the host's query language (`lang`);
  without one it is the language over what events recorded.
*/
import { cloneElement, useMemo, useRef, useState, type ReactElement, type ReactNode } from "react";
import { Search, X } from "lucide-react";
import { SourceTag } from "../design/pills.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { fmtDate, eventName, type Ev } from "../model/events.ts";
import type { Field, QueryLanguage } from "../model/query.ts";
import { RECORDED_LANGUAGE, columnChoices, level, show } from "./columns.ts";

/* The row's level (warning or error), on the 3px bar at the left edge of the row's first cell (.ev-row, tokens.css). */
export function LevelMark({ e }: { e: Ev }) {
  const l = level(e);
  if (l === "info") return null;
  const word = l === "warn" ? "Warning" : "Error";
  return <Tooltip content={word} className="absolute inset-y-0 left-0 w-2"><span className="sr-only">{word}</span></Tooltip>;
}

/* One cell of the event list; `cell` is the td's classes and `sel` marks the open row. `lead` goes first inside the cell
   (the first cell takes the row's LevelMark). */
export function eventCell(c: string, e: Ev, cell: string, sel = false, lead?: ReactNode, lang: QueryLanguage = RECORDED_LANGUAGE) {
  if (!lead) return cellOf(c, e, cell, sel, lang);
  const td = cellOf(c, e, `${cell} relative`, sel, lang) as ReactElement<{ children?: ReactNode }>;
  return cloneElement(td, undefined, lead, td.props.children);
}

function cellOf(c: string, e: Ev, cell: string, sel: boolean, lang: QueryLanguage) {
  if (c === "date") return <td key={c} className={`${cell} tabular-nums ${sel ? "text-accent" : "text-ink-3"}`}>{fmtDate(e.ms)}</td>;
  if (c === "event")
    return (
      <td key={c} className={cell}>
        <span className="font-sans text-[13px] font-medium text-ink">{eventName(e)}</span>
      </td>
    );
  if (c === "detail") return <td key={c} className={cell}>{e.detail}</td>;
  if (c === "source") return <td key={c} className={cell}><SourceTag source={e.source} /></td>;
  if (c === "message_id") return <td key={c} className={`${cell} text-ink`}>{e.messageId.slice(0, 8)}</td>;
  const f = lang.def(c);
  if (!f) return <td key={c} className={cell} />;
  const v = f.get(e);
  return <td key={c} className={cell}>{v === undefined ? (f.buckets ? null : <span className="font-sans text-ink-3">{c === "body" ? "not stored" : "none"}</span>) : show(f, v)}</td>;
}

/* The list of columns to tick, grouped as the field registry groups them. */
/* only: the columns to offer (the ones some event fills); every column by default */
export function ColumnChoices({ columns, setColumns, live = false, lang = RECORDED_LANGUAGE, only }: { columns: string[]; setColumns: (c: string[]) => void; live?: boolean; lang?: QueryLanguage; only?: Set<string> }) {
  return (
    <>
      {columnChoices(lang, live).map(({ group, cols }) => ({ group, cols: only ? cols.filter((c) => only.has(c.id)) : cols })).filter((g) => g.cols.length).map(({ group, cols }) => (
        <div key={group} className="py-1.5">
          <p className="mb-1 text-xs font-medium text-ink-3">{group}</p>
          {cols.map((c) => {
            const on = columns.includes(c.id);
            return (
              <label key={c.id} className="-mx-1.5 flex cursor-pointer items-center gap-2 rounded-[5px] px-1.5 py-[3px] hover:bg-sunken">
                <input
                  type="checkbox"
                  checked={on}
                  disabled={on && columns.filter((x) => !only || only.has(x)).length === 1}
                  onChange={() => setColumns(on ? columns.filter((x) => x !== c.id) : [...columns, c.id])}
                  className="size-3.5 shrink-0 accent-brand"
                />
                <span className={`min-w-0 flex-1 truncate text-ink ${group === "Event" ? "text-[13px]" : "font-mono text-caption"}`}>{c.label}</span>
              </label>
            );
          })}
        </div>
      ))}
    </>
  );
}

/*
  The query bar: one line of field:value terms, highlighted as they parse, with
  suggestions for the word being typed and the first mistake said under it.
  values gives a field's known values with their counts, for suggestions.
  vars are names that may follow a $, as a dashboard's variables do.
*/
export function QueryInput({ query, setQuery, values, right, note, vars = [], bodyPaths = [], live = false, lang = RECORDED_LANGUAGE, placeholder = "Filter events, for example queue:orders-events status:error", label = "Filter events" }: {
  query: string;
  lang?: QueryLanguage;
  /* the query runs on a workspace's own events: sampleOnly fields are not suggested, and naming one is an error */
  live?: boolean;
  /* the body paths to suggest: the stored bodies' (bodyPathsOf), or the hosted sample payloads' */
  bodyPaths?: string[];
  setQuery: (q: string) => void;
  values?: (field: Field) => [string, number][] | undefined;
  right?: ReactNode;
  note?: ReactNode;
  vars?: string[];
  placeholder?: string;
  label?: string;
}) {
  const [focused, setFocused] = useState(false);
  const mirror = useRef<HTMLDivElement>(null);
  const terms = useMemo(() => lang.parse(query, live), [lang, query, live]);
  const isVar = (raw: string) => vars.some((v) => raw.endsWith(`$${v}`));
  const error = terms.find((t) => t.error && !isVar(t.raw))?.error;

  /* Suggestions for the word being typed at the end of the query. */
  const last = /(\S*)$/.exec(query)?.[1] ?? "";
  const suggestions = useMemo(() => {
    if (!focused || query.endsWith(" ") || !last || last.startsWith('"')) return [];
    const neg = last.startsWith("-") ? "-" : "";
    const word = neg ? last.slice(1) : last;
    const i = word.indexOf(":");
    if (i < 0) {
      const w = word.toLowerCase();
      /* a path into the message body: offer the paths seen in stored bodies */
      if (w.length >= 2 && "body.".startsWith(w.slice(0, 5)) && (w.length <= 5 || w.startsWith("body."))) {
        const rest = word.slice(5).toLowerCase();
        return [...(w.length <= 4 ? [{ text: `${neg}body:`, hint: "Text anywhere in the body" }] : []), ...bodyPaths.filter((p) => p.toLowerCase().startsWith(rest)).slice(0, 7).map((p) => ({ text: `${neg}body.${p}:`, hint: "Message body" }))];
      }
      return lang.fieldsFor(live).map((f) => f.name).filter((f) => f.toLowerCase().startsWith(w) || f.toLowerCase().includes(`.${w}`)).slice(0, 7).map((f) => ({ text: `${neg}${f}:`, hint: lang.def(f)!.group as string }));
    }
    const field: Field = lang.def(word.slice(0, i))?.name ?? "";
    const typed = word.slice(i + 1).toLowerCase();
    if (typed.startsWith("$")) return vars.filter((v) => `$${v}`.startsWith(typed) && `$${v}` !== typed).map((v) => ({ text: `${neg}${field}:$${v} `, hint: "Dashboard variable" }));
    const known = field ? values?.(field) : undefined;
    if (!known) return [];
    return known
      .filter(([v]) => v.toLowerCase().startsWith(typed) && v.toLowerCase() !== typed)
      .slice(0, 6)
      .map(([v, n]) => ({ text: `${neg}${field}:${/[\s()]/.test(v) ? `"${v}"` : v} `, hint: `${n} ${n === 1 ? "event" : "events"}` }));
  }, [focused, query, last, values, vars, lang, live, bodyPaths]);
  const accept = (text: string) => setQuery(query.slice(0, query.length - last.length) + text);

  /* The highlighted copy of the query that sits under the transparent input. */
  const pieces: ReactNode[] = [];
  let at = 0;
  terms.forEach((t, i) => {
    if (t.start > at) pieces.push(query.slice(at, t.start));
    const c = t.raw.indexOf(":");
    const bad = t.error && !isVar(t.raw);
    pieces.push(
      <span key={i} className={`rounded-[3px] ${bad ? "bg-critical-soft shadow-[0_0_0_2px_var(--color-critical-soft),0_0_0_3px_var(--color-critical)]" : t.neg ? "bg-critical-soft shadow-[0_0_0_2px_var(--color-critical-soft)]" : t.field ? "bg-sunken shadow-[0_0_0_2px_var(--color-sunken)]" : ""}`}>
        {t.field || t.error ? (<><span className="text-ink-2">{t.raw.slice(0, c + 1)}</span><span className={isVar(t.raw) ? "text-brand" : "text-ink"}>{t.raw.slice(c + 1)}</span></>) : t.raw}
      </span>,
    );
    at = t.end;
  });
  if (at < query.length) pieces.push(query.slice(at));

  return (
    <div className="relative min-w-0 flex-1">
      <div className={`flex min-h-10 items-center gap-2 rounded-lg border bg-surface py-1 pr-2 pl-3 font-mono text-[13px] ${error ? "border-critical" : focused ? "border-accent ring-4 ring-accent/15" : "border-line-strong"}`}>
        <Search size={15} className="shrink-0 text-ink-3" />
        <div className="relative min-w-0 flex-1">
          <div ref={mirror} aria-hidden className="pointer-events-none absolute inset-0 overflow-hidden py-1 leading-5 whitespace-pre text-ink">
            {pieces}
          </div>
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onFocus={() => setFocused(true)}
            onBlur={() => setFocused(false)}
            onScroll={(e) => { if (mirror.current) mirror.current.scrollLeft = e.currentTarget.scrollLeft; }}
            onKeyDown={(e) => { if (e.key === "Tab" && suggestions.length) { e.preventDefault(); accept(suggestions[0].text); } }}
            spellCheck={false}
            autoComplete="off"
            aria-label={label}
            aria-invalid={!!error}
            placeholder={placeholder}
            className="relative block w-full bg-transparent py-1 font-mono text-[13px] leading-5 text-transparent caret-ink !outline-none placeholder:font-sans placeholder:text-ink-3"
          />
        </div>
        {query && (
          <button type="button" onClick={() => setQuery("")} aria-label="Clear the query" className="rounded p-1 text-ink-3 hover:bg-sunken hover:text-ink"><X size={14} /></button>
        )}
        {right}
      </div>
      {suggestions.length > 0 && (
        <ul className="absolute top-full left-0 z-10 mt-1.5 w-[420px] max-w-full rounded-lg border border-line bg-surface p-1.5 text-[13px] shadow-raised">
          {suggestions.map((s, i) => (
            <li key={s.text}>
              <button type="button" onMouseDown={(e) => { e.preventDefault(); accept(s.text); }} className={`flex w-full items-baseline gap-3 rounded-md px-2 py-1.5 text-left hover:bg-sunken ${i === 0 ? "bg-sunken" : ""}`}>
                <code className="min-w-[150px] font-mono text-xs text-ink">{s.text.trim()}</code>
                <span className="text-ink-3">{s.hint}{i === 0 ? ", press Tab" : ""}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
      {error && <p className="mt-1.5 text-[13px] text-critical-text">{error}</p>}
      {!error && note}
    </div>
  );
}
