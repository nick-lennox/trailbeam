/*
  The Event explorer's columns: which there are, what each is called, how a value is written, and which ones the events
  in a range fill. The explorer, the dashboards' event lists and their editors all read these, so a column looks the
  same wherever it appears. The field registry is the host's query language (queryLanguage, model/query.ts): on a
  workspace's own data and on self-hosted Trailbeam it reads what each event recorded, on the hosted sample its
  attributes. Pure.
*/
import { fmtBytes, fmtMs } from "../model/format.ts";
import { GROUPS, queryLanguage, type FieldDef, type Group, type QueryLanguage } from "../model/query.ts";
import { eventName, fmtDate, type Ev } from "../model/events.ts";

/** The query language over what events recorded (Ev.attrs, Ev.body): the default for every part here. */
export const RECORDED_LANGUAGE: QueryLanguage = queryLanguage();

/** Columns drawn specially; every other column is a field from the registry. */
export const SPECIAL: Record<string, string> = { date: "Date", event: "Event", detail: "Detail" };
export const DEFAULT_COLUMNS = ["date", "event", "queue", "message_id", "service", "tenant", "receive_count", "body_size", "processing_time", "time_in_queue", "body", "detail", "source"];

/** The columns to choose from, grouped as the field registry groups them. live: a workspace's own events, which have no sampleOnly field. */
export const columnChoices = (lang: QueryLanguage = RECORDED_LANGUAGE, live = false): { group: Group; cols: { id: string; label: string }[] }[] => GROUPS.map((g) => ({
  group: g,
  cols: [
    ...(g === "Event" ? [{ id: "date", label: "Date" }, { id: "event", label: "Event" }, { id: "detail", label: "Detail" }, { id: "body", label: "Body" }] : []),
    ...lang.fieldsFor(live).filter((f) => f.group === g && f.name !== "event").map((f) => ({ id: f.name, label: f.label })),
  ],
})).filter((c) => c.cols.length);

export const colLabel = (id: string, lang: QueryLanguage = RECORDED_LANGUAGE) => SPECIAL[id] ?? lang.def(id)?.label ?? id;

/** A value as a column shows it. */
export const show = (f: FieldDef, v: string | number) =>
  typeof v !== "number" ? v : f.kind === "duration" ? (v && v % 1000 === 0 ? `${v / 1000} s` : fmtMs(Math.round(v))) : f.kind === "bytes" ? fmtBytes(v) : f.name === "queue.MessageRetentionPeriod" ? `${v} ${v === 1 ? "day" : "days"}` : String(v);

/** A cell's text, written in full: what an export writes. */
export function cellText(c: string, e: Ev, lang: QueryLanguage = RECORDED_LANGUAGE, sourceLabel: (s: Ev["source"]) => string = (s) => s): string {
  if (c === "date") return fmtDate(e.ms);
  if (c === "event") return eventName(e);
  if (c === "detail") return e.detail;
  if (c === "source") return sourceLabel(e.source);
  if (c === "message_id") return e.messageId;
  const f = lang.def(c);
  const v = f?.get(e);
  return f && v !== undefined ? String(show(f, v)) : "";
}

/**
  The columns and filters some event fills, of `ids` (column ids or field names): a column no row fills is left out of
  the table and the column list, and a filter no event has a value for is left out of the filter panel. Date, Event,
  Message ID and Source are on every event. Read from the range's events, not the ones a query keeps, so the table
  doesn't change shape as a query is typed.
*/
export function filledColumns(ids: Iterable<string>, events: readonly Ev[], lang: QueryLanguage = RECORDED_LANGUAGE): Set<string> {
  const out = new Set<string>();
  for (const id of ids) {
    if (id === "date" || id === "event" || id === "message_id" || id === "source") { out.add(id); continue; }
    if (id === "detail") { if (events.some((e) => e.detail)) out.add(id); continue; }
    const f = lang.def(id);
    if (f && events.some((e) => f.get(e) !== undefined && f.get(e) !== "")) out.add(id);
  }
  return out;
}

/* Color says one thing only, as in a log explorer: how the event went. Every row has a bar on its left edge: none for
   a normal event, amber for one to look at (a visibility timeout that expired), red for an error. What kind of event
   it was is the text. */
export type Level = "info" | "warn" | "error";
export const level = (e: Ev): Level => (e.status === "error" ? "error" : e.event === "visibility_expired" ? "warn" : "info");
export const LEVEL_COLOR: Record<Level, string> = { info: "transparent", warn: "var(--color-warning)", error: "var(--color-critical)" };

export const TH = "border-b border-line px-2.5 py-[7px] text-left font-sans text-xs font-medium whitespace-nowrap text-ink-3";
export const TD = "max-w-[300px] truncate border-b border-line px-2.5 py-1.5 text-ink-2";
