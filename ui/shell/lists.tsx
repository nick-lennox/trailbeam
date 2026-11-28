/*
  The list parts other pages use: Stuck's filter form and range control, and refresh. Messages (`/`) and Duplicates
  (`/duplicates`) are messages.tsx, on the shared MessagesView; Stuck, grouped by cause, is stuck.tsx, a plain GET form
  over its API, so the URL is the page's state.
*/
import { useEffect, useState, type FormEvent } from "react";
import { RANGE_PRESETS, REFRESH_S, formQuery, toInputUtc, type TimeRange } from "../src/index.ts";

export type Field = { name: string; label: string; kind: "text" | "number" | [string, string][]; placeholder?: string; disabled?: boolean };

/** A list's time range: what the window is on (`what`, for the sentences) and whether its API takes to_ms. */
export type RangeOpt = { what: "last seen" | "first seen" | "cause seen"; upper: boolean };

export const pick = (params: Record<string, string>, names: string[]) => Object.fromEntries(names.filter((n) => params[n]).map((n) => [n, params[n]]));
export const qs = (p: Record<string, string>) => new URLSearchParams(Object.entries(p).filter(([, v]) => v !== "")).toString();
export const withQuery = (path: string, p: Record<string, string>) => { const q = qs(p); return q ? `${path}?${q}` : path; };
const input = "h-9 rounded-lg border border-line-strong bg-surface px-3 font-mono text-caption text-ink";

/** The range control: presets, all time, or custom (two UTC times; Stuck takes only a start). */
function RangeFields({ range, opt }: { range: TimeRange; opt: RangeOpt }) {
  const initial = range.kind === "preset" ? range.preset : range.kind;
  const [kind, setKind] = useState<string>(initial === "all" ? "" : initial);
  const custom: { from_ms?: number; to_ms?: number } = range.kind === "custom" ? range : {};
  return (
    <>
      <label className="flex flex-col gap-1">
        <span>{opt.what[0].toUpperCase() + opt.what.slice(1)}</span>
        <select name="range" value={kind} onChange={(e) => setKind(e.target.value)} className={input}>
          <option value="">any time</option>
          {RANGE_PRESETS.map(([k, label]) => <option key={k} value={k}>{label.toLowerCase()}</option>)}
          <option value="custom">custom</option>
        </select>
      </label>
      {kind === "custom" && <>
        <label className="flex flex-col gap-1">
          <span>From (UTC)</span>
          <input name="from" type="datetime-local" defaultValue={custom.from_ms !== undefined ? toInputUtc(custom.from_ms) : ""} className={input} />
        </label>
        {opt.upper && (
          <label className="flex flex-col gap-1">
            <span>To (UTC)</span>
            <input name="to" type="datetime-local" defaultValue={custom.to_ms !== undefined ? toInputUtc(custom.to_ms) : ""} className={input} />
          </label>
        )}
      </>}
    </>
  );
}

/** The filters, a GET form; `hidden` goes with them unchanged (Stuck's picked cause). */
export function Filters({ action, values, fields, range, refresh, hidden = {} }: { action: string; values: Record<string, string>; fields: Field[]; range?: { r: TimeRange; opt: RangeOpt }; refresh: boolean; hidden?: Record<string, string> }) {
  // the form's fields become the URL (formQuery: a custom range turns into from_ms/to_ms); refresh stays as it is
  const submit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const q = formQuery([...new FormData(e.currentTarget).entries()].map(([k, v]) => [k, String(v)] as [string, string]));
    location.assign(withQuery(action, { ...q, ...(refresh && { refresh: String(REFRESH_S) }) }));
  };
  return (
    <form method="get" action={action} onSubmit={submit} className="mt-4 flex flex-wrap items-end gap-3 text-sm text-ink-2">
      {fields.map((f) => (
        <label key={f.name} className="flex flex-col gap-1">
          <span>{f.label}</span>
          {Array.isArray(f.kind) ? (
            <select name={f.name} defaultValue={values[f.name] ?? ""} className={input}>
              {f.kind.map(([v, t]) => <option key={v} value={v}>{t}</option>)}
            </select>
          ) : (
            <input name={f.name} type={f.kind} defaultValue={values[f.name] ?? ""} placeholder={f.placeholder} disabled={f.disabled}
              className={`${input} ${f.kind === "number" ? "w-24" : f.placeholder ? "w-72" : "w-44"} placeholder:text-ink-3 disabled:cursor-not-allowed disabled:opacity-60`} />
          )}
        </label>
      ))}
      {range && <RangeFields range={range.r} opt={range.opt} />}
      {Object.entries(hidden).map(([k, v]) => <input key={k} type="hidden" name={k} value={v} />)}
      <button className="inline-flex h-9 items-center rounded-lg border border-line-strong bg-surface px-4 text-[13px] text-ink-2 hover:text-ink">Apply</button>
    </form>
  );
}

/** Ask again every REFRESH_S seconds while on (and the tab is showing); on/off is kept in the URL as refresh=15. */
export function useRefresh(initial: boolean): [boolean, (on: boolean) => void, number] {
  const [on, setOn] = useState(initial);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (!on) return;
    const id = setInterval(() => { if (!document.hidden) setTick((t) => t + 1); }, REFRESH_S * 1000);
    const back = () => { if (!document.hidden) setTick((t) => t + 1); };
    document.addEventListener("visibilitychange", back);
    return () => { clearInterval(id); document.removeEventListener("visibilitychange", back); };
  }, [on]);
  const set = (v: boolean) => {
    setOn(v);
    const u = new URL(location.href);
    if (v) u.searchParams.set("refresh", String(REFRESH_S)); else u.searchParams.delete("refresh");
    history.replaceState(history.state, "", u);
  };
  return [on, set, tick];
}

export const clock = (t: number) => new Date(t).toISOString().slice(11, 19);
