"use client";

import { useEffect, useState } from "react";
import { Check, ChevronDown, Clock } from "lucide-react";
import { ANY_TIME, PRESETS, anyTime, preset, parseRange, custom, toInput, fromInput, type TimeRange } from "../model/timerange.ts";

/* The time range, as a log explorer offers it: a button that names the range, opening a list of common ranges, a box
   to type one, and a from and to for an exact span. Every page with a time range uses this one picker. `now` is where a
   range that ends now ends: the clock, or the end of the hosted sample hour. `any`: offer Any time first, for a list
   that opens on all its history (Messages). */
export function TimePicker({ time, setTime, now, any = false }: { time: TimeRange; setTime: (t: TimeRange) => void; now: number; any?: boolean }) {
  const [open, setOpen] = useState(false);
  const [text, setText] = useState("");
  const [from, setFrom] = useState(toInput(time.from));
  const [to, setTo] = useState(toInput(time.to));
  const typed = text.trim() ? parseRange(text, now) : null;
  const exact = custom(fromInput(from), fromInput(to));
  const exactOk = Number.isFinite(exact.from) && Number.isFinite(exact.to) && exact.from < exact.to;

  useEffect(() => {
    if (!open) return;
    setText("");
    setFrom(toInput(time.from));
    setTo(toInput(time.to));
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, time.from, time.to]);

  const choose = (t: TimeRange) => { setTime(t); setOpen(false); };
  const field = "h-8 w-full rounded-md border border-line-strong bg-canvas px-2 font-mono text-caption text-ink !outline-none focus:border-accent";

  return (
    <div className="relative">
      <button
        onClick={() => setOpen(!open)}
        aria-expanded={open}
        aria-haspopup="dialog"
        className={`inline-flex h-10 items-center gap-2 rounded-lg border bg-surface px-3 text-[13px] whitespace-nowrap text-ink hover:border-ink-3 ${open ? "border-accent" : "border-line-strong"}`}
      >
        <Clock size={14} className="text-ink-3" />
        <span className={time.preset ? "" : "font-mono text-caption"}>{time.label}</span>
        <ChevronDown size={14} className="text-ink-3" />
      </button>
      {open && (
        <>
          <button aria-label="Close the time range list" className="fixed inset-0 z-30 cursor-default" onClick={() => setOpen(false)} />
          <div role="dialog" aria-label="Choose a time range" className="absolute top-full right-0 z-40 mt-1.5 w-[340px] overflow-hidden rounded-xl border border-line-strong bg-surface text-[13px] shadow-panel">
            <form className="border-b border-line p-3" onSubmit={(e) => { e.preventDefault(); if (typed) choose(typed); }}>
              <input
                autoFocus
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder="Type a range: 45m, 4h, 2d, 14:05 to 14:15"
                aria-label="Type a time range"
                spellCheck={false}
                className={`${field} font-sans text-[13px] placeholder:text-ink-3`}
              />
              <p className={`mt-1.5 text-xs ${text.trim() && !typed ? "text-critical-text" : "text-ink-3"}`}>
                {!text.trim() ? "Press Enter to apply what you type." : typed ? <>Enter to show <b className="font-medium text-ink">{typed.label}</b></> : "That isn't a range. Try 45m, 2d or 14:05 to 14:15."}
              </p>
            </form>
            <ul className="border-b border-line p-1.5">
              {[...(any ? [{ id: ANY_TIME, label: "Any time" }] : []), ...PRESETS].map((p) => {
                const on = time.preset === p.id;
                return (
                  <li key={p.id}>
                    <button onClick={() => choose(p.id === ANY_TIME ? anyTime(now) : preset(p.id, now))} aria-pressed={on} className={`flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-sunken ${on ? "text-ink" : "text-ink-2"}`}>
                      <span className="w-4">{on && <Check size={14} className="text-accent" />}</span>
                      <span className={`flex-1 ${on ? "font-medium" : ""}`}>{p.label}</span>
                      {p.id !== ANY_TIME && <span className="font-mono text-micro text-ink-3">{p.id}</span>}
                    </button>
                  </li>
                );
              })}
            </ul>
            <form className="grid gap-2 p-3" onSubmit={(e) => { e.preventDefault(); if (exactOk) choose(exact); }}>
              <p className="text-xs font-medium text-ink-2">Exact dates and times</p>
              <div className="grid grid-cols-[42px_minmax(0,1fr)] items-center gap-x-2 gap-y-1.5">
                <label htmlFor="tp-from" className="text-xs text-ink-3">From</label>
                <input id="tp-from" type="datetime-local" value={from} max={to} onChange={(e) => setFrom(e.target.value)} className={field} />
                <label htmlFor="tp-to" className="text-xs text-ink-3">To</label>
                <input id="tp-to" type="datetime-local" value={to} min={from} onChange={(e) => setTo(e.target.value)} className={field} />
              </div>
              <div className="flex items-center justify-between gap-2">
                <p className="text-xs text-ink-3">{exactOk ? "" : "From must be before To."}</p>
                <button type="submit" disabled={!exactOk} className="h-8 rounded-md bg-brand px-3 text-[13px] font-medium text-on-brand hover:bg-brand-strong disabled:opacity-40">Apply</button>
              </div>
            </form>
          </div>
        </>
      )}
    </div>
  );
}
