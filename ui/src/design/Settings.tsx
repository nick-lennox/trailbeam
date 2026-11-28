"use client";

import { useId, type ReactNode, type InputHTMLAttributes, type SelectHTMLAttributes } from "react";
import { Button } from "./Button.tsx";
import { Tooltip } from "./Tooltip.tsx";

/*
  The parts the account pages share (People, Tokens, the account screen), as the hosted app's settings draw them:
  a titled section, a labeled field, a state mark, and the boxes for a problem or a one-time value. Labels are in
  the UI face; the mono face is kept for values (emails, token prefixes, times).
*/

/** A titled part of a page: heading, one line about it, an action on the right, then its content. */
export function SettingsSection({ id, title, about, action, children }: { id?: string; title: string; about?: ReactNode; action?: ReactNode; children: ReactNode }) {
  const heading = useId();
  return (
    <section id={id} className="scroll-mt-20" aria-labelledby={heading}>
      <div className="flex flex-wrap items-end gap-x-4 gap-y-2">
        <div className="min-w-0">
          <h2 id={heading} className="text-base font-semibold text-ink">{title}</h2>
          {about && <p className="mt-1 max-w-2xl text-sm leading-relaxed text-ink-2">{about}</p>}
        </div>
        {action && <div className="ml-auto flex items-center gap-2">{action}</div>}
      </div>
      <div className="mt-4">{children}</div>
    </section>
  );
}

/** A text field's and a select's look, for a form the field parts below don't fit. */
export const INPUT = "h-9 w-full min-w-0 rounded-control border border-line bg-canvas px-3 text-sm text-ink outline-none transition-colors placeholder:text-ink-3 hover:border-line-strong focus:border-brand disabled:cursor-not-allowed disabled:opacity-60";
export const SELECT = "h-9 rounded-control border border-line bg-canvas px-2.5 text-sm text-ink outline-none hover:border-line-strong focus:border-brand disabled:cursor-not-allowed disabled:opacity-60";

/** A labeled text field; `mono` for a value (a code, an ID), never for words. */
export function Field({ label, hint, mono = false, className = "", ...props }: { label: string; hint?: ReactNode; mono?: boolean } & InputHTMLAttributes<HTMLInputElement>) {
  const id = useId();
  return (
    <div className={`grid min-w-0 content-start gap-1.5 ${className}`}>
      <label htmlFor={id} className="text-xs font-medium text-ink-2">{label}</label>
      <input id={id} aria-describedby={hint ? `${id}-hint` : undefined} spellCheck={false} autoComplete="off" {...props}
        className={`${INPUT} ${mono ? "font-mono text-caption" : ""}`} />
      {hint && <span id={`${id}-hint`} className="text-xs text-ink-3">{hint}</span>}
    </div>
  );
}

/** A labeled select. */
export function SelectField({ label, children, className = "", ...props }: { label: string; children: ReactNode } & SelectHTMLAttributes<HTMLSelectElement>) {
  const id = useId();
  return (
    <div className={`grid content-start gap-1.5 ${className}`}>
      <label htmlFor={id} className="text-xs font-medium text-ink-2">{label}</label>
      <select id={id} {...props} className={SELECT}>{children}</select>
    </div>
  );
}

export const FormError = ({ children }: { children?: ReactNode }) =>
  children ? <p role="alert" className="text-sm text-critical-text">{children}</p> : null;

/** A problem the page shows above its content: a refused change, a list that couldn't be read. */
export const Problem = ({ children }: { children?: ReactNode }) =>
  children ? <p role="alert" className="rounded-control border border-critical-ring bg-critical-soft px-4 py-3 text-sm text-critical-text">{children}</p> : null;

/** Something to read before going on (open ingest, for one): a warning, not a failure. */
export const Caution = ({ children }: { children: ReactNode }) =>
  <p role="note" className="rounded-control border border-warning-ring bg-warning-soft px-4 py-3 text-sm leading-relaxed text-warning-text">{children}</p>;

const SQUARE = { good: "bg-good", warning: "bg-warning", critical: "bg-critical", idle: "bg-ink-3/50" } as const;
export type StateTone = keyof typeof SQUARE;

/** A state: a small square and a plain word, with its note on hover (the app's Tooltip, never `title`). */
export function StateMark({ tone, note, children }: { tone: StateTone; note?: ReactNode; children: ReactNode }) {
  return (
    <Tooltip content={note} className="inline-flex items-center gap-1.5 whitespace-nowrap text-xs text-ink-2" tabIndex={note ? 0 : undefined}>
      <span aria-hidden className={`size-2 shrink-0 rounded-[2px] ${SQUARE[tone]}`} />
      {children}
    </Tooltip>
  );
}

/**
  Asking before a change that can't be undone, in place of the button that asked (QV-739): the sentence that says
  what happens, then Cancel and the change, named as the button was. The one way the app confirms on a row or in a
  form; a change made from a menu, with nothing on the page to stand in for, asks in the Modal instead. Cancel takes
  the focus, so Enter or Space alone never makes the change, and Escape cancels.
*/
export function Confirm({ label, children, note, action, onCancel, onConfirm, disabled = false, className = "justify-end" }: {
  /** the group's name for a screen reader: "Remove ana@example.com" */
  label: string;
  /** what happens, as a question: "Remove Ana? Their sign-ins stop working. This can't be undone." */
  children: ReactNode;
  /** a warning to read as well, in the warning colour */
  note?: ReactNode;
  /** the change's button, named as the one that asked: "Remove" */
  action: string;
  onCancel: () => void;
  onConfirm: () => void;
  /** the change is being sent: its button is off */
  disabled?: boolean;
  /** where it sits: on the row's right by default (justify-end) */
  className?: string;
}) {
  return (
    <div role="group" aria-label={label} onKeyDown={(e) => { if (e.key === "Escape") { e.stopPropagation(); onCancel(); } }}
      className={`flex flex-wrap items-center gap-2 ${className}`}>
      <span className="max-w-[34ch] text-sm text-ink-2">{children}</span>
      {note && <span className="max-w-[34ch] text-sm text-warning-text" role="note">{note}</span>}
      <Button type="button" size="sm" variant="ghost" autoFocus onClick={onCancel}>Cancel</Button>
      <Button type="button" size="sm" className="text-critical-text disabled:cursor-not-allowed disabled:opacity-50" disabled={disabled} onClick={onConfirm}>{action}</Button>
    </div>
  );
}

/** The box a one-time value sits in (a new token, an invite link): it is shown once, so it stands out. */
export const OneTimeBox = ({ children }: { children: ReactNode }) =>
  <div role="status" className="rounded-card border border-brand/60 bg-surface px-5 py-4 text-sm leading-relaxed text-ink-2 shadow-card">{children}</div>;
