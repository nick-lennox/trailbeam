import type { ReactNode } from "react";

/*
  Badges label state. Status tones (good…critical) must include an icon or a
  dot plus text — color is never the only signal.
*/
const DAY = {
  neutral: "bg-sunken text-ink-2 ring-line",
  outline: "bg-surface text-ink-2 ring-line shadow-card",
  brand: "bg-brand-soft text-brand ring-brand/15",
  good: "bg-good-soft text-good-text ring-good-ring",
  warning: "bg-warning-soft text-warning-text ring-warning-ring",
  serious: "bg-serious-soft text-serious-text ring-serious-ring",
  critical: "bg-critical-soft text-critical-text ring-critical-ring",
} as const;

const NIGHT = {
  neutral: "bg-white/5 text-night-ink-2 ring-night-line",
  outline: "bg-transparent text-night-ink-2 ring-night-line",
  brand: "bg-brand-night/15 text-brand-night-text ring-brand-night/20",
  good: "bg-good-night/15 text-good-night ring-good-night/20",
  warning: "bg-warning-night/15 text-warning-night ring-warning-night/20",
  serious: "bg-serious-night/15 text-serious-night ring-serious-night/20",
  critical: "bg-critical-night/15 text-critical-night-text ring-critical-night/20",
} as const;

const DOT = {
  neutral: "bg-ink-3", outline: "bg-ink-3", brand: "bg-brand",
  good: "bg-good", warning: "bg-warning", serious: "bg-serious", critical: "bg-critical",
} as const;

export type BadgeTone = keyof typeof DAY;

export function Badge({
  tone = "neutral",
  night = false,
  dot = false,
  live = false,
  children,
}: {
  tone?: BadgeTone;
  night?: boolean;
  dot?: boolean;
  live?: boolean;
  children: ReactNode;
}) {
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium ring-1 ring-inset ${(night ? NIGHT : DAY)[tone]}`}>
      {(dot || live) && <span className={`size-1.5 rounded-full ${DOT[tone]} ${live ? "live-dot" : ""}`} aria-hidden />}
      {children}
    </span>
  );
}
