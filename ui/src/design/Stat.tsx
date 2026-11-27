"use client";

import type { ReactNode } from "react";
import { UiLink } from "./nav.tsx";
import { Sparkline } from "./Sparkline.tsx";

/*
  The design system's stat card (Card, "Stat card"): a label, one measured figure in
  the mono face at 30px, and a hint. A 4px bar on the left, in the serious or critical
  color, appears only when the figure is a problem count; the whole card then links
  to the list behind it. Optional: a sparkline of the figure over the period, beside
  it, and a footer under the hint for a small graphic.
*/
const BAR = { serious: "bg-serious", critical: "bg-critical" } as const;

export function StatCard({ label, value, hint, tone, href, spark, sparkColor, footer }: { label: string; value: ReactNode; hint?: ReactNode; tone?: keyof typeof BAR; href?: string; spark?: number[]; sparkColor?: string; footer?: ReactNode }) {
  const body = (
    <>
      {tone && <span className={`absolute inset-y-0 left-0 w-1 ${BAR[tone]}`} aria-hidden />}
      <div className="text-small text-ink-2">{label}</div>
      <div className="mt-1.5 font-mono text-[1.875rem] leading-[1.15] font-medium tabular-nums text-ink">{value}</div>
      {hint && <div className={`mt-1.5 truncate text-xs text-ink-3 ${spark ? "pr-[72px]" : ""}`}>{hint}</div>}
      {/* the trend sits in the card's bottom corner, level with the hint, so it never crowds the figure */}
      {spark && <span className="absolute right-4 bottom-4"><Sparkline data={spark} w={60} h={26} color={sparkColor} /></span>}
      {footer}
    </>
  );
  const box = "relative block overflow-hidden rounded-card border border-line bg-surface p-5 shadow-card";
  return href ? <UiLink href={href} className={`${box} transition-colors hover:border-line-strong`}>{body}</UiLink> : <div className={box}>{body}</div>;
}
