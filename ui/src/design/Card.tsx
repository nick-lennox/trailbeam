import type { ElementType, ReactNode } from "react";

export function Card({ children, className = "", interactive = false }: { children: ReactNode; className?: string; interactive?: boolean }) {
  return (
    <div className={`rounded-card border border-line bg-surface shadow-card ${interactive ? "transition-colors hover:border-line-strong" : ""} ${className}`}>
      {children}
    </div>
  );
}

/* Icon containers. "brand" for features, "neutral" for steps, "good" for trust. */
const TILE = {
  brand: "rounded-control bg-brand-soft text-brand",
  neutral: "rounded-full border border-line bg-canvas text-ink-2",
  good: "rounded-card bg-good-soft text-good-text",
} as const;

export function IconTile({ icon: Icon, tone = "brand", size = "md" }: { icon: ElementType; tone?: keyof typeof TILE; size?: "md" | "lg" }) {
  const box = size === "lg" ? "size-14" : "size-9";
  return (
    <span className={`grid shrink-0 place-items-center ${box} ${TILE[tone]}`} aria-hidden>
      <Icon size={size === "lg" ? 28 : 17} />
    </span>
  );
}
