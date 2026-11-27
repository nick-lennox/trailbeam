import type { ReactNode } from "react";

/*
  Dark product frame — the "night" surface. Used for the hero product shot and
  code samples. `glow` adds the brand-tinted 1px edge for the single hero shot.
*/
export function NightWindow({
  title, meta, children, glow = false, className = "",
}: { title?: ReactNode; meta?: ReactNode; children: ReactNode; glow?: boolean; className?: string }) {
  const frame = (
    <div className={`overflow-hidden bg-night text-small text-night-ink-2 ${glow ? "rounded-panel" : "rounded-card border border-night-line shadow-panel"} ${className}`}>
      {(title || meta) && (
        <div className="flex items-center gap-3 border-b border-night-line px-4 py-2.5 font-mono text-caption text-night-ink-3">
          {title}
          {meta && <span className="ml-auto">{meta}</span>}
        </div>
      )}
      {children}
    </div>
  );
  if (!glow) return frame;
  return <div className="rounded-panel bg-gradient-to-b from-brand/30 via-brand/5 to-transparent p-px shadow-panel">{frame}</div>;
}

export function WindowDots() {
  return (
    <span className="flex gap-1.5" aria-hidden>
      <span className="size-2.5 rounded-full bg-white/15" />
      <span className="size-2.5 rounded-full bg-white/15" />
      <span className="size-2.5 rounded-full bg-white/15" />
    </span>
  );
}
