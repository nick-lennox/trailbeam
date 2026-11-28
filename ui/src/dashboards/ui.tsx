"use client";

import { useEffect, type ComponentProps, type ReactNode } from "react";
import { X } from "lucide-react";
import type { Tone } from "./measures.ts";
import { Tooltip } from "../design/Tooltip.tsx";

/* Small parts the dashboard pages share. Menu and useSize are the design system's and the charts' (QV-591). */
export { Menu, MenuItem } from "../design/Menu.tsx";
export { useSize } from "../charts/TimeSeries.tsx";

export const TONE_VAR: Record<Tone, string> = { good: "var(--color-good)", warning: "var(--color-warning)", critical: "var(--color-critical)" };
export const TONE_TEXT: Record<Tone, string> = { good: "text-ink", warning: "!text-warning-text", critical: "!text-critical-text" };

/* Status, as the queue tables show state: a small square and a word. */
export function StateMark({ tone, children, muted = false }: { tone: Tone | null; children: ReactNode; muted?: boolean }) {
  return (
    <span className={`inline-flex items-center gap-1.5 font-sans text-[13px] font-medium whitespace-nowrap ${muted ? "text-ink-2" : "text-ink"}`}>
      <span className="size-2 shrink-0 rounded-[2px]" style={{ background: tone ? TONE_VAR[tone] : "var(--color-ink-3)" }} />
      {children}
    </span>
  );
}

export function IconBtn({ label, children, className = "", ...rest }: { label: string; children: ReactNode; className?: string } & Omit<ComponentProps<"button">, "className" | "children">) {
  return (
    <Tooltip content={label} className="inline-flex shrink-0">
      <button type="button" aria-label={label} className={`grid size-[26px] shrink-0 place-items-center rounded-md text-ink-3 hover:bg-sunken hover:text-ink ${className}`} {...rest}>
        {children}
      </button>
    </Tooltip>
  );
}

export function Modal({ title, onClose, children, footer, width = "max-w-[440px]" }: { title: string; onClose: () => void; children: ReactNode; footer?: ReactNode; width?: string }) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);
  return (
    <div className="fixed inset-0 z-50 grid place-items-center overflow-y-auto bg-[rgb(5_6_20/0.6)] p-6" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div role="dialog" aria-label={title} className={`w-full ${width} overflow-hidden rounded-panel border border-line bg-surface text-ink shadow-panel`}>
        <div className="flex items-center gap-3 pt-4 pr-4 pl-5">
          <h2 className="text-base font-semibold">{title}</h2>
          <IconBtn label="Close" className="ml-auto" onClick={onClose}><X size={16} /></IconBtn>
        </div>
        {children}
        {footer && <div className="flex items-center justify-end gap-2 border-t border-line bg-canvas px-5 py-3">{footer}</div>}
      </div>
    </div>
  );
}

export const FIELD = "h-9 w-full rounded-lg border border-line-strong bg-surface px-2.5 text-[13px] text-ink !outline-none focus:border-accent";
export function Labeled({ label, hint, children }: { label: string; hint?: string; children: ReactNode }) {
  return (
    <label className="grid gap-1.5">
      <span className="text-[13px] text-ink-2">{label}</span>
      {children}
      {hint && <span className="text-xs text-ink-3">{hint}</span>}
    </label>
  );
}
