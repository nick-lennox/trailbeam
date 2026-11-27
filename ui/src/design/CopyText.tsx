"use client";

import { useEffect, useState, type ReactNode } from "react";
import { Check, Copy, Link2 } from "lucide-react";
import { Tooltip } from "./Tooltip.tsx";

/*
  A labelled button that copies text, or a link to a path on this site (resolved when clicked, so it carries the
  current origin). `value` and `path` may be functions, for a link that pins what the page shows at the moment of
  the click. When the browser won't let the page use the clipboard, the text is put in a box, selected, for
  Ctrl+C. The hosted app's CopyText, shared; its note is the design system's Tooltip, never `title`.
*/
export function CopyText({ value, path, label, icon = "copy", className = "", children }: {
  value?: string | (() => string); path?: string | (() => string); label: string; icon?: "copy" | "link"; className?: string; children?: ReactNode;
}) {
  const [done, setDone] = useState<null | "ok" | { failed: string }>(null);
  useEffect(() => {
    if (done !== "ok") return;
    const t = setTimeout(() => setDone(null), 1800);
    return () => clearTimeout(t);
  }, [done]);
  const copy = async () => {
    const p = typeof path === "function" ? path() : path;
    const text = p !== undefined ? new URL(p, window.location.origin).toString() : typeof value === "function" ? value() : value ?? "";
    try { await navigator.clipboard.writeText(text); setDone("ok"); } catch { setDone({ failed: text }); }
  };
  const Icon = done === "ok" ? Check : icon === "link" ? Link2 : Copy;
  const failed = done !== null && done !== "ok" ? done.failed : null;
  return (
    <span className="inline-flex items-center gap-2">
      <Tooltip content={children ? label : undefined}>
        <button type="button" onClick={copy} aria-label={label}
          className={`inline-flex items-center gap-1.5 text-ink-3 hover:text-ink ${done === "ok" ? "!text-good-text" : ""} ${className}`}>
          {children}
          <Icon size={13} aria-hidden className="shrink-0" />
          <span aria-live="polite" className={children ? "sr-only" : ""}>{done === "ok" ? "Copied" : children ? "" : label}</span>
        </button>
      </Tooltip>
      {failed !== null && (
        <input readOnly value={failed} aria-label="Your browser didn't allow copying: press Ctrl+C or Cmd+C" autoFocus
          onFocus={(e) => e.currentTarget.select()} onBlur={() => setDone(null)}
          className="w-64 rounded-control border border-line bg-surface px-2 py-0.5 font-mono text-caption text-ink" />
      )}
    </span>
  );
}
