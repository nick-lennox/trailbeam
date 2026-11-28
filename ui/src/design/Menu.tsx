"use client";

import { useState, type ReactNode } from "react";

/* A menu that opens under its button and closes on a click anywhere else (the hosted dashboards' Menu, QV-583). */
export function Menu({ button, children, align = "right", width = "w-56" }: {
  button: (open: boolean, toggle: () => void) => ReactNode;
  children: (close: () => void) => ReactNode;
  align?: "left" | "right";
  width?: string;
}) {
  const [open, setOpen] = useState(false);
  return (
    <span className="relative inline-flex">
      {button(open, () => setOpen(!open))}
      {open && (
        <>
          <button aria-label="Close the menu" className="fixed inset-0 z-30 cursor-default" onClick={() => setOpen(false)} />
          <div role="menu" className={`absolute top-full z-40 mt-1.5 ${width} rounded-card border border-line-strong bg-surface p-1 text-left text-[13px] shadow-raised ${align === "right" ? "right-0" : "left-0"}`}>
            {children(() => setOpen(false))}
          </div>
        </>
      )}
    </span>
  );
}

export function MenuItem({ onClick, children, danger = false, disabled = false }: { onClick: () => void; children: ReactNode; danger?: boolean; disabled?: boolean }) {
  return (
    <button role="menuitem" type="button" disabled={disabled} onClick={onClick} className={`flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-sunken disabled:opacity-40 ${danger ? "text-critical-text" : "text-ink"}`}>
      {children}
    </button>
  );
}
