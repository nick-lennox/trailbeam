"use client";

import { useEffect, useId, useLayoutEffect, useRef, useState, type CSSProperties, type HTMLAttributes, type ReactNode } from "react";
import { createPortal } from "react-dom";

/*
  A tooltip: a short note about the thing under the pointer, shown on hover and on focus.
  It is the app's own, never the browser's `title`, so it takes the theme's surface, line
  and ink, and can hold marks as well as words.

  The trigger is a span around `children`; `className` and `style` go on that span, so the
  trigger can be the positioned element itself. The note is drawn in the nearest theme
  scope ([data-app-theme]) with fixed position, so a scrolling or contained ancestor never
  clips it. It sits above the trigger, centered on it, kept inside the window, and goes
  below when there is no room above. It closes on Escape and when anything scrolls.

  With no content (undefined, null, false or "") it is the plain span, so a note that only some
  rows have needs no second branch.

  Rules (design system, Tooltip): one or two sentences, never a control, never the only
  place something essential is said. Styles are in design.css (.ui-tooltip).
*/
const GAP = 8; // between the trigger and the note
const EDGE = 8; // the closest the note comes to the window's edge

type Props = Omit<HTMLAttributes<HTMLSpanElement>, "content"> & {
  content: ReactNode;
  children: ReactNode;
  delay?: number; // ms the pointer rests before the note shows; focus shows it at once
  onOpenChange?: (open: boolean) => void;
};

export function Tooltip({ content, children, delay = 120, onOpenChange, ...rest }: Props) {
  const id = useId();
  const trigger = useRef<HTMLSpanElement>(null);
  const tip = useRef<HTMLSpanElement>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const [open, setOpen] = useState(false);
  const [place, setPlace] = useState<{ style: CSSProperties; side: "above" | "below" } | null>(null);

  const set = (next: boolean) => { setOpen(next); if (!next) setPlace(null); onOpenChange?.(next); };
  const show = (wait: number) => { clearTimeout(timer.current); timer.current = setTimeout(() => set(true), wait); };
  const hide = () => { clearTimeout(timer.current); set(false); };
  useEffect(() => () => clearTimeout(timer.current), []);

  useLayoutEffect(() => {
    if (!open || !trigger.current || !tip.current) return;
    const t = trigger.current.getBoundingClientRect(), n = tip.current.getBoundingClientRect();
    const above = t.top - GAP - n.height >= EDGE;
    setPlace({
      side: above ? "above" : "below",
      style: {
        left: Math.max(EDGE, Math.min(t.left + t.width / 2 - n.width / 2, window.innerWidth - n.width - EDGE)),
        top: above ? t.top - GAP - n.height : t.bottom + GAP,
      },
    });
  }, [open, content]);

  useEffect(() => {
    if (!open) return;
    const key = (e: KeyboardEvent) => { if (e.key === "Escape") hide(); };
    window.addEventListener("keydown", key);
    window.addEventListener("scroll", hide, true);
    window.addEventListener("resize", hide);
    return () => { window.removeEventListener("keydown", key); window.removeEventListener("scroll", hide, true); window.removeEventListener("resize", hide); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  if (content === undefined || content === null || content === false || content === "") return <span {...rest}>{children}</span>;
  return (
    <span
      {...rest}
      ref={trigger}
      aria-describedby={open ? id : undefined}
      onMouseEnter={() => show(delay)}
      onMouseLeave={hide}
      onFocus={() => show(0)}
      onBlur={hide}
    >
      {children}
      {open && createPortal(
        <span ref={tip} id={id} role="tooltip" className="ui-tooltip" data-side={place?.side} style={place ? place.style : { left: 0, top: 0, visibility: "hidden" }}>
          {content}
        </span>,
        trigger.current?.closest("[data-app-theme]") ?? document.body,
      )}
    </span>
  );
}
