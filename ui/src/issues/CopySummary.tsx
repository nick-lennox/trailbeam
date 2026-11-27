"use client";

import { useState } from "react";
import { Check, Copy } from "lucide-react";
import { Button } from "../design/Button.tsx";

/* Copies the issue as a few plain lines with its link, to paste into an incident channel or a ticket. */
export function CopySummary({ lines }: { lines: string[] }) {
  const [done, setDone] = useState(false);
  const copy = async () => {
    try { await navigator.clipboard.writeText([...lines, window.location.href].join("\n")); setDone(true); setTimeout(() => setDone(false), 1600); } catch {}
  };
  return <Button size="sm" variant="ghost" onClick={copy}>{done ? <><Check size={14} /> Copied</> : <><Copy size={14} /> Copy summary</>}</Button>;
}
