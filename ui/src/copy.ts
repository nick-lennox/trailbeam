/* Copying a one-time value (an invite link, a new token) to the clipboard, shared by the People and Tokens views. */
import { useEffect, useState } from "react";

/** `copied` for 2 seconds after a copy; `select` when the clipboard can't be used, so the page asks for Ctrl+C. */
export type CopyState = "idle" | "copied" | "select";

export function useCopy(text: string): [CopyState, () => void] {
  const [state, setState] = useState<CopyState>("idle");
  useEffect(() => {
    if (state !== "copied") return;
    const t = setTimeout(() => setState("idle"), 2000);
    return () => clearTimeout(t);
  }, [state]);
  const copy = () => {
    const clip = typeof navigator !== "undefined" ? navigator.clipboard : undefined;
    if (!clip?.writeText) return setState("select");
    clip.writeText(text).then(() => setState("copied"), () => setState("select"));
  };
  return [state, copy];
}
