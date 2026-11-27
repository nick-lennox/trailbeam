"use client";

import { useEffect, useId, useRef, useState, type FormEvent } from "react";
import { Search } from "lucide-react";
import { useNav } from "./nav.tsx";

/*
  The top bar's search, in TopBar's `search` slot, shared by both products: a GET form to the host's message
  search (`action`, with the text as `name`), which finds a message by its ID and lists a tenant's messages
  (tenant:acme). While typing, it suggests the tenants the host's `suggest` names (the hosted app's
  /api/w/<slug>/tenants, the open-source server's /api/tenants/search), as tenant:<name>.
  A host whose search page can't read the text itself passes `go`: it turns the text into the page to open,
  and the form moves there through the host's Nav; without scripts the form still submits to `action`.
  Command+K on a Mac, Ctrl+K elsewhere, puts the cursor in it. The hint is drawn after mount, once the
  browser has said which of the two it is.
*/
export type MessageSearchProps = {
  /** where the form goes: the host's message search */
  action: string;
  /** the text's parameter name (default q) */
  name?: string;
  /** tenant names that start with the text, for the suggestions; none when absent */
  suggest?: (text: string, signal: AbortSignal) => Promise<string[]>;
  /** the page for the text, or null to stay; the form submits to `action` when absent */
  go?: (text: string) => string | null;
  placeholder?: string;
};

/** The text without a "tenant:" prefix: what a tenant search asks for. */
export const tenantText = (q: string) => q.replace(/^tenant:\s*/i, "").trim();

export function MessageSearch({ action, name = "q", suggest, go, placeholder = "Find a message by ID, trace ID, or a tenant" }: MessageSearchProps) {
  const nav = useNav();
  const list = useId();
  const input = useRef<HTMLInputElement>(null);
  const [mac, setMac] = useState<boolean | null>(null);
  const [q, setQ] = useState("");
  const [tenants, setTenants] = useState<string[]>([]);

  useEffect(() => {
    const text = tenantText(q);
    if (!suggest || text.length < 1) { setTenants([]); return; }
    const ctl = new AbortController();
    const t = setTimeout(() => {
      suggest(text, ctl.signal).then((x) => { if (!ctl.signal.aborted) setTenants(x); }, () => undefined);
    }, 150);
    return () => { clearTimeout(t); ctl.abort(); };
  }, [q, suggest]);

  useEffect(() => {
    const isMac = /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);
    setMac(isMac);
    const onKey = (e: KeyboardEvent) => {
      if (e.key.toLowerCase() !== "k" || e.altKey || e.shiftKey) return;
      if (isMac ? !e.metaKey || e.ctrlKey : !e.ctrlKey || e.metaKey) return;
      e.preventDefault();
      input.current?.focus();
      input.current?.select();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const submit = go && ((e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const href = go(q.trim());
    if (href) nav.push(href);
  });

  return (
    <form role="search" action={action} onSubmit={submit || undefined}
      className="flex w-full max-w-md items-center gap-2 rounded-lg border border-line bg-surface px-3 py-1.5 text-sm focus-within:border-brand focus-within:ring-4 focus-within:ring-brand/10">
      <Search size={14} className="shrink-0 text-ink-3" aria-hidden />
      <input ref={input} name={name} value={q} onChange={(e) => setQ(e.target.value)} placeholder={placeholder} aria-label={placeholder}
        list={tenants.length ? list : undefined} autoComplete="off"
        className="w-full min-w-0 bg-transparent text-ink outline-none placeholder:text-ink-3" />
      {tenants.length > 0 && <datalist id={list}>{tenants.map((t) => <option key={t} value={`tenant:${t}`}>Tenant</option>)}</datalist>}
      {mac !== null && (
        <kbd className="whitespace-nowrap rounded border border-line px-1.5 font-mono text-micro text-ink-3">{mac ? "⌘K" : "Ctrl K"}</kbd>
      )}
    </form>
  );
}
