/*
  The Tokens page's parts (users and roles, phase 2; proposal on QV-149, section 7): the table of ingest tokens or of
  your own personal API tokens, the form that creates one, and the new token, shown once to copy. They take the
  API's data as props (types.ts) and call back for every change: the host sends the request (self-hosted Trailbeam:
  /api/tokens/ingest for admins, /api/me/tokens for anyone signed in) and passes the new list back in.
  The legacy shared token (TRAILBEAM_TOKEN) is listed with the ingest tokens but can't be revoked here: it is set in
  the server's environment. No view ever has a token's value except the one a create answered with.
*/
import { useId, useState, type FormEvent } from "react";
import { useCopy } from "../copy.ts";
import { btn, fmtWhen } from "../people/People.tsx";
import type { IngestToken, PersonalToken, SharedToken } from "./types.ts";

export type TokenKind = "ingest" | "personal";
type AnyToken = IngestToken | PersonalToken;

export const INGEST_TEXT = "An ingest token lets one service send spans to this Trailbeam, and nothing else: it can't read anything. "
  + "Give each service its own, so you can revoke one without touching the others.";
export const PERSONAL_TEXT = "A personal API token acts as you, with your role, for scripts and the API "
  + "(send it as Authorization: Bearer). It stops working when it expires, when you revoke it, or if your account is disabled or removed.";

/** Shown while ingest is open (no TRAILBEAM_TOKEN, no ingest token yet): the first token closes it. */
export const OPEN_INGEST = "Ingest is open: any service that can reach this server can send spans without a token. "
  + "Creating the first ingest token closes it. Within 5 seconds, every service that sends spans without a token gets 401 and its spans are lost, "
  + "so give the token to your services as soon as you have it.";
export const OPEN_INGEST_CONFIRM = "Close open ingest: services without a token are refused from now on";

export const STATUS_TEXT: Record<AnyToken["status"], string> = { active: "Active", expired: "Expired", revoked: "Revoked" };

/** How long a new personal API token works; null is until it is revoked. */
export const EXPIRY: readonly [number | null, string][] = [[30, "30 days"], [90, "90 days"], [365, "1 year"], [null, "Until revoked"]];
export const DEFAULT_EXPIRY = 90;

/** The part of a token the page can show: its first 8 characters, never more. */
export const shownPrefix = (prefix: string) => `${prefix}…`;

/** The legacy shared token's line: what it is, what it works for, and how to stop it (not from this page). */
export function sharedSentence(s: SharedToken): string {
  if (!s.set) return "No legacy shared token: TRAILBEAM_TOKEN isn't set on this server.";
  const what = s.works_for.map((w) => (w === "metrics" ? "/metrics" : "sending spans")).join(" and ");
  return `Works for ${what}. It is set in the server's environment, so it can't be revoked here: to stop it, `
    + "remove TRAILBEAM_TOKEN (or --token-file) and restart the server.";
}

export function secretSentence(kind: TokenKind, name: string): string {
  const what = kind === "ingest"
    ? `The ingest token "${name}". Give it to the service as OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<token>.`
    : `Your personal API token "${name}". Send it as Authorization: Bearer <token>.`;
  return `${what} It isn't shown again: copy it now, and keep it somewhere safe. If it is lost, create a new one and revoke this one.`;
}

export function revokeSentence(kind: TokenKind, t: Pick<AnyToken, "name">): string {
  return kind === "ingest"
    ? `Revoke "${t.name}"? A service still using it gets 401 within 30 seconds and its spans are lost. This can't be undone.`
    : `Revoke "${t.name}"? Anything still using it gets 401 within 20 seconds. This can't be undone.`;
}

const when = (ms: number | null, none: string) => (ms === null ? none : fmtWhen(ms));

export type TokensTableProps = {
  kind: TokenKind;
  tokens: AnyToken[];
  /** ingest only: the legacy shared token, listed first when it is set */
  shared?: SharedToken;
  /** the token whose revoke is being sent: its button is off until the host passes the new list */
  pending?: number | null;
  onRevoke: (t: AnyToken) => void;
};

/** One row per token, in the API's order; revoked and expired ones stay, so you can see what stopped and when. */
export function TokensTable({ kind, tokens, shared, pending = null, onRevoke }: TokensTableProps) {
  const [revoking, setRevoking] = useState<number | null>(null);
  const cell = "px-3 py-3";
  return (
    <table className="w-full text-[15px]">
      <thead>
        <tr className="border-b border-line text-left font-mono text-caption text-ink-3">
          <th className="px-5 py-2 font-medium">Name</th>
          <th className="px-3 py-2 font-medium">Starts with</th>
          <th className="px-3 py-2 font-medium">Status</th>
          <th className="px-3 py-2 font-medium">Created</th>
          {kind === "personal" && <th className="px-3 py-2 font-medium">Expires</th>}
          <th className="px-3 py-2 font-medium">Last used</th>
          <th className="px-5 py-2 text-right font-medium"><span className="sr-only">Actions</span></th>
        </tr>
      </thead>
      <tbody>
        {shared?.set && (
          <tr className="border-b border-line/60 align-top" data-token="shared">
            <td className="px-5 py-3 text-ink">{shared.label}</td>
            <td className={`${cell} font-mono text-caption text-ink-3`}>not shown</td>
            <td className={`${cell} text-sm text-ink-2`}>Active</td>
            <td className={`${cell} max-w-[44ch] text-sm text-ink-3`} colSpan={3}>{sharedSentence(shared)}</td>
          </tr>
        )}
        {tokens.length === 0 && (
          <tr><td className="px-5 py-4 text-ink-3" colSpan={kind === "personal" ? 7 : 6}>
            {kind === "ingest" ? "No ingest tokens yet." : "You have no personal API tokens yet."}
          </td></tr>
        )}
        {tokens.map((t) => {
          const active = t.status === "active";
          return (
            <tr key={t.id} className="border-b border-line/60 align-top last:border-0" data-token={t.id}>
              <td className="px-5 py-3 text-ink">{t.name}</td>
              <td className={`${cell} font-mono text-caption text-ink-3`}>{shownPrefix(t.prefix)}</td>
              <td className={`${cell} text-sm ${active ? "text-ink-2" : "text-ink-3"}`}>
                {STATUS_TEXT[t.status]}
                {t.revoked_at_ms !== null && <div className="font-mono text-caption text-ink-3">{fmtWhen(t.revoked_at_ms)}</div>}
              </td>
              <td className={`${cell} font-mono text-caption text-ink-3`}>
                {fmtWhen(t.created_at_ms)}
                {"created_by" in t && t.created_by && <div>by {t.created_by}</div>}
              </td>
              {kind === "personal" && <td className={`${cell} font-mono text-caption text-ink-3`}>{when((t as PersonalToken).expires_at_ms, "never")}</td>}
              <td className={`${cell} font-mono text-caption text-ink-3`}>{when(t.last_used_ms, "never")}</td>
              <td className="px-5 py-3">
                {active && (revoking === t.id ? (
                  <div className="flex flex-wrap items-center justify-end gap-2" role="group" aria-label={`Revoke ${t.name}`}>
                    <span className="max-w-[34ch] text-sm text-ink-2">{revokeSentence(kind, t)}</span>
                    <button type="button" className={`${btn} text-critical-text`} disabled={pending === t.id}
                      onClick={() => { setRevoking(null); onRevoke(t); }}>Revoke</button>
                    <button type="button" className={btn} onClick={() => setRevoking(null)}>Cancel</button>
                  </div>
                ) : (
                  <div className="flex justify-end">
                    <button type="button" className={btn} disabled={pending === t.id} onClick={() => setRevoking(t.id)}>Revoke</button>
                  </div>
                ))}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

export type NewTokenRequest = { name: string; expires_in_days?: number | null; close_open_ingest?: boolean };

export type CreateTokenProps = {
  kind: TokenKind;
  /** ingest only: ingest is open now, so the form asks to confirm closing it */
  ingestOpen?: boolean;
  /** resolves when the token is made (the form clears), rejects with a message to show (the form keeps what was typed) */
  onCreate: (r: NewTokenRequest) => Promise<void>;
};

/** A name (what the token is for), and for a personal token how long it works. */
export function CreateToken({ kind, ingestOpen = false, onCreate }: CreateTokenProps) {
  const id = useId();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const form = e.currentTarget;
    const data = new FormData(form);
    const name = String(data.get("name") ?? "").trim();
    if (!name) return;
    const r: NewTokenRequest = { name };
    if (kind === "personal") {
      const days = String(data.get("expires") ?? "");
      r.expires_in_days = days === "" ? null : Number(days);
    } else if (ingestOpen) {
      r.close_open_ingest = data.get("close_open_ingest") === "on";
    }
    setBusy(true);
    setError("");
    onCreate(r).then(() => form.reset(), (err: Error) => setError(err.message)).finally(() => setBusy(false));
  };
  const input = "rounded-full border border-line bg-surface px-3 py-1 text-sm text-ink placeholder:text-ink-3";
  return (
    <form onSubmit={submit} className="flex flex-wrap items-end gap-3 text-sm text-ink-2"
      aria-label={kind === "ingest" ? "Create an ingest token" : "Create a personal API token"}>
      <label className="flex flex-col gap-1" htmlFor={`${id}-name`}>
        <span>{kind === "ingest" ? "Service" : "What it's for"}</span>
        <input id={`${id}-name`} name="name" type="text" required maxLength={200} autoComplete="off"
          placeholder={kind === "ingest" ? "orders-api prod" : "laptop scripts"} className={`${input} w-64`} />
      </label>
      {kind === "personal" && (
        <label className="flex flex-col gap-1" htmlFor={`${id}-expires`}>
          <span>Works for</span>
          <select id={`${id}-expires`} name="expires" defaultValue={String(DEFAULT_EXPIRY)} className={input}>
            {EXPIRY.map(([d, label]) => <option key={label} value={d === null ? "" : String(d)}>{label}</option>)}
          </select>
        </label>
      )}
      {kind === "ingest" && ingestOpen && (
        <label className="flex w-full items-start gap-2 text-warning-text" htmlFor={`${id}-close`}>
          <input id={`${id}-close`} name="close_open_ingest" type="checkbox" required className="mt-1" />
          <span>{OPEN_INGEST_CONFIRM}</span>
        </label>
      )}
      <button type="submit" disabled={busy}
        className="rounded-full border border-ink bg-ink px-4 py-1 text-sm text-surface hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60">
        {busy ? "Creating" : "Create token"}
      </button>
      {error && <p className="w-full text-critical-text" role="alert">{error}</p>}
    </form>
  );
}

export type OneTimeTokenProps = { kind: TokenKind; name: string; secret: string; onDismiss: () => void };

/** The token a create answered with, to copy now: it is shown only once. */
export function OneTimeToken({ kind, name, secret, onDismiss }: OneTimeTokenProps) {
  const [copied, copy] = useCopy(secret);
  return (
    <div className="rounded-[16px] border border-brand/60 bg-surface px-5 py-4 text-[15px] leading-[1.5] text-ink-2" role="status">
      <p className="max-w-[80ch]">{secretSentence(kind, name)}</p>
      <div className="mt-3 flex flex-wrap items-center gap-2">
        <input readOnly value={secret} aria-label="New token" onFocus={(e) => e.currentTarget.select()} autoFocus={copied === "select"}
          className="min-w-0 flex-1 basis-96 rounded-full border border-line bg-surface px-3 py-1 font-mono text-caption text-ink" />
        <button type="button" className={btn} onClick={copy}>{copied === "copied" ? "Copied" : "Copy token"}</button>
        <button type="button" className={btn} onClick={onDismiss}>Done</button>
      </div>
      <p className="sr-only" aria-live="polite">
        {copied === "copied" ? "Token copied to the clipboard." : copied === "select" ? "Couldn't use the clipboard: select the token and press Ctrl+C or Cmd+C to copy." : ""}
      </p>
    </div>
  );
}
