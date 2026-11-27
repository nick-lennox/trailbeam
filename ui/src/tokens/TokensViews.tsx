"use client";

/*
  The Tokens page's parts on the design system (QV-587), shared by self-hosted Trailbeam and the hosted app: the
  table of ingest tokens or of your own personal API tokens, the form that creates one, and the new token, shown
  once to copy. They take the API's data as props (types.ts) and call back for every change: the host sends the
  request (self-hosted Trailbeam: /api/tokens/ingest for admins, /api/me/tokens for anyone signed in) and passes the
  new list back in. Drawn as the hosted tables are: sans labels, mono values only. The words are Tokens.tsx's.
  A client module, reached through @trailbeam/ui/design, never the main entry (test/server-import.test.ts).
*/
import { useId, useState, type FormEvent } from "react";
import { useCopy } from "../copy.ts";
import { Button } from "../design/Button.tsx";
import { Card } from "../design/Card.tsx";
import { Field, FormError, INPUT, OneTimeBox, SelectField, StateMark, type StateTone } from "../design/Settings.tsx";
import { fmtWhen } from "../people/People.tsx";
import {
  DEFAULT_EXPIRY, EXPIRY, OPEN_INGEST_CONFIRM, STATUS_TEXT, revokeSentence, secretSentence, sharedSentence, shownPrefix,
  type AnyToken, type NewTokenRequest, type TokenKind,
} from "./Tokens.tsx";
import type { PersonalToken, SharedToken } from "./types.ts";

const TONE: Record<AnyToken["status"], StateTone> = { active: "good", expired: "idle", revoked: "idle" };

/* the design system's table cells, as the hosted tables draw them: sans labels, mono values only */
const TH = "border-b border-line px-3 py-2 text-left text-xs font-medium whitespace-nowrap text-ink-3";
const TD = "border-b border-line px-3 py-3 align-top";
const VALUE = "font-mono text-caption text-ink-2";

/** A time, or a word when there is none ("never"): the word is prose, so it is not in mono. */
const When = ({ ms, none }: { ms: number | null; none: string }) =>
  ms === null ? <span className="text-sm text-ink-3">{none}</span> : <span className={VALUE}>{fmtWhen(ms)}</span>;

export type TokensTableProps = {
  kind: TokenKind;
  tokens: AnyToken[];
  /** ingest only: the legacy shared token, listed first when it is set */
  shared?: SharedToken;
  /** the token whose revoke is being sent: its button is off until the host passes the new list */
  pending?: number | null;
  onRevoke: (t: AnyToken) => void;
};

/** One row per token, in the API's order, in a card; revoked and expired ones stay, so you can see what stopped and when. */
export function TokensTable({ kind, tokens, shared, pending = null, onRevoke }: TokensTableProps) {
  const [revoking, setRevoking] = useState<number | null>(null);
  const cols = kind === "personal" ? 7 : 6;
  return (
    <Card className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr>
            <th className={`${TH} pl-5`}>Name</th>
            <th className={TH}>Starts with</th>
            <th className={TH}>Status</th>
            <th className={TH}>Created</th>
            {kind === "personal" && <th className={TH}>Expires</th>}
            <th className={TH}>Last used</th>
            <th className={`${TH} pr-5 text-right`}><span className="sr-only">Actions</span></th>
          </tr>
        </thead>
        <tbody className="[&>tr:last-child>td]:border-0">
          {shared?.set && (
            <tr data-token="shared">
              <td className={`${TD} pl-5 text-ink`}>{shared.label}</td>
              <td className={`${TD} text-ink-3`}>not shown</td>
              <td className={TD}><StateMark tone="good">Active</StateMark></td>
              <td className={`${TD} max-w-[44ch] text-ink-3`} colSpan={cols - 3}>{sharedSentence(shared)}</td>
            </tr>
          )}
          {tokens.length === 0 && (
            <tr><td className={`${TD} pl-5 text-ink-3`} colSpan={cols}>
              {kind === "ingest" ? "No ingest tokens yet." : "You have no personal API tokens yet."}
            </td></tr>
          )}
          {tokens.map((t) => {
            const active = t.status === "active";
            return (
              <tr key={t.id} data-token={t.id}>
                <td className={`${TD} pl-5 ${active ? "text-ink" : "text-ink-2"}`}>{t.name}</td>
                <td className={TD}><span className={VALUE}>{shownPrefix(t.prefix)}</span></td>
                <td className={TD}>
                  <StateMark tone={TONE[t.status]}>{STATUS_TEXT[t.status]}</StateMark>
                  {t.revoked_at_ms !== null && <div className="mt-0.5"><span className={VALUE}>{fmtWhen(t.revoked_at_ms)}</span></div>}
                </td>
                <td className={TD}>
                  <When ms={t.created_at_ms} none="never" />
                  {"created_by" in t && t.created_by && <div className="mt-0.5 text-xs text-ink-3">by {t.created_by}</div>}
                </td>
                {kind === "personal" && <td className={TD}><When ms={(t as PersonalToken).expires_at_ms} none="never" /></td>}
                <td className={TD}><When ms={t.last_used_ms} none="never" /></td>
                <td className={`${TD} pr-5`}>
                  {active && (revoking === t.id ? (
                    <div className="flex flex-wrap items-center justify-end gap-2" role="group" aria-label={`Revoke ${t.name}`}>
                      <span className="max-w-[34ch] text-ink-2">{revokeSentence(kind, t)}</span>
                      <Button size="sm" variant="ghost" onClick={() => setRevoking(null)}>Cancel</Button>
                      <Button size="sm" className="text-critical-text disabled:opacity-50" disabled={pending === t.id}
                        onClick={() => { setRevoking(null); onRevoke(t); }}>Revoke</Button>
                    </div>
                  ) : (
                    <div className="flex justify-end">
                      <Button size="sm" variant="ghost" className="disabled:opacity-50" disabled={pending === t.id} onClick={() => setRevoking(t.id)}>Revoke</Button>
                    </div>
                  ))}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </Card>
  );
}

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
  return (
    <form onSubmit={submit} className="grid gap-2" aria-label={kind === "ingest" ? "Create an ingest token" : "Create a personal API token"}>
      <div className="flex flex-wrap items-end gap-2">
        <Field label={kind === "ingest" ? "Service" : "What it's for"} name="name" type="text" required maxLength={200}
          placeholder={kind === "ingest" ? "orders-api prod" : "laptop scripts"} className="w-72" />
        {kind === "personal" && (
          <SelectField label="Works for" name="expires" defaultValue={String(DEFAULT_EXPIRY)}>
            {EXPIRY.map(([d, label]) => <option key={label} value={d === null ? "" : String(d)}>{label}</option>)}
          </SelectField>
        )}
        <Button variant="primary" size="sm" type="submit" disabled={busy} className="h-9 disabled:opacity-60">{busy ? "Creating" : "Create token"}</Button>
      </div>
      {kind === "ingest" && ingestOpen && (
        <label className="flex items-start gap-2 text-sm text-warning-text" htmlFor={`${id}-close`}>
          <input id={`${id}-close`} name="close_open_ingest" type="checkbox" required className="mt-1 accent-brand" />
          <span>{OPEN_INGEST_CONFIRM}</span>
        </label>
      )}
      <FormError>{error}</FormError>
    </form>
  );
}

export type OneTimeTokenProps = { kind: TokenKind; name: string; secret: string; onDismiss: () => void };

/** The token a create answered with, to copy now: it is shown only once. */
export function OneTimeToken({ kind, name, secret, onDismiss }: OneTimeTokenProps) {
  const [copied, copy] = useCopy(secret);
  return (
    <OneTimeBox>
      <p className="max-w-[80ch]">{secretSentence(kind, name)}</p>
      <div className="mt-3 flex flex-wrap items-center gap-2">
        <input readOnly value={secret} aria-label="New token" onFocus={(e) => e.currentTarget.select()} autoFocus={copied === "select"}
          className={`${INPUT} flex-1 basis-96 font-mono text-caption`} />
        <Button size="sm" variant="ink" onClick={copy}>{copied === "copied" ? "Copied" : "Copy token"}</Button>
        <Button size="sm" onClick={onDismiss}>Done</Button>
      </div>
      <p className="sr-only" aria-live="polite">
        {copied === "copied" ? "Token copied to the clipboard." : copied === "select" ? "Couldn't use the clipboard: select the token and press Ctrl+C or Cmd+C to copy." : ""}
      </p>
    </OneTimeBox>
  );
}
