"use client";

/*
  The People page's parts on the design system (QV-587), shared by self-hosted Trailbeam and the hosted app: the
  list of people with what an admin can do to each, the form that adds someone, and the one-time link an add or a
  "new link" answers with. They take the API's data as props (types.ts) and call back for every change: the host
  sends the request (self-hosted Trailbeam: PATCH, DELETE and POST on /api/users) and passes the new list back in.
  Drawn as the hosted app's member list: names in the UI face, emails and times in mono, a state as a square and a
  word with its note in the app's Tooltip. The words are People.tsx's.
  A client module, reached through @trailbeam/ui/design, never the main entry (test/server-import.test.ts).
*/
import { useState, type FormEvent } from "react";
import { useCopy } from "../copy.ts";
import { Button } from "../design/Button.tsx";
import { Card } from "../design/Card.tsx";
import { Confirm, Field, FormError, INPUT, OneTimeBox, SELECT, SelectField, StateMark, type StateTone } from "../design/Settings.tsx";
import {
  LAST_ADMIN, ROLES, STATUS_HINT, STATUS_TEXT, disableSentence, emailedSentence, fmtWhen, isLastAdmin, linkButton, linkSentence, notSentSentence,
  ownRoleSentence, removeSentence, removeSsoNote, unsentSentence,
} from "./People.tsx";
import type { Person, PersonEmail, PersonLink, PersonStatus, Role } from "./types.ts";

const TONE: Record<PersonStatus, StateTone> = { active: "good", invited: "idle", locked: "warning", disabled: "critical" };

const OFF = "disabled:cursor-not-allowed disabled:opacity-50";

export type PeopleTableProps = {
  people: Person[];
  /** the person whose change is being sent: their controls are off until the host passes the new list */
  pending?: number | null;
  onRole: (p: Person, role: Role) => void;
  onDisabled: (p: Person, disabled: boolean) => void;
  onRemove: (p: Person) => void;
  onLink: (p: Person) => void;
};

/** The change a row is asking about before it is sent: a remove, a disable, or taking away your own admin. */
type Asking = { id: number; kind: "remove" | "disable" } | { id: number; kind: "role"; role: Role };

/** One row per person, in the API's order (oldest first), in a card. */
export function PeopleTable({ people, pending = null, onRole, onDisabled, onRemove, onLink }: PeopleTableProps) {
  const [asking, setAsking] = useState<Asking | null>(null);
  const cancel = () => setAsking(null);
  return (
    <Card>
      <ul className="divide-y divide-line" aria-label="People">
        {people.map((p) => {
          const last = isLastAdmin(people, p);
          const busy = pending === p.id;
          const ask = asking?.id === p.id ? asking : null;
          return (
            <li key={p.id} data-person={p.id}
              className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-4 gap-y-2 px-5 py-3.5 md:grid-cols-[minmax(12rem,1fr)_7rem_7rem_10.5rem_20rem]">
              <div className="min-w-0">
                <div className="truncate font-medium text-ink">{p.name}{p.you && <span className="font-normal text-ink-3"> (you)</span>}</div>
                <div className="mt-0.5 truncate font-mono text-caption text-ink-3">{p.email}</div>
                {/* said in the row, not on hover: a keyboard can't reach a control that is off */}
                {last && <p className="mt-1 text-xs text-ink-3" data-last-admin="">{LAST_ADMIN}</p>}
              </div>
              <select aria-label={`Role of ${p.email}`} value={ask?.kind === "role" ? ask.role : p.role} disabled={busy || (last && p.role === "admin")}
                onChange={(e) => { const role = e.target.value as Role; if (ownRoleSentence(p, role)) setAsking({ id: p.id, kind: "role", role }); else { cancel(); onRole(p, role); } }}
                className={`${SELECT} h-8 w-28 ${OFF}`}>
                {ROLES.map(([v, label]) => <option key={v} value={v}>{label}</option>)}
              </select>
              <div><StateMark tone={TONE[p.status]} note={STATUS_HINT[p.status]}>{STATUS_TEXT[p.status]}</StateMark></div>
              <div className="text-xs text-ink-3">
                Last sign-in{" "}
                {p.last_sign_in_ms === null ? <span className="text-ink-2">never</span> : <span className="font-mono text-caption text-ink-2">{fmtWhen(p.last_sign_in_ms)}</span>}
              </div>
              <div className="col-span-2 md:col-span-1">
                {ask?.kind === "remove" ? (
                  <Confirm label={`Remove ${p.email}`} note={removeSsoNote(p)} action="Remove" disabled={busy} onCancel={cancel}
                    onConfirm={() => { cancel(); onRemove(p); }}>{removeSentence(p)}</Confirm>
                ) : ask?.kind === "disable" ? (
                  <Confirm label={`Disable ${p.email}`} action="Disable" disabled={busy} onCancel={cancel}
                    onConfirm={() => { cancel(); onDisabled(p, true); }}>{disableSentence(p)}</Confirm>
                ) : ask?.kind === "role" ? (
                  <Confirm label={`Change the role of ${p.email}`} action="Make me a member" disabled={busy} onCancel={cancel}
                    onConfirm={() => { cancel(); onRole(p, ask.role); }}>{ownRoleSentence(p, ask.role)}</Confirm>
                ) : (
                  <div className="flex flex-wrap justify-end gap-1">
                    {p.status !== "disabled" && <Button size="sm" variant="ghost" className={OFF} disabled={busy} onClick={() => onLink(p)}>{linkButton(p)}</Button>}
                    <Button size="sm" variant="ghost" className={OFF} disabled={busy || (last && p.status !== "disabled")}
                      onClick={() => p.status === "disabled" ? onDisabled(p, false) : setAsking({ id: p.id, kind: "disable" })}>{p.status === "disabled" ? "Enable" : "Disable"}</Button>
                    <Button size="sm" variant="ghost" className={OFF} disabled={busy || last} onClick={() => setAsking({ id: p.id, kind: "remove" })}>Remove</Button>
                  </div>
                )}
              </div>
            </li>
          );
        })}
      </ul>
    </Card>
  );
}

export type AddPersonProps = {
  /** resolves when the person is added (the form clears), rejects with a message to show (the form keeps what was typed) */
  onAdd: (p: { email: string; name: string; role: Role }) => Promise<void>;
  /** the server emails invites (GET /api/users sends_email), so the button says it sends one */
  sendsEmail?: boolean;
};

/** Email, name (optional: the part before @ by default) and role. Adding someone answers with their invite link. */
export function AddPerson({ onAdd, sendsEmail = false }: AddPersonProps) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const form = e.currentTarget;
    const data = new FormData(form);
    const email = String(data.get("email") ?? "").trim();
    if (!email) return;
    setBusy(true);
    setError("");
    onAdd({ email, name: String(data.get("name") ?? "").trim(), role: (data.get("role") === "admin" ? "admin" : "member") })
      .then(() => form.reset(), (err: Error) => setError(err.message))
      .finally(() => setBusy(false));
  };
  return (
    <form onSubmit={submit} className="grid gap-2" aria-label="Add a person">
      <div className="flex flex-wrap items-end gap-2">
        <Field label="Email" name="email" type="email" required maxLength={254} placeholder="ben@example.com" className="w-72" />
        <Field label="Name (optional)" name="name" type="text" maxLength={200} placeholder="Ben" className="w-48" />
        <SelectField label="Role" name="role" defaultValue="member">
          {ROLES.map(([v, label]) => <option key={v} value={v}>{label}</option>)}
        </SelectField>
        <Button variant="primary" size="sm" type="submit" disabled={busy} className="h-9 disabled:opacity-60">
          {busy ? "Adding" : sendsEmail ? "Add and email invite" : "Add and make invite link"}
        </Button>
      </div>
      <FormError>{error}</FormError>
    </form>
  );
}

export type OneTimeLinkProps = {
  person: Pick<Person, "name" | "email">;
  link: PersonLink;
  /** the email that took it to them, if the server sent one */
  email?: PersonEmail | null;
  onDismiss: () => void;
};

/** The link an add or "new link" answered with, to copy now: it is shown only once. Once emailed, only what happened. */
export function OneTimeLink({ person, link, email, onDismiss }: OneTimeLinkProps) {
  const [copied, copy] = useCopy(link.url ?? "");
  if (link.url === undefined) {
    return (
      <OneTimeBox>
        <p className="max-w-[80ch]">{email?.sent ? emailedSentence(person, link, email.to) : notSentSentence(person)}</p>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <Button size="sm" onClick={onDismiss}>Done</Button>
        </div>
      </OneTimeBox>
    );
  }
  return (
    <OneTimeBox>
      {email && !email.sent && <p className="mb-2 max-w-[80ch] text-critical-text">{unsentSentence(email)}</p>}
      <p className="max-w-[80ch]">{linkSentence(person, link)}</p>
      <div className="mt-3 flex flex-wrap items-center gap-2">
        <input readOnly value={link.url} aria-label="One-time link" onFocus={(e) => e.currentTarget.select()} autoFocus={copied === "select"}
          className={`${INPUT} flex-1 basis-96 font-mono text-caption`} />
        <Button size="sm" variant="ink" onClick={copy}>{copied === "copied" ? "Copied" : "Copy link"}</Button>
        <Button size="sm" onClick={onDismiss}>Done</Button>
      </div>
      <p className="sr-only" aria-live="polite">
        {copied === "copied" ? "Link copied to the clipboard." : copied === "select" ? "Couldn't use the clipboard: select the link and press Ctrl+C or Cmd+C to copy." : ""}
      </p>
    </OneTimeBox>
  );
}
