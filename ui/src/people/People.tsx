/*
  The People page's parts (users and roles, phase 2; proposal on QV-149, section 7): the table of people with what an
  admin can do to each, the form that adds someone, and the one-time link an add or a "new link" answers with.
  They take the API's data as props (types.ts) and call back for every change: the host sends the request
  (self-hosted Trailbeam: PATCH, DELETE and POST on /api/users) and passes the new list back in.
  Two roles only, admin and member. The last admin who can sign in can't be demoted, disabled or removed: the
  controls for that person are off here, and the server refuses it anyway (409, reason last_admin).
  Colored with role tokens (theme.css), as the Tenants table is.
*/
import { useId, useState, type FormEvent } from "react";
import { useCopy } from "../copy.ts";
import type { Person, PersonEmail, PersonLink, PersonStatus, Role } from "./types.ts";

export const ROLES: readonly [Role, string][] = [["admin", "Admin"], ["member", "Member"]];

/** What each role may do, for the page's explanation. */
export const ROLE_TEXT = "Members can see everything Trailbeam shows, message bodies included, and make their own personal API tokens. "
  + "Admins can also add and remove people and manage ingest tokens.";

export const STATUS_TEXT: Record<PersonStatus, string> = {
  active: "Active",
  invited: "Invited",
  locked: "Locked for now",
  disabled: "Disabled",
};

/** The status column's hover text. */
export const STATUS_HINT: Record<PersonStatus, string> = {
  active: "Can sign in.",
  invited: "Hasn't used their invite link yet, so has no password.",
  locked: "Too many wrong passwords: can't sign in for a while. A reset link lets them choose a new password now.",
  disabled: "Can't sign in. Enable them to let them back in.",
};

export const LAST_ADMIN = "The only admin who can sign in. Make someone else an admin first.";

/** The admins who can sign in: the server keeps at least one (an invited admin counts, as on the server). */
export const signInAdmins = (people: Person[]) => people.filter((p) => p.role === "admin" && p.status !== "disabled");

/** True when demoting, disabling or removing `p` would leave no admin who can sign in. */
export function isLastAdmin(people: Person[], p: Person): boolean {
  const admins = signInAdmins(people);
  return admins.length === 1 && admins[0].id === p.id;
}

/** What "new link" makes for this person: an invite until they have a password, else a password reset. */
export const linkKind = (p: Person): PersonLink["kind"] => (p.status === "invited" ? "invite" : "reset");
export const linkButton = (p: Person) => (linkKind(p) === "invite" ? "New invite link" : "Password reset link");

/** A time as the page shows it: "2026-11-24 06:02 UTC". */
export const fmtWhen = (ms: number) => new Date(ms).toISOString().slice(0, 16).replace("T", " ") + " UTC";

/** The sentence above a one-time link: whose it is, what it does, how long it lasts, and that it is shown once. */
export function linkSentence(p: Pick<Person, "name" | "email">, link: PersonLink): string {
  const who = p.name && p.name !== p.email ? `${p.name} (${p.email})` : p.email;
  const what = link.kind === "invite"
    ? `${who}'s invite link: it lets them choose a password and sign in.`
    : `${who}'s password reset link: it lets them choose a new password, and signs them out everywhere else.`;
  return `${what} It works once, until ${fmtWhen(link.expires_at_ms)}. `
    + "Send it to them privately: until it is used, anyone with it can sign in as them. It isn't shown again; make a new link if it is lost.";
}

/** The confirmation before a remove. */
export const removeSentence = (p: Pick<Person, "name" | "email" | "you" | "single_sign_on">) =>
  p.you ? `Remove your own account? You are signed out${p.single_sign_on ? "." : ", and can't sign in again."}`
    : `Remove ${p.name || p.email}? Their sign-ins, personal API tokens and links stop working. This can't be undone.`;

/**
 * The warning under the remove confirmation for someone who signs in with single sign-on, else null: removing them
 * deletes the link to their identity provider account, so their next sign-in makes a new person with the default
 * role (as `trailbeam users remove` says).
 */
export function removeSsoNote(p: Pick<Person, "name" | "email" | "you" | "single_sign_on">): string | null {
  if (!p.single_sign_on) return null;
  if (p.you) return "You sign in with single sign-on, so removing your account doesn't keep you out: at your next sign-in "
    + "you come back as a new person with the default role.";
  return `${p.name || p.email} signs in with single sign-on, so removing them doesn't keep them out: at their next sign-in `
    + "they come back as a new person with the default role. To keep them out, disable them instead.";
}

export const btn = "rounded-full border border-line px-3 py-0.5 text-sm text-ink-2 hover:border-ink hover:text-ink disabled:cursor-not-allowed disabled:opacity-50";

export type PeopleTableProps = {
  people: Person[];
  /** the person whose change is being sent: their controls are off until the host passes the new list */
  pending?: number | null;
  onRole: (p: Person, role: Role) => void;
  onDisabled: (p: Person, disabled: boolean) => void;
  onRemove: (p: Person) => void;
  onLink: (p: Person) => void;
};

/** One row per person, in the API's order (oldest first). */
export function PeopleTable({ people, pending = null, onRole, onDisabled, onRemove, onLink }: PeopleTableProps) {
  const [removing, setRemoving] = useState<number | null>(null);
  return (
    <table className="w-full text-[15px]">
      <thead>
        <tr className="border-b border-line text-left font-mono text-caption text-ink-3">
          <th className="px-5 py-2 font-medium">Person</th>
          <th className="px-3 py-2 font-medium">Role</th>
          <th className="px-3 py-2 font-medium">Status</th>
          <th className="px-3 py-2 font-medium">Last sign-in</th>
          <th className="px-5 py-2 text-right font-medium"><span className="sr-only">Actions</span></th>
        </tr>
      </thead>
      <tbody>
        {people.map((p) => {
          const last = isLastAdmin(people, p);
          const busy = pending === p.id;
          return (
            <tr key={p.id} className="border-b border-line/60 align-top last:border-0" data-person={p.id}>
              <td className="px-5 py-3">
                <div className="text-ink">{p.name}{p.you && <span className="ml-2 text-caption text-ink-3">you</span>}</div>
                <div className="font-mono text-caption text-ink-3">{p.email}</div>
              </td>
              <td className="px-3 py-3">
                <label className="sr-only" htmlFor={`role-${p.id}`}>Role of {p.email}</label>
                <select id={`role-${p.id}`} value={p.role} disabled={busy || (last && p.role === "admin")} title={last ? LAST_ADMIN : undefined}
                  onChange={(e) => onRole(p, e.target.value as Role)}
                  className="rounded-full border border-line bg-surface px-3 py-1 text-sm text-ink disabled:cursor-not-allowed disabled:opacity-60">
                  {ROLES.map(([v, label]) => <option key={v} value={v}>{label}</option>)}
                </select>
              </td>
              <td className={`px-3 py-3 text-sm ${p.status === "active" ? "text-ink-2" : p.status === "disabled" ? "text-critical-text" : "text-warning-text"}`}
                title={STATUS_HINT[p.status]}>{STATUS_TEXT[p.status]}</td>
              <td className="px-3 py-3 font-mono text-caption text-ink-3">{p.last_sign_in_ms === null ? "never" : fmtWhen(p.last_sign_in_ms)}</td>
              <td className="px-5 py-3">
                {removing === p.id ? (
                  <div className="flex flex-wrap items-center justify-end gap-2" role="group" aria-label={`Remove ${p.email}`}>
                    <span className="max-w-[34ch] text-sm text-ink-2">{removeSentence(p)}</span>
                    {removeSsoNote(p) && <span className="max-w-[34ch] text-sm text-warning-text" role="note">{removeSsoNote(p)}</span>}
                    <button type="button" className={`${btn} text-critical-text`} disabled={busy} onClick={() => { setRemoving(null); onRemove(p); }}>Remove</button>
                    <button type="button" className={btn} onClick={() => setRemoving(null)}>Cancel</button>
                  </div>
                ) : (
                  <div className="flex flex-wrap justify-end gap-2">
                    {p.status !== "disabled" && <button type="button" className={btn} disabled={busy} onClick={() => onLink(p)}>{linkButton(p)}</button>}
                    <button type="button" className={btn} disabled={busy || (last && p.status !== "disabled")} title={last ? LAST_ADMIN : undefined}
                      onClick={() => onDisabled(p, p.status !== "disabled")}>{p.status === "disabled" ? "Enable" : "Disable"}</button>
                    <button type="button" className={btn} disabled={busy || last} title={last ? LAST_ADMIN : undefined}
                      onClick={() => setRemoving(p.id)}>Remove</button>
                  </div>
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
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
  const id = useId();
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
  const input = "rounded-full border border-line bg-surface px-3 py-1 text-sm text-ink placeholder:text-ink-3";
  return (
    <form onSubmit={submit} className="flex flex-wrap items-end gap-3 text-sm text-ink-2" aria-label="Add a person">
      <label className="flex flex-col gap-1" htmlFor={`${id}-email`}>
        <span>Email</span>
        <input id={`${id}-email`} name="email" type="email" required maxLength={254} autoComplete="off" placeholder="ben@example.com" className={`${input} w-64`} />
      </label>
      <label className="flex flex-col gap-1" htmlFor={`${id}-name`}>
        <span>Name (optional)</span>
        <input id={`${id}-name`} name="name" type="text" maxLength={200} autoComplete="off" placeholder="Ben" className={`${input} w-44`} />
      </label>
      <label className="flex flex-col gap-1" htmlFor={`${id}-role`}>
        <span>Role</span>
        <select id={`${id}-role`} name="role" defaultValue="member" className={input}>
          {ROLES.map(([v, label]) => <option key={v} value={v}>{label}</option>)}
        </select>
      </label>
      <button type="submit" disabled={busy}
        className="rounded-full border border-ink bg-ink px-4 py-1 text-sm text-surface hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60">
        {busy ? "Adding" : sendsEmail ? "Add and email invite" : "Add and make invite link"}
      </button>
      {error && <p className="w-full text-critical-text" role="alert">{error}</p>}
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

/** The sentence for a link the server emailed: to whom, what it does and how long it lasts. */
export function emailedSentence(p: Pick<Person, "name" | "email">, link: PersonLink, to: string): string {
  const whose = p.name && p.name !== p.email ? `${p.name}'s` : "Their";
  const what = `${whose} ${link.kind === "invite" ? "invite" : "password reset"} link`;
  return `${what} was emailed to ${to}. It works once, until ${fmtWhen(link.expires_at_ms)}. Make a new link if it doesn't arrive.`;
}

/** Why the email with the link wasn't sent, and what to do instead. */
export function unsentSentence(email: PersonEmail): string {
  const why = (email.error ?? "").trim();
  return `The email to ${email.to} wasn't sent${why ? `: ${why.replace(/\.$/, "")}` : ""}. Copy the link and send it to them yourself.`;
}

/** The link an add or "new link" answered with, to copy now: it is shown only once. Once emailed, only what happened. */
export function OneTimeLink({ person, link, email, onDismiss }: OneTimeLinkProps) {
  const [copied, copy] = useCopy(link.url ?? "");
  if (link.url === undefined) {
    return (
      <div className="rounded-[16px] border border-brand/60 bg-surface px-5 py-4 text-[15px] leading-[1.5] text-ink-2" role="status">
        <p className="max-w-[80ch]">{emailedSentence(person, link, email?.to ?? person.email)}</p>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <button type="button" className={btn} onClick={onDismiss}>Done</button>
        </div>
      </div>
    );
  }
  return (
    <div className="rounded-[16px] border border-brand/60 bg-surface px-5 py-4 text-[15px] leading-[1.5] text-ink-2" role="status">
      {email && !email.sent && <p className="mb-2 max-w-[80ch] text-critical-text">{unsentSentence(email)}</p>}
      <p className="max-w-[80ch]">{linkSentence(person, link)}</p>
      <div className="mt-3 flex flex-wrap items-center gap-2">
        <input readOnly value={link.url} aria-label="One-time link" onFocus={(e) => e.currentTarget.select()} autoFocus={copied === "select"}
          className="min-w-0 flex-1 basis-96 rounded-full border border-line bg-surface px-3 py-1 font-mono text-caption text-ink" />
        <button type="button" className={btn} onClick={copy}>{copied === "copied" ? "Copied" : "Copy link"}</button>
        <button type="button" className={btn} onClick={onDismiss}>Done</button>
      </div>
      <p className="sr-only" aria-live="polite">
        {copied === "copied" ? "Link copied to the clipboard." : copied === "select" ? "Couldn't use the clipboard: select the link and press Ctrl+C or Cmd+C to copy." : ""}
      </p>
    </div>
  );
}
