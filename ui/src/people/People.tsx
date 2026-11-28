/*
  The People page's words and rules (users and roles, phase 2; proposal on QV-149, section 7): the roles, each
  status and its note, the one-time link's sentences, and who counts as the last admin. The parts that draw them
  (the table of people, the form that adds someone, the one-time link) are PeopleViews.tsx's, on the design
  system, reached through @trailbeam/ui/design. Two roles only, admin and member. The last admin who can sign in
  can't be demoted, disabled or removed: the controls for that person are off, and the server refuses it anyway
  (409, reason last_admin).
*/
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

/** The sentence for a link the server emailed: to whom, what it does and how long it lasts. */
export function emailedSentence(p: Pick<Person, "name" | "email">, link: PersonLink, to: string): string {
  const whose = p.name && p.name !== p.email ? `${p.name}'s` : "Their";
  const what = `${whose} ${link.kind === "invite" ? "invite" : "password reset"} link`;
  return `${what} was emailed to ${to}. It works once, until ${fmtWhen(link.expires_at_ms)}. Make a new link if it doesn't arrive.`;
}

/** The sentence when the server neither showed nor emailed the link: the request wasn't a browser session (a sign-in
 * through the proxy), so it only added the person (QV-712, QV-755). */
export function notSentSentence(p: Pick<Person, "name" | "email">): string {
  return `${p.name || p.email} was added, but no link was shown or emailed: only someone signed in to Trailbeam in a browser, `
    + "not through the proxy, can make one. People who sign in through the proxy don't need one.";
}

/** Why the email with the link wasn't sent, and what to do instead. */
export function unsentSentence(email: PersonEmail): string {
  const why = (email.error ?? "").trim();
  return `The email to ${email.to} wasn't sent${why ? `: ${why.replace(/\.$/, "")}` : ""}. Copy the link and send it to them yourself.`;
}
