/*
  The Tokens page's words (users and roles, phase 2; proposal on QV-149, section 7): what each kind of token is, the
  open-ingest warning, how long a personal token works, and the sentences for a new, revoked or shared token. The
  parts that draw them (the table, the form that creates one, the new token shown once) are TokensViews.tsx's, on
  the design system, reached through @trailbeam/ui/design. The legacy shared token (TRAILBEAM_TOKEN) is listed with
  the ingest tokens but can't be revoked: it is set in the server's environment. No view ever has a token's value
  except the one a create answered with.
*/
import type { IngestToken, PersonalToken, SharedToken } from "./types.ts";

export type TokenKind = "ingest" | "personal";
export type AnyToken = IngestToken | PersonalToken;

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

export type NewTokenRequest = { name: string; expires_in_days?: number | null; close_open_ingest?: boolean };
