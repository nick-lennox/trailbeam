/*
  The account screen's words and the pure parts behind them (QV-587), shared by self-hosted Trailbeam and the hosted
  app: what the card says about a password, an account at an identity provider and passkeys, and the name a new
  passkey gets. The card that draws them is AccountCard.tsx, in @trailbeam/ui/design. No React here, so a server
  build can import it.
*/
import type { AccountWay, SignInFacts } from "./types.ts";

export const ACCOUNT_ABOUT = "Who you are signed in as, and the ways you can sign in.";
export const PASSKEYS_ABOUT = "Sign in with Face ID, Touch ID, Windows Hello or a security key instead of a password.";
export const PASSKEY_NOT_SAVED = "The passkey wasn't saved. Your browser may have canceled it.";
export const WAY_OFF = "This Trailbeam doesn't offer this way in now, so it can't sign you in.";

/** Self-hosted, signed in without a person (QV-767): the shared TRAILBEAM_TOKEN, or a server that needs no sign-in.
    `signedIn` is said on the card in place of the person; `accounts`, under it, how a person gets an account here:
    with users and roles off (`/api/me` features.accounts false, the default) that takes TRAILBEAM_ACCOUNTS=1 first,
    since `trailbeam users create` refuses without --accounts and the server ignores an admin made with it. */
export function withoutPerson(f: { via: string; accounts: boolean }): { signedIn: string; accounts: string } {
  const signedIn = f.via === "shared_token" || f.via === "shared_cookie"
    ? "Signed in with this server's shared access token (TRAILBEAM_TOKEN), not as a person."
    : f.via === "none" ? "This server needs no sign-in, so this browser isn't signed in as anyone." : "Not signed in as a person.";
  const create = "trailbeam users create --accounts --email <email> --role admin";
  const accounts = f.accounts
    ? `Personal accounts are on, but this server has no admin yet. An operator creates the first one with ${create}.`
    : `Personal accounts are a preview, off on this server. To turn them on, an operator starts the server with TRAILBEAM_ACCOUNTS=1 and, if it has no admin yet, creates one with ${create}.`;
  return { signedIn, accounts };
}

const when = (ms: number) => new Date(ms).toISOString().slice(0, 16).replace("T", " ") + " UTC";

/** A passkey's default name: the browser and the system it was made on ("Chrome on Mac"). */
export function deviceName(ua: string): string {
  const os = /iPhone|iPad/.test(ua) ? "iPhone" : /Mac/.test(ua) ? "Mac" : /Android/.test(ua) ? "Android" : /Windows/.test(ua) ? "Windows" : "This device";
  const browser = /Edg\//.test(ua) ? "Edge" : /Chrome\//.test(ua) ? "Chrome" : /Firefox\//.test(ua) ? "Firefox" : /Safari\//.test(ua) ? "Safari" : "";
  return browser ? `${browser} on ${os}` : os;
}

/**
 * The card's list of ways in, from what the server knows (self-hosted: /api/me's sign_in and via): the password when
 * there is one, then each account at an identity provider, oldest first, and last, without a password, how to get one. Signed in by a proxy, that is said instead: the
 * proxy, not Trailbeam, decides who gets in.
 */
export function accountWays(f: SignInFacts): AccountWay[] {
  if (f.via === "proxy") {
    return [{ key: "proxy", title: "Your sign-in service", detail: "The proxy in front of this Trailbeam signs you in. Your password and any other way in are kept there, not here." }];
  }
  const ways: AccountWay[] = [];
  const change = f.admin ? "To change it, make yourself a reset link on the People page." : "To change it, ask an admin for a reset link.";
  if (f.password) ways.push({ key: "password", title: "Password", detail: `You have a password. ${change}` });
  for (const i of f.identities) {
    const name = i.label ?? (i.provider === "github" ? "GitHub" : "Single sign-on");
    ways.push({
      key: `${i.provider}:${i.email}`,
      title: name,
      value: i.email,
      detail: i.last_sign_in_ms ? `Last used ${when(i.last_sign_in_ms)}.` : "Not used yet.",
      off: !i.on,
    });
  }
  if (!f.password && f.identities.length === 0) {
    ways.push({ key: "password", title: "Password", detail: "You don't have a password yet. Open the invite or reset link an admin sent you to choose one." });
  } else if (!f.password) {
    ways.push({ key: "password", title: "Password", detail: `You don't have a password, so you sign in another way. ${f.admin ? "A reset link from the People page sets one." : "An admin can send you a reset link to set one."}` });
  }
  return ways;
}
