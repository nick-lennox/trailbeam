/* The account screen's data (QV-587): what the host knows about how the signed-in person signs in. */

/** An account at an identity provider that signs the person in (self-hosted: /api/me sign_in.identities). */
export type SignInIdentity = {
  provider: "sso" | "github";
  /** the sign-in page's name for it ("Okta", "GitHub"); null when the server doesn't offer it now */
  label: string | null;
  on: boolean;
  email: string;
  last_sign_in_ms: number | null;
};

/** What accountWays reads: /api/me's sign_in, how the request got in, and whether the person is an admin. */
export type SignInFacts = { via: string; admin: boolean; password: boolean; identities: SignInIdentity[] };

/** One row of the card's "Ways to sign in": a name, a value in mono (an email), a sentence, and whether it is off. */
export type AccountWay = { key: string; title: string; value?: string; detail: string; off?: boolean };

/** A passkey, as both servers list them. */
export type Passkey = { id: string; name?: string | null; createdAt: string | number | Date };
