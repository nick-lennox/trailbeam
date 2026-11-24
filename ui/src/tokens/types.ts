/* What the token routes return (contract/openapi.yaml: IngestToken, PersonalToken, the list and create answers). */

/** A token that lets a service send spans, and nothing else. Admins manage them. */
export type IngestToken = {
  id: number;
  name: string;
  /** the token's first 8 characters: never the token itself */
  prefix: string;
  status: "active" | "revoked";
  created_at_ms: number;
  /** the email of the admin who made it, if they still have an account */
  created_by: string | null;
  last_used_ms: number | null;
  revoked_at_ms: number | null;
};

/** A token that acts as the person who made it, with their role, for scripts and the API. */
export type PersonalToken = {
  id: number;
  name: string;
  prefix: string;
  status: "active" | "expired" | "revoked";
  created_at_ms: number;
  expires_at_ms: number | null;
  last_used_ms: number | null;
  revoked_at_ms: number | null;
};

/** `TRAILBEAM_TOKEN`: set in the server's environment, so the page shows it but can't change it. */
export type SharedToken = { set: boolean; label: string; works_for: ("ingest" | "metrics")[] };

export type IngestTokenList = {
  tokens: IngestToken[];
  shared_token: SharedToken;
  /** `open`: no TRAILBEAM_TOKEN and no ingest token yet, so any service can send spans without a token */
  ingest: "token" | "open";
};

/** A create answers with the token itself, once. */
export type NewToken<T> = { token: T; secret: string };
