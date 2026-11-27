/* What the people routes return (contract/openapi.yaml: User, UserLink, UserWithLink). Two roles only, by board decision. */
export type Role = "admin" | "member";

/**
 * `invited`: no password yet (their invite link hasn't been used). `locked`: too many wrong passwords, for a while.
 * `disabled`: can't sign in.
 */
export type PersonStatus = "active" | "invited" | "locked" | "disabled";

export type Person = {
  id: number;
  email: string;
  name: string;
  role: Role;
  status: PersonStatus;
  created_at_ms: number;
  last_sign_in_ms: number | null;
  /** this is the person looking at the page */
  you: boolean;
  /**
   * they sign in with single sign-on (an identity provider account is linked to them). Removing them then doesn't keep
   * them out, so the remove confirmation says so. Absent from servers without single sign-on: treated as false.
   */
  single_sign_on?: boolean;
};

/** A one-time link: an invite (7 days) for someone with no password yet, else a password reset (24 hours). */
export type PersonLink = {
  kind: "invite" | "reset";
  /** `/set-password#code=...` on this server; shown only in the answer that made it, and left out once it was emailed */
  url?: string;
  expires_at_ms: number;
};

/** The email that took the link to the person (a server with a mail server set up), or null when none was sent for. */
export type PersonEmail = { sent: boolean; to: string; error: string | null };

export type PersonWithLink = { user: Person; link: PersonLink; email?: PersonEmail | null };
