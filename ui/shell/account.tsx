/*
  The Account page at /account (anyone signed in: the server answers others with its sign-in or "Not allowed"
  page), in the shell's frame (frame.tsx) on the design system. One card, the shared AccountCard: who you are
  signed in as, from /api/me, and the ways you can sign in, from its sign_in (a password, and accounts at single
  sign-on or GitHub). Sign out is the sidebar's. Passkeys join the card with QV-665 (/api/me/passkeys, the card's
  `passkeys`).
*/
import { useEffect } from "react";
import { ACCOUNT_ABOUT, ROLES, accountWays } from "../src/index.ts";
import { AccountCard, PageHeader } from "../src/design/index.ts";
import { useMe } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";

export function AccountPage() {
  const me = useMe();
  useEffect(() => { document.title = "Account · Trailbeam"; }, []);
  const user = me?.user ?? null;
  const ways = me?.sign_in && user
    ? accountWays({ via: me.via, admin: user.role === "admin", password: me.sign_in.password, identities: me.sign_in.identities })
    : [];
  return (
    <ShellFrame path="/account">
      <PageHeader title="Your account" subtitle={ACCOUNT_ABOUT} search={<ShellSearch />} />
      <div className="grid max-w-3xl gap-12 px-8 pb-16">
        <AccountCard
          user={user && { name: user.name, email: user.email, role: ROLES.find((r) => r[0] === user.role)?.[1] }}
          ways={ways} />
      </div>
    </ShellFrame>
  );
}
