/*
  The Account page at /account (anyone signed in: the server answers others with its sign-in or "Not allowed"
  page), in the shell's frame (frame.tsx) on the design system. One card, the shared AccountCard: who you are
  signed in as, from /api/me, the ways you can sign in, from its sign_in (a password, and accounts at single
  sign-on or GitHub), and the passkeys that can sign you in (/api/me/passkeys, QV-665; contract/openapi.yaml, tag
  Account). Add a passkey asks the server for options, has the browser make one (navigator.credentials.create) and
  sends it back; Remove deletes one. After each, the list is read again, so the page shows what the server holds.
  With passkeys off on the server, or a browser that can't make them, the card has no passkey part and a line under
  it says why. Sign out is the sidebar's.
*/
import { useCallback, useEffect, useState } from "react";
import { ACCOUNT_ABOUT, ROLES, accountWays } from "../src/index.ts";
import { AccountCard, PageHeader } from "../src/design/index.ts";
import { getJSON, sendJSON, useMe } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";
import { browserProblem, creationOptions, deviceName, registrationJSON, type CreationOptionsJSON } from "./passkeys.ts";

type Passkey = { id: number; name: string; created_at_ms: number; last_used_ms: number | null; synced: boolean };
type List = { passkeys: Passkey[]; on: boolean };
type Load = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; data: List };

export function AccountPage() {
  const me = useMe();
  const user = me?.user ?? null;
  const [state, setState] = useState<Load>({ kind: "loading" });
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState("");
  useEffect(() => { document.title = "Account · Trailbeam"; }, []);

  const load = useCallback(() => getJSON<List>("/api/me/passkeys")
    .then((data) => setState({ kind: "ok", data }))
    .catch((e: Error) => setState({ kind: "error", message: e.message })), []);
  useEffect(() => { if (user) void load(); }, [user, load]);

  const add = async () => {
    setBusy(true);
    setProblem("");
    try {
      const { options } = await sendJSON<{ options: CreationOptionsJSON }>("POST", "/api/me/passkeys/options", {});
      let made: Credential | null;
      try {
        made = await navigator.credentials.create({ publicKey: creationOptions(options) });
      } catch (e) {
        setProblem(browserProblem(e));
        return;
      }
      if (!made) { setProblem(browserProblem(null)); return; }
      await sendJSON("POST", "/api/me/passkeys", { name: deviceName(navigator.userAgent), credential: registrationJSON(made as never) });
    } catch (e) {
      setProblem((e as Error).message);
    } finally {
      setBusy(false);
      void load();
    }
  };

  const remove = (id: string) => {
    setBusy(true);
    setProblem("");
    sendJSON("DELETE", `/api/me/passkeys/${id}`)
      .catch((e: Error) => setProblem(e.message))
      .then(load)
      .finally(() => setBusy(false));
  };

  const ways = me?.sign_in && user
    ? accountWays({ via: me.via, admin: user.role === "admin", password: me.sign_in.password, identities: me.sign_in.identities })
    : [];
  const supported = typeof window !== "undefined" && "PublicKeyCredential" in window;
  const list = state.kind === "ok" ? state.data : null;
  const passkeysShown = state.kind !== "ok" || (list!.on && supported);

  return (
    <ShellFrame path="/account">
      <PageHeader title="Your account" subtitle={ACCOUNT_ABOUT} search={<ShellSearch />} />
      <div className="grid max-w-3xl gap-4 px-8 pb-16">
        <AccountCard
          user={user && { name: user.name, email: user.email, role: ROLES.find((r) => r[0] === user.role)?.[1] }}
          ways={ways}
          passkeys={user && passkeysShown ? {
            keys: list ? list.passkeys.map((k) => ({ id: String(k.id), name: k.name, createdAt: k.created_at_ms })) : null,
            onAdd: add,
            onRemove: remove,
            busy: busy || !list,
            error: problem || (state.kind === "error" ? state.message : ""),
          } : undefined} />
        {user && list && !list.on && (
          <p className="max-w-2xl text-sm leading-relaxed text-ink-3">
            Passkeys are off on this server. An operator turns them on by setting its public URL (<code className="font-mono text-caption">TRAILBEAM_PUBLIC_URL</code>) and installing <code className="font-mono text-caption">trailbeam[server,passkeys]</code>.
          </p>
        )}
        {user && list?.on && !supported && <p className="text-sm text-ink-3">This browser can't make passkeys.</p>}
      </div>
    </ShellFrame>
  );
}
