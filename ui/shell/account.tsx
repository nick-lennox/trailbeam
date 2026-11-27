/*
  The Account page at /account (anyone signed in as a person), in the shell's frame (frame.tsx) on the design system:
  who you are, and the passkeys that can sign you in (/api/me/passkeys, QV-665). Add a passkey asks the server for
  options, has the browser make one (navigator.credentials.create) and sends it back; Remove deletes one. After each,
  the list is read again, so the page shows what the server holds. The copy is the hosted app's AccountCard's.
  contract/openapi.yaml, tag Account.
*/
import { useCallback, useEffect, useState } from "react";
import { fmtWhen } from "../src/index.ts";
import { Button, PageHeader, Problem, SettingsSection } from "../src/design/index.ts";
import { getJSON, sendJSON, useMe } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";
import { browserProblem, creationOptions, deviceName, registrationJSON, type CreationOptionsJSON } from "./passkeys.ts";

type Passkey = { id: number; name: string; created_at_ms: number; last_used_ms: number | null; synced: boolean };
type List = { passkeys: Passkey[]; on: boolean };
type Load = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; data: List };

export function AccountPage() {
  const me = useMe();
  const person = me?.user ?? null;
  const [state, setState] = useState<Load>({ kind: "loading" });
  const [busy, setBusy] = useState(false);
  const [pending, setPending] = useState<number | null>(null);
  const [problem, setProblem] = useState("");
  useEffect(() => { document.title = "Account · Trailbeam"; }, []);

  const load = useCallback(() => getJSON<List>("/api/me/passkeys")
    .then((data) => setState({ kind: "ok", data }))
    .catch((e: Error) => setState({ kind: "error", message: e.message })), []);
  useEffect(() => { if (person) void load(); }, [person, load]);

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

  const remove = (id: number) => {
    setPending(id);
    setProblem("");
    sendJSON("DELETE", `/api/me/passkeys/${id}`)
      .catch((e: Error) => setProblem(e.message))
      .then(load)
      .finally(() => setPending(null));
  };

  const supported = typeof window !== "undefined" && "PublicKeyCredential" in window;
  const list = state.kind === "ok" ? state.data : null;

  return (
    <ShellFrame path="/account">
      <PageHeader title="Account" subtitle="Who you are signed in as, and how you sign in." search={<ShellSearch />} />
      <div className="grid max-w-5xl gap-12 px-8 pb-16">
        {me && !person && (
          <p className="max-w-2xl text-sm leading-relaxed text-ink-2">Your account is a person's: sign in with your email and password to see it.</p>
        )}
        {person && (
          <SettingsSection id="sign-in" title="Sign-in"
            about={<>Signed in as <span className="text-ink">{person.name}</span>, <span className="font-mono text-caption">{person.email}</span></>}>
            <div className="grid gap-4">
              <div className="flex flex-wrap items-center gap-3">
                <div className="min-w-0">
                  <h3 className="text-sm font-medium text-ink">Passkeys</h3>
                  <p className="mt-1 text-sm text-ink-2">Sign in with Face ID, Touch ID, Windows Hello or a security key instead of a password.</p>
                </div>
                {list?.on && supported && (
                  <Button size="sm" variant="ink" className="ml-auto" onClick={add} disabled={busy}>{busy ? "Adding" : "Add a passkey"}</Button>
                )}
              </div>
              {list && !list.on && (
                <p className="max-w-2xl text-sm leading-relaxed text-ink-3">
                  Passkeys are off on this server. An operator turns them on by setting its public URL (<code className="font-mono text-caption">TRAILBEAM_PUBLIC_URL</code>) and installing <code className="font-mono text-caption">trailbeam[server,passkeys]</code>.
                </p>
              )}
              {list?.on && !supported && <p className="text-sm text-ink-3">This browser can't make passkeys.</p>}
              <Problem>{problem}</Problem>
              {state.kind === "loading" && <p className="text-sm text-ink-3">Loading your passkeys</p>}
              {state.kind === "error" && <Problem>{state.message}</Problem>}
              {list && list.passkeys.length > 0 && (
                <ul className="divide-y divide-line rounded-lg border border-line">
                  {list.passkeys.map((k) => (
                    <li key={k.id} className="flex flex-wrap items-center gap-3 px-3 py-2 text-sm">
                      <span className="text-ink">{k.name}</span>
                      {k.synced && <span className="text-ink-3">synced</span>}
                      <span className="text-ink-3">added <span className="font-mono text-caption">{fmtWhen(k.created_at_ms)}</span></span>
                      <span className="text-ink-3">last used {k.last_used_ms === null ? "never" : <span className="font-mono text-caption">{fmtWhen(k.last_used_ms)}</span>}</span>
                      <Button size="sm" variant="ghost" className="ml-auto" disabled={pending === k.id} onClick={() => remove(k.id)}
                        aria-label={`Remove ${k.name}`}>Remove</Button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </SettingsSection>
        )}
      </div>
    </ShellFrame>
  );
}
