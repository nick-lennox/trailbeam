/*
  The Account page at /account (anyone signed in as a person): who you are, and the passkeys that can sign you in
  (/api/me/passkeys, QV-665). Add a passkey asks the server for options, has the browser make one
  (navigator.credentials.create) and sends it back; Remove deletes one. After each, the list is read again, so the
  page shows what the server holds. The copy is the hosted app's AccountCard's. contract/openapi.yaml, tag Account.
*/
import { useCallback, useEffect, useState } from "react";
import { btn, fmtWhen } from "../src/people/People.tsx";
import { Frame, getJSON, sendJSON, useMe } from "./chrome.tsx";
import { browserProblem, creationOptions, deviceName, registrationJSON, type CreationOptionsJSON } from "./passkeys.ts";

type Passkey = { id: number; name: string; created_at_ms: number; last_used_ms: number | null; synced: boolean };
type List = { passkeys: Passkey[]; on: boolean };
type Load = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; data: List };

const box = "mt-4 rounded-[16px] border border-sky-line bg-sky-2/55";

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
    <Frame>
      <h1 className="text-[1.75rem] text-sky-ink">Account</h1>
      {me && !person && (
        <p className="mt-3 max-w-[80ch] text-sky-ink-2">Your account is a person's: sign in with your email and password to see it.</p>
      )}
      {person && (
        <section className="mt-6" aria-labelledby="sign-in">
          <h2 id="sign-in" className="text-[1.25rem] text-sky-ink">Sign-in</h2>
          <p className="mt-1 text-sky-ink-2">
            Signed in as <span className="text-sky-ink">{person.name}</span>, <span className="font-mono text-caption">{person.email}</span>
          </p>
          <div className={`${box} px-5 py-4`}>
            <div className="flex flex-wrap items-center gap-3">
              <h3 className="text-sky-ink">Passkeys</h3>
              {list?.on && supported && (
                <button type="button" onClick={add} disabled={busy}
                  className="ml-auto rounded-full border border-ink bg-ink px-4 py-1 text-sm text-surface hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60">
                  {busy ? "Adding" : "Add a passkey"}
                </button>
              )}
            </div>
            <p className="mt-1 max-w-[80ch] text-sm text-sky-ink-2">Sign in with Face ID, Touch ID, Windows Hello or a security key instead of a password.</p>
            {list && !list.on && (
              <p className="mt-2 max-w-[80ch] text-sm text-sky-ink-3">
                Passkeys are off on this server. An operator turns them on by setting its public URL (<code className="font-mono text-caption">TRAILBEAM_PUBLIC_URL</code>) and installing <code className="font-mono text-caption">trailbeam[server,passkeys]</code>.
              </p>
            )}
            {list?.on && !supported && <p className="mt-2 text-sm text-sky-ink-3">This browser can't make passkeys.</p>}
            {problem && <p className="mt-2 text-sm text-critical-text" role="alert">{problem}</p>}
            {state.kind === "loading" && <p className="mt-3 text-sm text-sky-ink-3">Loading your passkeys</p>}
            {state.kind === "error" && <p className="mt-3 text-sm text-critical-text" role="alert">{state.message}</p>}
            {list && list.passkeys.length > 0 && (
              <ul className="mt-3 divide-y divide-sky-line rounded-[12px] border border-sky-line">
                {list.passkeys.map((k) => (
                  <li key={k.id} className="flex flex-wrap items-center gap-3 px-3 py-2 text-sm">
                    <span className="text-sky-ink">{k.name}</span>
                    {k.synced && <span className="text-sky-ink-3">synced</span>}
                    <span className="text-sky-ink-3">added <span className="font-mono text-caption">{fmtWhen(k.created_at_ms)}</span></span>
                    <span className="text-sky-ink-3">last used {k.last_used_ms === null ? "never" : <span className="font-mono text-caption">{fmtWhen(k.last_used_ms)}</span>}</span>
                    <button type="button" className={`${btn} ml-auto`} disabled={pending === k.id} onClick={() => remove(k.id)}
                      aria-label={`Remove ${k.name}`}>Remove</button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </section>
      )}
    </Frame>
  );
}
