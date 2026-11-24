/*
  The Tokens page at /tokens (anyone signed in as a person). Admins see the ingest tokens (/api/tokens/ingest, with the
  legacy shared token from TRAILBEAM_TOKEN listed but not changeable) and everyone sees their own personal API tokens
  (/api/me/tokens): create (POST, which answers with the token, shown once) and revoke (DELETE .../{id}). After each,
  the list is read again, so the page shows what the server holds. contract/openapi.yaml, tag Tokens.
*/
import { useCallback, useEffect, useState } from "react";
import {
  CreateToken, INGEST_TEXT, OPEN_INGEST, OneTimeToken, PERSONAL_TEXT, TokensTable,
  type IngestTokenList, type NewToken, type NewTokenRequest, type PersonalToken, type TokenKind,
} from "../src/index.ts";
import { Frame, getJSON, sendJSON, useMe } from "./chrome.tsx";

type Load<T> = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; data: T };
type Shown = { kind: TokenKind; name: string; secret: string };

const PATHS: Record<TokenKind, string> = { ingest: "/api/tokens/ingest", personal: "/api/me/tokens" };

/** One section's list, its create and its revoke. */
function useTokens<T>(kind: TokenKind, on: boolean) {
  const [state, setState] = useState<Load<T>>({ kind: "loading" });
  const [pending, setPending] = useState<number | null>(null);
  const [problem, setProblem] = useState("");
  const load = useCallback(() => getJSON<T>(PATHS[kind])
    .then((data) => setState({ kind: "ok", data }))
    .catch((e: Error) => setState({ kind: "error", message: e.message })), [kind]);
  useEffect(() => { if (on) void load(); }, [on, load]);
  const revoke = (id: number) => {
    setPending(id);
    setProblem("");
    sendJSON("DELETE", `${PATHS[kind]}/${id}`)
      .catch((e: Error) => setProblem(e.message))
      .then(load)
      .finally(() => setPending(null));
  };
  /** Rejects with the server's sentence, which the form shows. Reads the list again either way: a refused first
      ingest token (409 closes_open_ingest) means ingest was open, so the form then asks to confirm closing it. */
  const create = (r: NewTokenRequest) => {
    setProblem("");
    return sendJSON<NewToken<{ name: string }>>("POST", PATHS[kind], r).finally(() => void load());
  };
  return { state, pending, problem, revoke, create };
}

const box = "mt-4 rounded-[16px] border border-sky-line bg-sky-2/55";
const note = (text: string) => <p className="mt-4 rounded-[12px] border border-sky-line bg-sky-2 px-4 py-3 text-[15px] text-sky-ink-2" role="alert">{text}</p>;

export function TokensPage() {
  const me = useMe();
  const person = me?.user ?? null;
  const admin = person?.role === "admin";
  const ingest = useTokens<IngestTokenList>("ingest", admin);
  const mine = useTokens<{ tokens: PersonalToken[] }>("personal", person !== null);
  const [shown, setShown] = useState<Shown | null>(null);
  useEffect(() => { document.title = "Tokens · Trailbeam"; }, []);

  type Create = (r: NewTokenRequest) => Promise<NewToken<{ name: string }>>;
  const onCreate = (kind: TokenKind, create: Create) => async (r: NewTokenRequest) => {
    const made = await create(r);
    setShown({ kind, name: made.token.name, secret: made.secret });
  };

  return (
    <Frame>
      <h1 className="text-[1.75rem] text-sky-ink">Tokens</h1>
      {me && !person && (
        <p className="mt-3 max-w-[80ch] text-sky-ink-2">
          Tokens belong to a person: sign in with your email and password to manage yours.
          {me.auth !== "accounts" && " This server has no accounts yet; the first admin is made with trailbeam users create --role admin."}
        </p>
      )}
      {shown && <div className="mt-4"><OneTimeToken {...shown} onDismiss={() => setShown(null)} /></div>}

      {admin && (
        <section className="mt-6" aria-labelledby="ingest-tokens">
          <h2 id="ingest-tokens" className="text-[1.25rem] text-sky-ink">Ingest tokens</h2>
          <p className="mt-1 max-w-[80ch] text-sky-ink-2">{INGEST_TEXT} Only admins see these.</p>
          {ingest.state.kind === "ok" && ingest.state.data.ingest === "open" && note(OPEN_INGEST)}
          <div className={`${box} px-5 py-4`}>
            <CreateToken kind="ingest" ingestOpen={ingest.state.kind === "ok" && ingest.state.data.ingest === "open"}
              onCreate={onCreate("ingest", ingest.create)} />
          </div>
          {ingest.problem && note(ingest.problem)}
          {ingest.state.kind === "loading" && <p className="mt-4 text-sky-ink-3">Loading ingest tokens</p>}
          {ingest.state.kind === "error" && note(ingest.state.message)}
          {ingest.state.kind === "ok" && (
            <div className={`${box} overflow-x-auto`}>
              <TokensTable kind="ingest" tokens={ingest.state.data.tokens} shared={ingest.state.data.shared_token}
                pending={ingest.pending} onRevoke={(t) => ingest.revoke(t.id)} />
            </div>
          )}
          <p className="mt-3 max-w-[80ch] text-sm text-sky-ink-3">
            The same from a shell: <code className="font-mono text-caption">trailbeam tokens</code>.
          </p>
        </section>
      )}

      {person && (
        <section className="mt-8" aria-labelledby="personal-tokens">
          <h2 id="personal-tokens" className="text-[1.25rem] text-sky-ink">Your personal API tokens</h2>
          <p className="mt-1 max-w-[80ch] text-sky-ink-2">{PERSONAL_TEXT} Only you see yours.</p>
          <div className={`${box} px-5 py-4`}>
            <CreateToken kind="personal" onCreate={onCreate("personal", mine.create)} />
          </div>
          {mine.problem && note(mine.problem)}
          {mine.state.kind === "loading" && <p className="mt-4 text-sky-ink-3">Loading your tokens</p>}
          {mine.state.kind === "error" && note(mine.state.message)}
          {mine.state.kind === "ok" && (
            <div className={`${box} overflow-x-auto`}>
              <TokensTable kind="personal" tokens={mine.state.data.tokens} pending={mine.pending} onRevoke={(t) => mine.revoke(t.id)} />
            </div>
          )}
          <p className="mt-3 max-w-[80ch] text-sm text-sky-ink-3">
            The same from a shell: <code className="font-mono text-caption">trailbeam tokens --personal</code>.
          </p>
        </section>
      )}
    </Frame>
  );
}
