/*
  The Tokens page at /tokens (anyone signed in as a person), in the shell's frame (frame.tsx) on the design system.
  Admins see the ingest tokens (/api/tokens/ingest, with the legacy shared token from TRAILBEAM_TOKEN listed but not
  changeable) and everyone sees their own personal API tokens (/api/me/tokens): create (POST, which answers with the
  token, shown once) and revoke (DELETE .../{id}). After each, the list is read again, so the page shows what the
  server holds. contract/openapi.yaml, tag Tokens.
*/
import { useCallback, useEffect, useState } from "react";
import {
  INGEST_TEXT, OPEN_INGEST, PERSONAL_TEXT,
  type IngestTokenList, type NewToken, type NewTokenRequest, type PersonalToken, type TokenKind,
} from "../src/index.ts";
import { Caution, CreateToken, OneTimeToken, PageHeader, Problem, SettingsSection, TokensTable } from "../src/design/index.ts";
import { getJSON, sendJSON, useMe } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";

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

const Shell = ({ command }: { command: string }) => (
  <p className="text-xs text-ink-3">The same from a shell: <code className="font-mono text-caption text-ink-2">{command}</code>.</p>
);

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
  const open = ingest.state.kind === "ok" && ingest.state.data.ingest === "open";

  return (
    <ShellFrame path="/tokens">
      <PageHeader title="Tokens" subtitle="What sends spans to this Trailbeam, and what reads it as you." search={<ShellSearch />} />
      <div className="grid max-w-5xl gap-12 px-8 pb-16">
        {me && !person && (
          <p className="max-w-2xl text-sm leading-relaxed text-ink-2">
            Tokens belong to a person: sign in with your email and password to manage yours.
            {me.auth !== "accounts" && <> This server has no accounts yet; the first admin is made with <code className="font-mono text-caption">trailbeam users create --role admin</code>.</>}
          </p>
        )}
        {shown && <OneTimeToken {...shown} onDismiss={() => setShown(null)} />}

        {admin && (
          <SettingsSection id="ingest-tokens" title="Ingest tokens" about={`${INGEST_TEXT} Only admins see these.`}>
            <div className="grid gap-4">
              {open && <Caution>{OPEN_INGEST}</Caution>}
              <CreateToken kind="ingest" ingestOpen={open} onCreate={onCreate("ingest", ingest.create)} />
              <Problem>{ingest.problem}</Problem>
              {ingest.state.kind === "loading" && <p className="text-sm text-ink-3">Loading ingest tokens</p>}
              {ingest.state.kind === "error" && <Problem>{ingest.state.message}</Problem>}
              {ingest.state.kind === "ok" && (
                <TokensTable kind="ingest" tokens={ingest.state.data.tokens} shared={ingest.state.data.shared_token}
                  pending={ingest.pending} onRevoke={(t) => ingest.revoke(t.id)} />
              )}
              <Shell command="trailbeam tokens" />
            </div>
          </SettingsSection>
        )}

        {person && (
          <SettingsSection id="personal-tokens" title="Your personal API tokens" about={`${PERSONAL_TEXT} Only you see yours.`}>
            <div className="grid gap-4">
              <CreateToken kind="personal" onCreate={onCreate("personal", mine.create)} />
              <Problem>{mine.problem}</Problem>
              {mine.state.kind === "loading" && <p className="text-sm text-ink-3">Loading your tokens</p>}
              {mine.state.kind === "error" && <Problem>{mine.state.message}</Problem>}
              {mine.state.kind === "ok" && (
                <TokensTable kind="personal" tokens={mine.state.data.tokens} pending={mine.pending} onRevoke={(t) => mine.revoke(t.id)} />
              )}
              <Shell command="trailbeam tokens --personal" />
            </div>
          </SettingsSection>
        )}
      </div>
    </ShellFrame>
  );
}
