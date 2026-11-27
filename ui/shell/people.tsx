/*
  The People page at /people (admins only: the server answers members with its "Not allowed" page). It lists
  /api/users with the shared PeopleTable, and sends each change (contract/openapi.yaml, tag People):
  add (POST /api/users, which answers with the invite link, or emails it when the server has a mail server), role and disabled (PATCH /api/users/{id}),
  remove (DELETE /api/users/{id}) and a new invite or reset link (POST /api/users/{id}/link). After each, the list is
  read again, so the page shows what the server holds.
*/
import { useCallback, useEffect, useState } from "react";
import {
  AddPerson, OneTimeLink, PeopleTable, ROLE_TEXT,
  type Person, type PersonWithLink, type Role,
} from "../src/index.ts";
import { ApiError, Frame, getJSON, sendJSON } from "./chrome.tsx";

type State = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; people: Person[]; sendsEmail: boolean };

export function PeoplePage() {
  const [state, setState] = useState<State>({ kind: "loading" });
  const [pending, setPending] = useState<number | null>(null);
  const [problem, setProblem] = useState("");
  const [shown, setShown] = useState<PersonWithLink | null>(null);

  const load = useCallback(() => getJSON<{ users: Person[]; sends_email?: boolean }>("/api/users")
    .then((d) => setState({ kind: "ok", people: d.users, sendsEmail: d.sends_email === true }))
    .catch((e: Error) => setState({ kind: "error", message: e.message })), []);

  useEffect(() => { document.title = "People · Trailbeam"; void load(); }, [load]);

  /** Send one change for `p`, then read the list again; a refusal (409 last_admin, ...) is shown above the table. */
  const change = (p: Person, send: () => Promise<unknown>, after?: (r: unknown) => void) => {
    setPending(p.id);
    setProblem("");
    send()
      .then((r) => { after?.(r); return load(); })
      .catch((e: Error) => { setProblem(e.message); return e instanceof ApiError && e.status === 404 ? load() : undefined; })
      .finally(() => setPending(null));
  };

  const onRole = (p: Person, role: Role) => change(p, () => sendJSON("PATCH", `/api/users/${p.id}`, { role }), () => {
    if (p.you && role !== "admin") location.assign("/");  // no longer an admin: this page is closed to you now
  });
  const onDisabled = (p: Person, disabled: boolean) => change(p, () => sendJSON("PATCH", `/api/users/${p.id}`, { disabled }));
  const onRemove = (p: Person) => change(p, () => sendJSON("DELETE", `/api/users/${p.id}`), () => {
    if (shown?.user.id === p.id) setShown(null);
    if (p.you) location.assign("/logout");
  });
  const onLink = (p: Person) => change(p, () => sendJSON<PersonWithLink>("POST", `/api/users/${p.id}/link`, {}), (r) => setShown(r as PersonWithLink));
  const onAdd = async (p: { email: string; name: string; role: Role }) => {
    setProblem("");
    const r = await sendJSON<PersonWithLink>("POST", "/api/users", { email: p.email, role: p.role, ...(p.name && { name: p.name }) });
    setShown(r);
    await load();
  };

  const sendsEmail = state.kind === "ok" && state.sendsEmail;
  return (
    <Frame>
      <h1 className="text-[1.75rem] text-sky-ink">People</h1>
      <p className="mt-1 max-w-[80ch] text-sky-ink-2">Who can sign in to this Trailbeam. {ROLE_TEXT}</p>
      <section className="mt-5 rounded-[16px] border border-sky-line bg-sky-2/55 px-5 py-4">
        <h2 className="mb-3 text-[1.1rem] text-sky-ink">Add a person</h2>
        <AddPerson onAdd={onAdd} sendsEmail={sendsEmail} />
        <p className="mt-3 max-w-[80ch] text-sm text-sky-ink-3">
          {sendsEmail
            ? "Adding someone emails them a one-time invite link, valid for 7 days: they open it and choose their password."
            : "Adding someone makes a one-time invite link, valid for 7 days. Send it to them privately: they open it and choose their password."}
        </p>
      </section>
      {shown && <div className="mt-4"><OneTimeLink person={shown.user} link={shown.link} email={shown.email} onDismiss={() => setShown(null)} /></div>}
      {problem && <p className="mt-4 rounded-[12px] border border-sky-line bg-sky-2 px-4 py-3 text-[15px] text-sky-ink-2" role="alert">{problem}</p>}
      <div className="mt-5">
        {state.kind === "loading" && <p className="text-sky-ink-3">Loading people</p>}
        {state.kind === "error" && <p className="text-sky-ink-2" role="alert">{state.message}</p>}
        {state.kind === "ok" && (
          <div className="overflow-x-auto rounded-[16px] border border-sky-line bg-sky-2/55">
            <PeopleTable people={state.people} pending={pending} onRole={onRole} onDisabled={onDisabled} onRemove={onRemove} onLink={onLink} />
          </div>
        )}
      </div>
      <p className="mt-3 max-w-[80ch] text-sm text-sky-ink-3">
        A new role, or disabling someone, signs them out everywhere within 20 seconds. A password reset link lasts 24 hours.
        The same from a shell: <code className="font-mono text-caption">trailbeam users</code>.
      </p>
    </Frame>
  );
}
