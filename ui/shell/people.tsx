/*
  The People page at /people (admins only: the server answers members with its "Not allowed" page), in the shell's
  frame (frame.tsx) on the design system. It lists /api/users with the shared PeopleTable, and sends each change
  (contract/openapi.yaml, tag People): add (POST /api/users, which answers with the invite link, or emails it when
  the server has a mail server), role and disabled (PATCH /api/users/{id}), remove (DELETE /api/users/{id}) and a new
  invite or reset link (POST /api/users/{id}/link). After each, the list is read again, so the page shows what the
  server holds.
*/
import { useCallback, useEffect, useState } from "react";
import { ROLE_TEXT, type Person, type PersonWithLink, type Role } from "../src/index.ts";
import { AddPerson, OneTimeLink, PageHeader, PeopleTable, Problem, SettingsSection } from "../src/design/index.ts";
import { ApiError, getJSON, sendJSON } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";

type State = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; people: Person[]; sendsEmail: boolean };

export const INVITE_TEXT = {
  email: "Adding someone emails them a one-time invite link, valid for 7 days: they open it and choose their password.",
  link: "Adding someone makes a one-time invite link, valid for 7 days. Send it to them privately: they open it and choose their password.",
};

export function PeoplePage() {
  const [state, setState] = useState<State>({ kind: "loading" });
  const [pending, setPending] = useState<number | null>(null);
  const [problem, setProblem] = useState("");
  const [shown, setShown] = useState<PersonWithLink | null>(null);

  const load = useCallback(() => getJSON<{ users: Person[]; sends_email?: boolean }>("/api/users")
    .then((d) => setState({ kind: "ok", people: d.users, sendsEmail: d.sends_email === true }))
    .catch((e: Error) => setState({ kind: "error", message: e.message })), []);

  useEffect(() => { document.title = "People · Trailbeam"; void load(); }, [load]);

  /** Send one change for `p`, then read the list again; a refusal (409 last_admin, ...) is shown above the list. */
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
    <ShellFrame path="/people">
      <PageHeader title="People" subtitle="Who can sign in to this Trailbeam, and what they can do." search={<ShellSearch />} />
      <div className="grid max-w-5xl gap-12 px-8 pb-16">
        <SettingsSection title="Add a person" about={sendsEmail ? INVITE_TEXT.email : INVITE_TEXT.link}>
          <AddPerson onAdd={onAdd} sendsEmail={sendsEmail} />
          {shown && <div className="mt-4"><OneTimeLink person={shown.user} link={shown.link} email={shown.email} onDismiss={() => setShown(null)} /></div>}
        </SettingsSection>
        <SettingsSection title="Everyone" about={ROLE_TEXT}>
          <div className="grid gap-3">
            <Problem>{problem}</Problem>
            {state.kind === "loading" && <p className="text-sm text-ink-3">Loading people</p>}
            {state.kind === "error" && <Problem>{state.message}</Problem>}
            {state.kind === "ok" && (
              <PeopleTable people={state.people} pending={pending} onRole={onRole} onDisabled={onDisabled} onRemove={onRemove} onLink={onLink} />
            )}
            <p className="max-w-2xl text-xs leading-relaxed text-ink-3">
              A new role, or disabling someone, signs them out everywhere within 20 seconds. A password reset link lasts 24 hours.
              The same from a shell: <code className="font-mono text-caption text-ink-2">trailbeam users</code>.
            </p>
          </div>
        </SettingsSection>
      </div>
    </ShellFrame>
  );
}
