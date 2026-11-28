"use client";

/*
  The account card (QV-587), the hosted app's AccountCard made shared: who is signed in, the ways they can sign in,
  and their passkeys. It fetches nothing. The host passes the person and the ways (self-hosted Trailbeam: /api/me,
  through accountWays in text.ts), and, where it has passkeys, the list with an add and a remove it sends itself
  (the hosted app: its auth client; self-hosted: /api/me/passkeys). Without `passkeys` that part isn't drawn.
  Sign out is not on the card: it is the sidebar's, in both products (board, QV-588). Names in the UI face, emails and dates in mono.
  A client module, reached through @trailbeam/ui/design, never the main entry.
*/
import { useState, type ReactNode } from "react";
import { Fingerprint, Trash2, UserRound } from "lucide-react";
import { Button } from "../design/Button.tsx";
import { Card } from "../design/Card.tsx";
import { Confirm, FormError, StateMark } from "../design/Settings.tsx";
import { PASSKEYS_ABOUT, WAY_OFF } from "./text.ts";
import type { AccountWay, Passkey } from "./types.ts";

export type AccountPasskeys = {
  /** null while the list is being read */
  keys: Passkey[] | null;
  onAdd: () => void;
  onRemove: (id: string) => void;
  /** an add or a remove is in progress: the buttons are off */
  busy?: boolean;
  error?: ReactNode;
};

export type AccountCardProps = {
  /** null while the host is still asking who is signed in */
  user: { name: string; email: string; role?: string } | null;
  /** Said in place of the person when the host knows there is none (self-hosted, signed in with the shared token,
      QV-767). Without it, a null `user` reads as still loading. */
  signedIn?: ReactNode;
  ways?: AccountWay[];
  passkeys?: AccountPasskeys;
};

const day = (d: Passkey["createdAt"]) => new Date(d).toISOString().slice(0, 10);

/** The confirmation before a passkey is removed (QV-739). */
export const removePasskeySentence = (k: Pick<Passkey, "name">) =>
  `Remove ${k.name || "this passkey"}? It can't sign you in after this.`;

export function AccountCard({ user, signedIn, ways = [], passkeys }: AccountCardProps) {
  const [removing, setRemoving] = useState<string | null>(null);
  return (
    <Card className="p-6">
      <div className="flex items-start gap-4">
        <span aria-hidden className="grid size-10 shrink-0 place-items-center rounded-xl bg-sunken text-ink-2"><UserRound size={18} /></span>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-start gap-x-4 gap-y-2">
            <div className="min-w-0">
              <h2 className="font-semibold text-ink">Sign-in</h2>
              <p className="mt-1 text-sm text-ink-2">
                {user
                  ? <>Signed in as <span className="text-ink">{user.name}</span>, <span className="break-all font-mono text-caption">{user.email}</span>{user.role && <>, {user.role.toLowerCase()}</>}</>
                  : signedIn ?? "Loading"}
              </p>
            </div>
          </div>

          {ways.length > 0 && (
            <>
              <h3 className="mt-5 text-sm font-medium text-ink">Ways to sign in</h3>
              <ul className="mt-2 divide-y divide-line rounded-lg border border-line" aria-label="Ways to sign in">
                {ways.map((w) => (
                  <li key={w.key} data-way={w.key} className="grid gap-1 px-3 py-2.5 text-sm">
                    <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                      <span className="text-ink">{w.title}</span>{" "}
                      {w.value && <span className="min-w-0 break-all font-mono text-caption text-ink-3">{w.value}</span>}{" "}
                      {w.off && <span className="ml-auto"><StateMark tone="idle">Off</StateMark></span>}
                    </div>
                    <p className="text-xs leading-relaxed text-ink-2">{w.detail}</p>
                    {/* said in the row, not on hover */}
                    {w.off && <p className="text-xs leading-relaxed text-ink-3">{WAY_OFF}</p>}
                  </li>
                ))}
              </ul>
            </>
          )}

          {passkeys && (
            <>
              <div className="mt-5 flex flex-wrap items-center gap-3">
                <h3 className="text-sm font-medium text-ink">Passkeys</h3>
                <Button size="sm" variant="ink" className="ml-auto" onClick={passkeys.onAdd} disabled={passkeys.busy}>
                  <Fingerprint size={14} className="mr-1.5" aria-hidden /> Add a passkey
                </Button>
              </div>
              <p className="mt-1 text-sm text-ink-2">{PASSKEYS_ABOUT}</p>
              <div className="mt-2"><FormError>{passkeys.error}</FormError></div>
              {passkeys.keys && passkeys.keys.length > 0 && (
                <ul className="mt-3 divide-y divide-line rounded-lg border border-line" aria-label="Passkeys">
                  {passkeys.keys.map((k) => (
                    <li key={k.id} className="flex flex-wrap items-center gap-3 px-3 py-2 text-sm">
                      <span className="text-ink">{k.name || "Passkey"}</span>{" "}
                      <span className="text-ink-3">added <span className="font-mono text-caption">{day(k.createdAt)}</span></span>
                      {removing === k.id ? (
                        <Confirm label={`Remove ${k.name || "passkey"}`} action="Remove" disabled={passkeys.busy} className="ml-auto justify-end"
                          onCancel={() => setRemoving(null)} onConfirm={() => { setRemoving(null); passkeys.onRemove(k.id); }}>{removePasskeySentence(k)}</Confirm>
                      ) : (
                        <button type="button" onClick={() => setRemoving(k.id)} disabled={passkeys.busy} aria-label={`Remove ${k.name || "passkey"}`}
                          className="ml-auto rounded-md p-1 text-ink-3 hover:bg-sunken hover:text-ink disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-transparent">
                          <Trash2 size={14} aria-hidden />
                        </button>
                      )}
                    </li>
                  ))}
                </ul>
              )}
            </>
          )}
        </div>
      </div>
    </Card>
  );
}
