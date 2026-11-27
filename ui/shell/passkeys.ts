/*
  Passkeys in the browser (WebAuthn), for the Account page: the server's options as JSON (binary fields in base64url,
  POST /api/me/passkeys/options) into what navigator.credentials.create takes, and the credential it makes back
  into JSON for POST /api/me/passkeys. Written out rather than PublicKeyCredential.parseCreationOptionsFromJSON and
  toJSON, which not every browser Trailbeam supports has yet. contract/openapi.yaml, tag Account.
*/

export function fromB64url(s: string): ArrayBuffer {
  const b64 = s.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (s.length % 4)) % 4);
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out.buffer;
}

export function toB64url(buf: ArrayBuffer | ArrayBufferView): string {
  const bytes = buf instanceof ArrayBuffer ? new Uint8Array(buf) : new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

type Descriptor = { id: string; type: "public-key"; transports?: string[] };
/** PublicKeyCredentialCreationOptions as the server sends them (webauthn's options_to_json). */
export type CreationOptionsJSON = {
  rp: { id?: string; name: string };
  user: { id: string; name: string; displayName: string };
  challenge: string;
  pubKeyCredParams: { type: "public-key"; alg: number }[];
  timeout?: number;
  excludeCredentials?: Descriptor[];
  authenticatorSelection?: AuthenticatorSelectionCriteria;
  attestation?: AttestationConveyancePreference;
};

export function creationOptions(o: CreationOptionsJSON): PublicKeyCredentialCreationOptions {
  return {
    ...o,
    user: { ...o.user, id: fromB64url(o.user.id) },
    challenge: fromB64url(o.challenge),
    excludeCredentials: (o.excludeCredentials ?? []).map((c) => ({ ...c, id: fromB64url(c.id), transports: c.transports as AuthenticatorTransport[] | undefined })),
  };
}

/** The part of a PublicKeyCredential from navigator.credentials.create that the server reads. */
type Made = {
  id: string; rawId: ArrayBuffer; type: string;
  response: { clientDataJSON: ArrayBuffer; attestationObject: ArrayBuffer; getTransports?: () => string[] };
  getClientExtensionResults?: () => AuthenticationExtensionsClientOutputs;
};

export function registrationJSON(c: Made) {
  return {
    id: c.id, rawId: toB64url(c.rawId), type: c.type,
    response: {
      clientDataJSON: toB64url(c.response.clientDataJSON), attestationObject: toB64url(c.response.attestationObject),
      transports: c.response.getTransports?.() ?? [],
    },
    clientExtensionResults: c.getClientExtensionResults?.() ?? {},
  };
}

/** A name for a new passkey from the browser it is made in, as the hosted app names them: "Chrome on Mac". */
export function deviceName(ua: string): string {
  const os = /iPhone|iPad/.test(ua) ? "iPhone" : /Mac/.test(ua) ? "Mac" : /Android/.test(ua) ? "Android" : /Windows/.test(ua) ? "Windows" : /Linux/.test(ua) ? "Linux" : "This device";
  const browser = /Edg\//.test(ua) ? "Edge" : /Firefox\//.test(ua) ? "Firefox" : /Chrome\//.test(ua) ? "Chrome" : /Safari\//.test(ua) ? "Safari" : "";
  return browser ? `${browser} on ${os}` : os;
}

/** What to tell the person when the browser's part failed: they canceled, it timed out, or it was refused. */
export function browserProblem(e: unknown): string {
  const name = e instanceof Error || (typeof e === "object" && e !== null && "name" in e) ? (e as { name: string }).name : "";
  if (name === "NotAllowedError") return "The passkey wasn't saved. Your browser may have canceled it.";
  if (name === "InvalidStateError") return "This device has a passkey for your account already.";
  if (name === "SecurityError") return "Your browser refused: passkeys work only at the address set as Trailbeam's public URL, over HTTPS.";
  return "The passkey wasn't saved. Your browser may have canceled it.";
}
