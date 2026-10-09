import { getDesktopScheme } from "../electron/ElectronProtocol.ts";

/**
 * Deep links that open the add-environment flow pre-filled:
 *
 * - `t3code://connect?pairing=<url-encoded https://host/pair#token=…>` pairs
 *   a remote environment from a full pairing URL.
 * - `t3code://connect?ssh=<host alias>` prepares an SSH connection using the
 *   system ssh config, so ProxyCommand wake-ups work.
 *
 * The client always confirms; the link only pre-fills the form.
 */

export const CONNECT_DEEP_LINK_HOST = "connect";

export type DesktopConnectDeepLink =
  | { readonly kind: "pairing"; readonly pairingUrl: string }
  | { readonly kind: "ssh"; readonly sshHost: string };

/** Parse a `t3code://connect` URL. Returns null for anything else. */
export function readConnectDeepLink(
  value: string | undefined,
  isDevelopment: boolean,
): DesktopConnectDeepLink | null {
  if (!value) return null;
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return null;
  }
  if (url.protocol !== `${getDesktopScheme(isDevelopment)}:`) return null;
  if (url.host !== CONNECT_DEEP_LINK_HOST) return null;

  const pairing = url.searchParams.get("pairing");
  if (pairing !== null && pairing.trim() !== "") {
    let parsed: URL;
    try {
      parsed = new URL(pairing);
    } catch {
      return null;
    }
    // Only plain web pairing URLs; no credentials, no other schemes.
    if (parsed.protocol !== "https:" && parsed.protocol !== "http:") return null;
    if (parsed.username || parsed.password) return null;
    return { kind: "pairing", pairingUrl: parsed.toString() };
  }

  const ssh = url.searchParams.get("ssh");
  if (ssh !== null && ssh.trim() !== "") {
    const host = ssh.trim();
    // An ssh config alias, hostname, or host:port — never a path or URL.
    if (/[\s/]/.test(host)) return null;
    return { kind: "ssh", sshHost: host };
  }

  return null;
}

/** Build the in-app settings URL the connect deep link navigates to. */
export function connectDeepLinkTarget(
  link: DesktopConnectDeepLink,
  isDevelopment: boolean,
): string {
  const params = new URLSearchParams();
  if (link.kind === "pairing") {
    params.set("connectPairing", link.pairingUrl);
  } else {
    params.set("connectSsh", link.sshHost);
  }
  const target = new URL(`${getDesktopScheme(isDevelopment)}://app/`);
  // The desktop renderer uses hash routing, so the route and its search
  // params live inside the hash.
  target.hash = `#/settings/connections?${params.toString()}`;
  return target.toString();
}
