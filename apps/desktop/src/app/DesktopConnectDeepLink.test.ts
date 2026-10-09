import { describe, it, expect } from "vite-plus/test";

import * as DesktopConnectDeepLink from "./DesktopConnectDeepLink.ts";

const PAIRING_URL = "https://3773-ikbmbrwgxyvdrag8z8tt6.e2b.app/pair#token=HBEZX87HCEE9";

const encodePairing = (url: string) => `t3code://connect?pairing=${encodeURIComponent(url)}`;

describe("readConnectDeepLink", () => {
  it("parses a remote pairing link from the production scheme", () => {
    expect(DesktopConnectDeepLink.readConnectDeepLink(encodePairing(PAIRING_URL), false)).toEqual({
      kind: "pairing",
      pairingUrl: PAIRING_URL,
    });
  });

  it("accepts the development scheme only in development", () => {
    const devLink = `t3code-dev://connect?ssh=box`;
    expect(DesktopConnectDeepLink.readConnectDeepLink(devLink, true)).toEqual({
      kind: "ssh",
      sshHost: "box",
    });
    expect(DesktopConnectDeepLink.readConnectDeepLink(devLink, false)).toBeNull();
    expect(DesktopConnectDeepLink.readConnectDeepLink(encodePairing(PAIRING_URL), true)).toBeNull();
  });

  it("parses ssh aliases including host:port and user@host", () => {
    expect(DesktopConnectDeepLink.readConnectDeepLink("t3code://connect?ssh=box", false)).toEqual({
      kind: "ssh",
      sshHost: "box",
    });
    expect(
      DesktopConnectDeepLink.readConnectDeepLink("t3code://connect?ssh=dev%40box.io%3A2222", false),
    ).toEqual({ kind: "ssh", sshHost: "dev@box.io:2222" });
  });

  it("rejects non-http pairing URLs, other hosts, and malformed input", () => {
    expect(
      DesktopConnectDeepLink.readConnectDeepLink(
        `t3code://connect?pairing=${encodeURIComponent(`file:///etc/passwd#token=x`)}`,
        false,
      ),
    ).toBeNull();
    expect(
      DesktopConnectDeepLink.readConnectDeepLink(
        `t3code://connect?pairing=${encodeURIComponent(`https://user:pass@host/pair#token=x`)}`,
        false,
      ),
    ).toBeNull();
    expect(
      DesktopConnectDeepLink.readConnectDeepLink("t3code://app/connect?ssh=box", false),
    ).toBeNull();
    expect(DesktopConnectDeepLink.readConnectDeepLink("t3code://connect", false)).toBeNull();
    expect(DesktopConnectDeepLink.readConnectDeepLink("not a url", false)).toBeNull();
    expect(DesktopConnectDeepLink.readConnectDeepLink(undefined, false)).toBeNull();
    expect(
      DesktopConnectDeepLink.readConnectDeepLink(
        `t3code://connect?ssh=${encodeURIComponent("box/path")}`,
        false,
      ),
    ).toBeNull();
  });
});

describe("connectDeepLinkTarget", () => {
  it("routes pairing links to the connections settings inside the hash", () => {
    const target = DesktopConnectDeepLink.connectDeepLinkTarget(
      { kind: "pairing", pairingUrl: PAIRING_URL },
      false,
    );
    const url = new URL(target);
    expect(url.protocol).toBe("t3code:");
    expect(url.host).toBe("app");
    expect(url.hash.startsWith("#/settings/connections?")).toBe(true);
    const search = new URLSearchParams(url.hash.slice("#/settings/connections".length));
    expect(search.get("connectPairing")).toBe(PAIRING_URL);
  });

  it("routes ssh links and keeps the dev scheme in development", () => {
    const target = DesktopConnectDeepLink.connectDeepLinkTarget(
      { kind: "ssh", sshHost: "box" },
      true,
    );
    const url = new URL(target);
    expect(url.protocol).toBe("t3code-dev:");
    const search = new URLSearchParams(url.hash.slice("#/settings/connections".length));
    expect(search.get("connectSsh")).toBe("box");
  });
});
