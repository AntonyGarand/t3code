import { createHashHistory, createRouter, createRootRoute } from "@tanstack/react-router";
import { describe, expect, it, vi } from "vite-plus/test";

import { validateSettingsConnectionsSearch } from "../routes/settings.connections";

interface TestWindow {
  location: URL;
  history: {
    state: unknown;
    replaceState: (data: unknown, unused: string, nextUrl?: string | URL) => void;
  };
  addEventListener: () => void;
  removeEventListener: () => void;
}

function installTestWindow(url: string) {
  const testWindow: TestWindow = {
    location: new URL(url),
    history: {
      state: null,
      replaceState: (data, _unused, nextUrl) => {
        testWindow.history.state = data;
        if (nextUrl !== undefined) {
          testWindow.location = new URL(nextUrl, testWindow.location.href);
        }
      },
    },
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
  };
  vi.stubGlobal("window", testWindow);
  return testWindow;
}

const PAIRING_URL = "https://3773-is4vs7aykdee14il5b81r.e2b.app/pair#token=6T95XY4VPBFE";

/**
 * The desktop main process loads
 * `t3code-dev://app/#/settings/connections?connectPairing=<encoded>`. The
 * renderer uses hash history, so the route and its search params must survive
 * TanStack's hash parsing with the pairing URL intact.
 */
describe("connect deep link search parsing", () => {
  it("keeps the full pairing URL through hash-history router parsing", async () => {
    const hash = `#/settings/connections?connectPairing=${encodeURIComponent(PAIRING_URL)}`;
    const testWindow = installTestWindow(`http://localhost/${hash}`);

    const router = createRouter({
      routeTree: createRootRoute().addChildren([]),
      history: createHashHistory({ window: testWindow as unknown as Window }),
    });
    await router.load();

    const search = validateSettingsConnectionsSearch(
      router.state.location.search as Record<string, unknown>,
    );
    expect(search.connectPairing).toBe(PAIRING_URL);
    expect(search.connectSsh).toBeUndefined();
  });

  it("keeps ssh hosts and drops unrelated params", () => {
    const search = validateSettingsConnectionsSearch({
      connectSsh: "e2b-is4vs7aykdee14il5b81r",
      connectPairing: "  ",
      other: "x",
    });
    expect(search).toEqual({ connectSsh: "e2b-is4vs7aykdee14il5b81r" });
    expect(validateSettingsConnectionsSearch({})).toEqual({});
  });
});
