import { createFileRoute } from "@tanstack/react-router";

import { ConnectionsSettings } from "../components/settings/ConnectionsSettings";

export interface SettingsConnectionsSearch {
  readonly connectPairing?: string;
  readonly connectSsh?: string;
}

/** Search params carried by `t3code://connect` deep links; consumed on mount. */
export const validateSettingsConnectionsSearch = (raw: Record<string, unknown>) => ({
  ...(typeof raw.connectPairing === "string" && raw.connectPairing.trim()
    ? { connectPairing: raw.connectPairing }
    : {}),
  ...(typeof raw.connectSsh === "string" && raw.connectSsh.trim()
    ? { connectSsh: raw.connectSsh }
    : {}),
});

export const Route = createFileRoute("/settings/connections")({
  validateSearch: validateSettingsConnectionsSearch,
  component: ConnectionsSettings,
});
