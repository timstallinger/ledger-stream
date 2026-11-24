export interface RuntimeConfig {
  readonly apiBaseUrl: string;
  readonly oidcAuthority: string;
  readonly oidcClientId: string;
  readonly oidcRedirectUri: string;
  readonly oidcSilentRedirectUri: string;
  readonly oidcPostLogoutRedirectUri: string;
}

function readNonEmpty(value: string | undefined, fallback: string): string {
  if (value !== undefined && value.trim().length > 0) {
    return value.trim();
  }
  return fallback;
}

export function loadRuntimeConfig(): RuntimeConfig {
  const injected = window.__LEDGER_CONFIG__;
  const origin = window.location.origin;

  const apiBaseUrl = readNonEmpty(
    injected?.apiBaseUrl ?? import.meta.env.VITE_API_BASE_URL,
    'http://localhost:8080'
  ).replace(/\/+$/, '');

  const oidcAuthority = readNonEmpty(
    injected?.oidcAuthority ?? import.meta.env.VITE_OIDC_AUTHORITY,
    'http://localhost:8180/realms/ledger-stream'
  ).replace(/\/+$/, '');

  const oidcClientId = readNonEmpty(
    injected?.oidcClientId ?? import.meta.env.VITE_OIDC_CLIENT_ID,
    'ledger-stream-spa'
  );

  return {
    apiBaseUrl,
    oidcAuthority,
    oidcClientId,
    oidcRedirectUri: `${origin}/callback`,
    oidcSilentRedirectUri: `${origin}/silent-renew.html`,
    oidcPostLogoutRedirectUri: `${origin}/`
  };
}
