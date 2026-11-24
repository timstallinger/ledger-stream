import { UserManager, WebStorageStateStore, type User } from 'oidc-client-ts';
import type { RuntimeConfig } from '../config/runtimeConfig';
import { isRecord, isString, type LedgerRole } from '../types/ledger';

const ALLOWED_ROLES: ReadonlySet<LedgerRole> = new Set<LedgerRole>(['viewer', 'writer', 'operator']);

function isLedgerRole(value: string): value is LedgerRole {
  return value === 'viewer' || value === 'writer' || value === 'operator';
}

export function createOidcUserManager(config: RuntimeConfig): UserManager {
  return new UserManager({
    authority: config.oidcAuthority,
    client_id: config.oidcClientId,
    redirect_uri: config.oidcRedirectUri,
    silent_redirect_uri: config.oidcSilentRedirectUri,
    post_logout_redirect_uri: config.oidcPostLogoutRedirectUri,
    response_type: 'code',
    scope: 'openid profile email',
    automaticSilentRenew: true,
    loadUserInfo: false,
    monitorSession: false,
    userStore: new WebStorageStateStore({ store: window.sessionStorage })
  });
}

function decodeBase64UrlJson(segment: string): unknown {
  const padded = segment.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(segment.length / 4) * 4, '=');
  const decoded = atob(padded);
  return JSON.parse(decoded);
}

/**
 * Extracts ONLY allowlisted realm roles (viewer, writer, operator) from the access token JWT.
 */
export function extractAllowlistedRoles(accessToken: string | undefined): readonly LedgerRole[] {
  if (accessToken === undefined) {
    return [];
  }
  const parts = accessToken.split('.');
  const payloadSegment = parts[1];
  if (payloadSegment === undefined) {
    return [];
  }
  try {
    const parsed = decodeBase64UrlJson(payloadSegment);
    if (!isRecord(parsed)) {
      return [];
    }
    const realmAccess = parsed['realm_access'];
    if (!isRecord(realmAccess)) {
      return [];
    }
    const rawRoles = realmAccess['roles'];
    if (!Array.isArray(rawRoles)) {
      return [];
    }
    const matched = new Set<LedgerRole>();
    for (const candidate of rawRoles) {
      if (isString(candidate) && isLedgerRole(candidate) && ALLOWED_ROLES.has(candidate)) {
        matched.add(candidate);
      }
    }
    return Array.from(matched);
  } catch {
    return [];
  }
}

export function extractUsername(user: User | null): string {
  if (user === null) {
    return '';
  }
  const preferred = user.profile.preferred_username;
  if (isString(preferred) && preferred.trim().length > 0) {
    return preferred;
  }
  return user.profile.sub;
}
