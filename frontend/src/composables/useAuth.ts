import { computed, onUnmounted, ref, type ComputedRef, type Ref } from 'vue';
import type { User, UserManager } from 'oidc-client-ts';
import { extractAllowlistedRoles, extractUsername } from '../auth/oidcClient';
import type { LedgerRole } from '../types/ledger';

export interface AuthController {
  readonly user: Ref<User | null>;
  readonly isAuthenticated: ComputedRef<boolean>;
  readonly isInitializing: Ref<boolean>;
  readonly sessionExpired: Ref<boolean>;
  readonly authError: Ref<string | null>;
  readonly username: ComputedRef<string>;
  readonly roles: ComputedRef<readonly LedgerRole[]>;
  readonly canWrite: ComputedRef<boolean>;
  readonly isOperator: ComputedRef<boolean>;
  readonly accessToken: ComputedRef<string | null>;
  initialize(): Promise<void>;
  login(): Promise<void>;
  logout(): Promise<void>;
  markSessionExpired(reason?: string): void;
}

export function useAuth(userManager: UserManager): AuthController {
  const user = ref<User | null>(null);
  const isInitializing = ref<boolean>(true);
  const sessionExpired = ref<boolean>(false);
  const authError = ref<string | null>(null);

  const onUserLoaded = (loadedUser: User): void => {
    user.value = loadedUser;
    sessionExpired.value = false;
    authError.value = null;
  };

  const onUserUnloaded = (): void => {
    user.value = null;
  };

  const onAccessTokenExpired = (): void => {
    user.value = null;
    sessionExpired.value = true;
    authError.value = 'Ihre Sitzung ist abgelaufen. Bitte melden Sie sich erneut an.';
  };

  const onSilentRenewError = (): void => {
    sessionExpired.value = true;
    authError.value = 'Automatische Token-Erneuerung fehlgeschlagen. Bitte melden Sie sich erneut an.';
  };

  userManager.events.addUserLoaded(onUserLoaded);
  userManager.events.addUserUnloaded(onUserUnloaded);
  userManager.events.addAccessTokenExpired(onAccessTokenExpired);
  userManager.events.addSilentRenewError(onSilentRenewError);

  onUnmounted(() => {
    userManager.events.removeUserLoaded(onUserLoaded);
    userManager.events.removeUserUnloaded(onUserUnloaded);
    userManager.events.removeAccessTokenExpired(onAccessTokenExpired);
    userManager.events.removeSilentRenewError(onSilentRenewError);
  });

  const isAuthenticated = computed<boolean>(() => {
    const current = user.value;
    return current !== null && !current.expired && current.access_token.length > 0;
  });

  const username = computed<string>(() => extractUsername(user.value));

  const roles = computed<readonly LedgerRole[]>(() => extractAllowlistedRoles(user.value?.access_token));

  const canWrite = computed<boolean>(() => roles.value.includes('writer') || roles.value.includes('operator'));

  const isOperator = computed<boolean>(() => roles.value.includes('operator'));

  const accessToken = computed<string | null>(() => {
    const current = user.value;
    if (current === null || current.expired) {
      return null;
    }
    return current.access_token;
  });

  const initialize = async (): Promise<void> => {
    isInitializing.value = true;
    authError.value = null;
    try {
      const url = new URL(window.location.href);
      const isCallback = url.pathname === '/callback' && (url.searchParams.has('code') || url.searchParams.has('error'));
      if (isCallback) {
        const signedIn = await userManager.signinRedirectCallback();
        user.value = signedIn;
        sessionExpired.value = false;
        window.history.replaceState({}, document.title, '/');
        return;
      }

      const existing = await userManager.getUser();
      if (existing !== null && existing.expired) {
        user.value = null;
        sessionExpired.value = true;
        authError.value = 'Ihr gespeichertes Access-Token ist abgelaufen. Bitte melden Sie sich erneut an.';
        return;
      }
      user.value = existing;
      sessionExpired.value = false;
    } catch (error: unknown) {
      user.value = null;
      sessionExpired.value = true;
      authError.value =
        error instanceof Error
          ? `Anmeldung fehlgeschlagen: ${error.message}`
          : 'Anmeldung fehlgeschlagen. Bitte erneut anmelden.';
    } finally {
      isInitializing.value = false;
    }
  };

  const login = async (): Promise<void> => {
    authError.value = null;
    await userManager.signinRedirect();
  };

  const logout = async (): Promise<void> => {
    user.value = null;
    sessionExpired.value = false;
    await userManager.signoutRedirect();
  };

  const markSessionExpired = (reason?: string): void => {
    user.value = null;
    sessionExpired.value = true;
    authError.value = reason ?? 'Token abgelaufen oder ungültig (HTTP 401). Bitte erneut anmelden.';
  };

  return {
    user,
    isAuthenticated,
    isInitializing,
    sessionExpired,
    authError,
    username,
    roles,
    canWrite,
    isOperator,
    accessToken,
    initialize,
    login,
    logout,
    markSessionExpired
  };
}
