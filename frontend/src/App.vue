<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue';
import { createLedgerApiClient, LedgerApiError } from './api/ledgerClient';
import { createOidcUserManager } from './auth/oidcClient';
import AccountDetail from './components/AccountDetail.vue';
import AccountList from './components/AccountList.vue';
import ErrorBanner from './components/ErrorBanner.vue';
import { useAuth } from './composables/useAuth';
import { useBookingForm } from './composables/useBookingForm';
import { useVisibilityPolling } from './composables/useVisibilityPolling';
import { loadRuntimeConfig } from './config/runtimeConfig';
import type { AccountDto, ApiErrorState, BookingDto, CreateAccountPayload } from './types/ledger';

const runtimeConfig = loadRuntimeConfig();
const userManager = createOidcUserManager(runtimeConfig);
const auth = useAuth(userManager);
const apiClient = createLedgerApiClient(runtimeConfig.apiBaseUrl, () => auth.accessToken.value);

const accounts = ref<readonly AccountDto[]>([]);
const selectedAccountId = ref<string | null>(null);
const bookings = ref<readonly BookingDto[]>([]);
const isLoadingAccounts = ref<boolean>(false);
const isLoadingBookings = ref<boolean>(false);
const globalError = ref<ApiErrorState | null>(null);

const selectedAccount = computed<AccountDto | null>(() => {
  const id = selectedAccountId.value;
  if (id === null) {
    return null;
  }
  return accounts.value.find((acc) => acc.id === id) ?? null;
});

const handleGlobalError = (error: unknown): void => {
  if (error instanceof LedgerApiError) {
    globalError.value = error.state;
    if (error.state.kind === 'unauthorized') {
      auth.markSessionExpired(error.state.message);
    }
    return;
  }
  globalError.value = {
    kind: 'network',
    status: null,
    message: 'Verbindung zum Backend fehlgeschlagen.'
  };
};

const loadAccountsAndSelected = async (): Promise<void> => {
  if (!auth.isAuthenticated.value) {
    return;
  }
  isLoadingAccounts.value = true;
  try {
    const loadedAccounts = await apiClient.listAccounts();
    accounts.value = loadedAccounts;
    globalError.value = null;

    if (selectedAccountId.value === null && loadedAccounts[0] !== undefined) {
      selectedAccountId.value = loadedAccounts[0].id;
    }

    if (selectedAccountId.value !== null) {
      isLoadingBookings.value = true;
      bookings.value = await apiClient.listBookings(selectedAccountId.value);
    }
  } catch (error: unknown) {
    handleGlobalError(error);
    throw error;
  } finally {
    isLoadingAccounts.value = false;
    isLoadingBookings.value = false;
  }
};

const bookingForm = useBookingForm(
  apiClient,
  async () => {
    await loadAccountsAndSelected();
  },
  () => {
    auth.markSessionExpired();
  }
);

const polling = useVisibilityPolling(loadAccountsAndSelected, {
  baseIntervalMs: 5000,
  maxIntervalMs: 60000,
  enabled: () => auth.isAuthenticated.value
});

const handleSelectAccount = async (accountId: string): Promise<void> => {
  selectedAccountId.value = accountId;
  bookingForm.closeForm();
  isLoadingBookings.value = true;
  try {
    bookings.value = await apiClient.listBookings(accountId);
    globalError.value = null;
  } catch (error: unknown) {
    handleGlobalError(error);
  } finally {
    isLoadingBookings.value = false;
  }
};

const handleCreateAccount = async (payload: CreateAccountPayload): Promise<void> => {
  try {
    const created = await apiClient.createAccount(payload);
    globalError.value = null;
    await loadAccountsAndSelected();
    await handleSelectAccount(created.id);
  } catch (error: unknown) {
    handleGlobalError(error);
  }
};

const handleCancelBooking = async (bookingId: string): Promise<void> => {
  const accountId = selectedAccountId.value;
  if (accountId === null) {
    return;
  }
  try {
    await apiClient.cancelBooking(
      accountId,
      bookingId,
      { reference: 'Storno via Web UI' },
      crypto.randomUUID()
    );
    globalError.value = null;
    await loadAccountsAndSelected();
  } catch (error: unknown) {
    handleGlobalError(error);
  }
};

watch(
  () => auth.isAuthenticated.value,
  (authenticated) => {
    if (authenticated) {
      void polling.triggerNow();
    }
  }
);

onMounted(async () => {
  await auth.initialize();
  if (auth.isAuthenticated.value) {
    await polling.triggerNow();
  }
});
</script>

<template>
  <div class="app-shell">
    <header class="topbar">
      <div class="topbar__brand">
        <h1>ledger-stream</h1>
        <span class="muted">Transactional Outbox &amp; Idempotent Ledger</span>
      </div>

      <div class="topbar__right">
        <template v-if="auth.isAuthenticated.value">
          <span class="polling-status">
            <template v-if="polling.isPausedHidden.value">Polling pausiert (Tab inaktiv)</template>
            <template v-else>Live-Sync alle {{ polling.currentIntervalMs.value / 1000 }}s</template>
          </span>
          <span class="user-chip">
            {{ auth.username.value }}
            <code v-if="auth.roles.value.length > 0">[{{ auth.roles.value.join(', ') }}]</code>
          </span>
          <button type="button" class="btn btn--secondary btn--sm" @click="auth.logout()">
            Abmelden
          </button>
        </template>
        <button v-else type="button" class="btn btn--primary btn--sm" @click="auth.login()">
          Mit Keycloak anmelden
        </button>
      </div>
    </header>

    <main class="main-content">
      <section v-if="auth.isInitializing.value" class="panel auth-gate">
        <p>Authentifizierung wird geprüft…</p>
      </section>

      <section v-else-if="auth.sessionExpired.value" class="panel auth-gate auth-gate--expired" role="alert">
        <h2>Sitzung abgelaufen</h2>
        <p>{{ auth.authError.value ?? 'Ihr Access-Token ist abgelaufen. Bitte melden Sie sich erneut an.' }}</p>
        <button type="button" class="btn btn--primary" @click="auth.login()">
          Erneut mit Keycloak anmelden
        </button>
      </section>

      <section v-else-if="!auth.isAuthenticated.value" class="panel auth-gate">
        <h2>Anmeldung erforderlich</h2>
        <p>Melden Sie sich per OIDC Authorization Code + PKCE (S256) bei Keycloak an.</p>
        <button type="button" class="btn btn--primary" @click="auth.login()">
          Mit Keycloak anmelden
        </button>
      </section>

      <template v-else>
        <ErrorBanner
          :error="globalError"
          @relogin="auth.login()"
          @retry="polling.triggerNow()"
        />

        <div class="workspace-grid">
          <AccountList
            :accounts="accounts"
            :selected-account-id="selectedAccountId"
            :can-write="auth.canWrite.value"
            :is-loading="isLoadingAccounts"
            @select="handleSelectAccount"
            @create="handleCreateAccount"
          />

          <AccountDetail
            :account="selectedAccount"
            :bookings="bookings"
            :can-write="auth.canWrite.value"
            :is-loading-bookings="isLoadingBookings"
            :booking-form="bookingForm"
            @cancel-booking="handleCancelBooking"
            @relogin="auth.login()"
          />
        </div>
      </template>
    </main>
  </div>
</template>
