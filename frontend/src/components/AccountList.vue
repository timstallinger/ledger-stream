<script setup lang="ts">
import { ref } from 'vue';
import type { AccountDto, CreateAccountPayload } from '../types/ledger';
import { formatMoney } from '../utils/money';

defineProps<{
  readonly accounts: readonly AccountDto[];
  readonly selectedAccountId: string | null;
  readonly canWrite: boolean;
  readonly isLoading: boolean;
}>();

const emit = defineEmits<{
  (e: 'select', accountId: string): void;
  (e: 'create', payload: CreateAccountPayload): void;
}>();

const showCreateForm = ref<boolean>(false);
const accountNumber = ref<string>('');
const currency = ref<string>('EUR');

const submitCreate = (): void => {
  const trimmedNumber = accountNumber.value.trim();
  const trimmedCurrency = currency.value.trim().toUpperCase();
  if (trimmedNumber.length === 0) {
    return;
  }
  emit('create', { accountNumber: trimmedNumber, currency: trimmedCurrency || 'EUR' });
  accountNumber.value = '';
  showCreateForm.value = false;
};
</script>

<template>
  <aside class="panel account-list">
    <div class="panel__header">
      <h2>Konten</h2>
      <button
        v-if="canWrite"
        type="button"
        class="btn btn--primary btn--sm"
        @click="showCreateForm = !showCreateForm"
      >
        {{ showCreateForm ? 'Abbrechen' : '+ Konto' }}
      </button>
    </div>

    <form v-if="showCreateForm && canWrite" class="inline-form" @submit.prevent="submitCreate">
      <label class="field">
        <span>Kontonummer</span>
        <input
          v-model="accountNumber"
          type="text"
          placeholder="z. B. DE-1001"
          required
          maxlength="64"
        >
      </label>
      <label class="field">
        <span>Währung</span>
        <input v-model="currency" type="text" maxlength="3" required>
      </label>
      <button type="submit" class="btn btn--primary btn--sm">Konto anlegen</button>
    </form>

    <p v-if="isLoading && accounts.length === 0" class="muted">Konten werden geladen…</p>
    <p v-else-if="accounts.length === 0" class="muted">Noch keine Konten vorhanden.</p>

    <ul v-else class="account-items">
      <li v-for="account in accounts" :key="account.id">
        <button
          type="button"
          class="account-item"
          :class="{ 'account-item--selected': account.id === selectedAccountId }"
          @click="emit('select', account.id)"
        >
          <span class="account-item__number">{{ account.accountNumber }}</span>
          <span class="account-item__balance">{{ formatMoney(account.balance, account.currency) }}</span>
          <span class="account-item__meta">v{{ account.version }}</span>
        </button>
      </li>
    </ul>
  </aside>
</template>
