<script setup lang="ts">
import type { BookingFormController } from '../composables/useBookingForm';
import type { AccountDto, BookingDto } from '../types/ledger';
import { formatMoney } from '../utils/money';
import BookingForm from './BookingForm.vue';

defineProps<{
  readonly account: AccountDto | null;
  readonly bookings: readonly BookingDto[];
  readonly canWrite: boolean;
  readonly isLoadingBookings: boolean;
  readonly bookingForm: BookingFormController;
}>();

const emit = defineEmits<{
  (e: 'cancel-booking', bookingId: string): void;
  (e: 'relogin'): void;
}>();
</script>

<template>
  <section v-if="account === null" class="panel panel--empty">
    <p class="muted">Bitte links ein Konto auswählen, um Kontostand, Buchungshistorie und Buchungsformular anzuzeigen.</p>
  </section>

  <div v-else class="account-detail">
    <section class="panel account-header">
      <div>
        <span class="muted">Konto</span>
        <h2>{{ account.accountNumber }}</h2>
        <code class="account-header__id">{{ account.id }}</code>
      </div>
      <div class="account-header__balance">
        <span class="muted">Aktueller Saldo (v{{ account.version }})</span>
        <strong class="balance-amount">{{ formatMoney(account.balance, account.currency) }}</strong>
      </div>
    </section>

    <BookingForm
      v-if="canWrite"
      :account-id="account.id"
      :currency="account.currency"
      :form="bookingForm"
      @relogin="emit('relogin')"
    />

    <section class="panel">
      <div class="panel__header">
        <h3>Buchungshistorie ({{ bookings.length }})</h3>
      </div>

      <p v-if="isLoadingBookings && bookings.length === 0" class="muted">Buchungen werden geladen…</p>
      <p v-else-if="bookings.length === 0" class="muted">Für dieses Konto liegen noch keine Buchungen vor.</p>

      <table v-else class="data-table">
        <thead>
          <tr>
            <th>Zeitpunkt</th>
            <th>Typ</th>
            <th>Betrag</th>
            <th>Verwendungszweck</th>
            <th>Status</th>
            <th>Idempotency-Key</th>
            <th v-if="canWrite">Aktion</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="booking in bookings" :key="booking.id">
            <td>{{ new Date(booking.createdAt).toLocaleString('de-DE') }}</td>
            <td>
              <span class="badge" :class="booking.type === 'CREDIT' ? 'badge--ok' : 'badge--debit'">
                {{ booking.type }}
              </span>
            </td>
            <td class="numeric">{{ formatMoney(booking.amount, booking.currency) }}</td>
            <td>{{ booking.reference }}</td>
            <td>{{ booking.status }}</td>
            <td><code class="key-chip">{{ booking.idempotencyKey }}</code></td>
            <td v-if="canWrite">
              <button
                v-if="booking.status === 'POSTED' && booking.originalBookingId === null"
                type="button"
                class="btn btn--secondary btn--xs"
                @click="emit('cancel-booking', booking.id)"
              >
                Stornieren
              </button>
              <span v-else class="muted">—</span>
            </td>
          </tr>
        </tbody>
      </table>
    </section>
  </div>
</template>
