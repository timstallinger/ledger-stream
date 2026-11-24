<script setup lang="ts">
import { computed, ref } from 'vue';
import type { BookingFormController } from '../composables/useBookingForm';
import { formatMoney } from '../utils/money';
import ErrorBanner from './ErrorBanner.vue';

const props = defineProps<{
  readonly accountId: string;
  readonly currency: string;
  readonly form: BookingFormController;
}>();

const emit = defineEmits<{
  (e: 'relogin'): void;
}>();

const copiedFeedback = ref<boolean>(false);
const isBusy = computed<boolean>(() => props.form.inFlightCount.value > 0);

const copyKey = async (): Promise<void> => {
  try {
    await navigator.clipboard.writeText(props.form.idempotencyKey.value);
    copiedFeedback.value = true;
    window.setTimeout(() => {
      copiedFeedback.value = false;
    }, 1500);
  } catch {
    copiedFeedback.value = false;
  }
};

const handleSubmit = async (): Promise<void> => {
  await props.form.submitBooking(props.accountId, props.currency);
};

const handleResend = async (): Promise<void> => {
  await props.form.resendWithSameKey(props.accountId, props.currency);
};

const handleIntentionalDuplicate = async (): Promise<void> => {
  await props.form.submitIntentionalDuplicate(props.accountId, props.currency);
};

const handleTypeChange = (event: Event): void => {
  if (event.target instanceof HTMLSelectElement) {
    const selected = event.target.value;
    if (selected === 'CREDIT' || selected === 'DEBIT') {
      props.form.setBookingType(selected);
    }
  }
};

const handleAmountInput = (event: Event): void => {
  if (event.target instanceof HTMLInputElement) {
    props.form.setAmount(event.target.value);
  }
};

const handleReferenceInput = (event: Event): void => {
  if (event.target instanceof HTMLInputElement) {
    props.form.setReference(event.target.value);
  }
};
</script>

<template>
  <section class="panel booking-form">
    <div class="panel__header">
      <h3>Neue Buchung</h3>
      <button
        v-if="!form.isOpen.value"
        type="button"
        class="btn btn--primary"
        data-testid="open-booking-form-btn"
        @click="form.openForm()"
      >
        Buchungsformular öffnen
      </button>
      <button v-else type="button" class="btn btn--secondary btn--sm" @click="form.closeForm()">
        Schließen
      </button>
    </div>

    <div v-if="form.isOpen.value" class="booking-form__body">
      <div class="idem-box">
        <div class="idem-box__label">
          <span>Aktiver <code>Idempotency-Key</code> (entsteht beim Öffnen, rotiert erst nach 2xx-Erfolg)</span>
          <button type="button" class="btn btn--secondary btn--xs" @click="copyKey">
            {{ copiedFeedback ? 'Kopiert!' : 'Kopieren' }}
          </button>
        </div>
        <code class="idem-box__value" data-testid="idempotency-key-display">{{ form.idempotencyKey.value }}</code>
      </div>

      <ErrorBanner
        :error="form.errorState.value"
        @relogin="emit('relogin')"
        @retry="handleResend"
      />

      <form class="form-grid" @submit.prevent="handleSubmit">
        <label class="field">
          <span>Typ</span>
          <select
            :value="form.bookingType.value"
            data-testid="booking-type-select"
            @change="handleTypeChange"
          >
            <option value="CREDIT">CREDIT (Einzahlung)</option>
            <option value="DEBIT">DEBIT (Auszahlung)</option>
          </select>
        </label>

        <label class="field">
          <span>Betrag ({{ currency }}, als Dezimal-String)</span>
          <input
            :value="form.amount.value"
            type="text"
            inputmode="decimal"
            placeholder="25.00"
            required
            data-testid="booking-amount-input"
            @input="handleAmountInput"
          >
        </label>

        <label class="field field--full">
          <span>Verwendungszweck</span>
          <input
            :value="form.reference.value"
            type="text"
            maxlength="255"
            required
            data-testid="booking-reference-input"
            @input="handleReferenceInput"
          >
        </label>

        <div class="form-actions">
          <!-- Submit-Button wird bewusst NICHT deaktiviert: Spinner + aria-busy statt disabled -->
          <button
            type="submit"
            class="btn btn--primary"
            :aria-busy="isBusy"
            data-testid="submit-booking-btn"
          >
            <span v-if="isBusy" class="spinner" aria-hidden="true" />
            <span>{{ isBusy ? 'Buche… (Doppelklick sicher)' : 'Buchen' }}</span>
          </button>

          <button
            type="button"
            class="btn btn--secondary"
            :aria-busy="isBusy"
            data-testid="resend-same-key-btn"
            @click="handleResend"
          >
            Erneut senden (gleicher Key)
          </button>

          <button
            type="button"
            class="btn btn--warning"
            :aria-busy="isBusy"
            data-testid="duplicate-send-btn"
            @click="handleIntentionalDuplicate"
          >
            Absichtlich doppelt senden (2× parallel)
          </button>
        </div>
      </form>

      <div v-if="form.lastResult.value !== null" class="result-card" data-testid="booking-result">
        <strong>Letzte Antwort (HTTP {{ form.lastResult.value.status }}):</strong>
        <span>
          Buchung <code>{{ form.lastResult.value.booking.id }}</code> —
          {{ formatMoney(form.lastResult.value.booking.amount, form.lastResult.value.booking.currency) }}
          ({{ form.lastResult.value.booking.type }})
        </span>
        <span class="badge" :class="form.lastResult.value.replayed ? 'badge--warn' : 'badge--ok'">
          {{ form.lastResult.value.replayed ? 'Idempotency-Replayed: true' : 'Neu verbucht' }}
        </span>
      </div>

      <div
        v-if="form.duplicateProtocol.value !== null"
        class="protocol-card"
        data-testid="duplicate-protocol"
      >
        <h4>Doppelklick-Protokoll (2 parallele Requests, gleicher Key)</h4>
        <p class="protocol-card__meta">
          Key: <code>{{ form.duplicateProtocol.value.idempotencyKey }}</code> ·
          Identische Buchungs-ID:
          <strong>{{ form.duplicateProtocol.value.sameBookingId ? 'JA (1 Buchung)' : 'NEIN' }}</strong> ·
          Byte-gleicher Response-Body:
          <strong>{{ form.duplicateProtocol.value.bodiesByteIdentical ? 'JA' : 'NEIN' }}</strong>
        </p>
        <table class="data-table">
          <thead>
            <tr>
              <th>Request</th>
              <th>HTTP</th>
              <th>Idempotency-Replayed</th>
              <th>Booking-ID</th>
              <th>Betrag</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>#1</td>
              <td>{{ form.duplicateProtocol.value.first.status }}</td>
              <td>{{ form.duplicateProtocol.value.first.replayed ? 'true' : 'false' }}</td>
              <td><code>{{ form.duplicateProtocol.value.first.booking.id }}</code></td>
              <td>
                {{
                  formatMoney(
                    form.duplicateProtocol.value.first.booking.amount,
                    form.duplicateProtocol.value.first.booking.currency
                  )
                }}
              </td>
            </tr>
            <tr>
              <td>#2</td>
              <td>{{ form.duplicateProtocol.value.second.status }}</td>
              <td>{{ form.duplicateProtocol.value.second.replayed ? 'true' : 'false' }}</td>
              <td><code>{{ form.duplicateProtocol.value.second.booking.id }}</code></td>
              <td>
                {{
                  formatMoney(
                    form.duplicateProtocol.value.second.booking.amount,
                    form.duplicateProtocol.value.second.booking.currency
                  )
                }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>
</template>
