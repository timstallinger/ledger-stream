<script setup lang="ts">
import type { ApiErrorState } from '../types/ledger';

defineProps<{
  readonly error: ApiErrorState | null;
}>();

const emit = defineEmits<{
  relogin: [];
  retry: [];
}>();
</script>

<template>
  <section
    v-if="error !== null"
    class="error-banner"
    :class="`error-banner--${error.kind}`"
    role="alert"
    aria-live="assertive"
  >
    <template v-if="error.kind === 'unauthorized'">
      <div class="error-banner__header">
        <strong>401 — Sitzung abgelaufen</strong>
        <button type="button" class="btn btn--primary btn--sm" @click="emit('relogin')">
          Erneut anmelden
        </button>
      </div>
      <p>{{ error.message }}</p>
    </template>

    <template v-else-if="error.kind === 'forbidden'">
      <div class="error-banner__header">
        <strong>403 — Keine Berechtigung</strong>
      </div>
      <p>{{ error.message }}</p>
    </template>

    <template v-else-if="error.kind === 'validation'">
      <div class="error-banner__header">
        <strong>{{ error.status }} — Validierungs-/Regelfehler ({{ error.errorCode }})</strong>
      </div>
      <p>{{ error.message }}</p>
      <ul v-if="error.fieldErrors.length > 0" class="error-banner__fields">
        <li v-for="fieldError in error.fieldErrors" :key="`${fieldError.field}-${fieldError.message}`">
          <code>{{ fieldError.field }}</code>: {{ fieldError.message }}
        </li>
      </ul>
    </template>

    <template v-else-if="error.kind === 'conflict'">
      <div class="error-banner__header">
        <strong>409 — Bereits gebucht, gleiche Antwort</strong>
      </div>
      <p>{{ error.message }}</p>
    </template>

    <template v-else-if="error.kind === 'network'">
      <div class="error-banner__header">
        <strong>Netzwerk- oder Serverfehler</strong>
        <button type="button" class="btn btn--secondary btn--sm" @click="emit('retry')">
          Erneut versuchen
        </button>
      </div>
      <p>{{ error.message }}</p>
    </template>
  </section>
</template>
