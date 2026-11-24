import { ref, type Ref } from 'vue';
import { LedgerApiError, type LedgerApiClient } from '../api/ledgerClient';
import type {
  ApiErrorState,
  BookingResponseEnvelope,
  BookingType,
  CreateBookingPayload,
  DuplicateSendProtocol
} from '../types/ledger';
import { normalizeDecimalInput } from '../utils/money';

export interface BookingFormController {
  readonly isOpen: Ref<boolean>;
  readonly bookingType: Ref<BookingType>;
  readonly amount: Ref<string>;
  readonly reference: Ref<string>;
  readonly idempotencyKey: Ref<string>;
  readonly lastSubmittedKey: Ref<string | null>;
  readonly inFlightCount: Ref<number>;
  readonly errorState: Ref<ApiErrorState | null>;
  readonly lastResult: Ref<BookingResponseEnvelope | null>;
  readonly duplicateProtocol: Ref<DuplicateSendProtocol | null>;
  openForm(): void;
  closeForm(): void;
  setBookingType(next: BookingType): void;
  setAmount(next: string): void;
  setReference(next: string): void;
  submitBooking(accountId: string, currency: string): Promise<BookingResponseEnvelope | null>;
  resendWithSameKey(accountId: string, currency: string): Promise<BookingResponseEnvelope | null>;
  submitIntentionalDuplicate(accountId: string, currency: string): Promise<DuplicateSendProtocol | null>;
}

/**
 * Manages the booking form and its Idempotency-Key lifecycle:
 *  - Key is generated ONCE when the form opens (crypto.randomUUID()).
 *  - Key is NEVER generated inside submitBooking() — concurrent double-clicks share the same key.
 *  - Key is rotated ONLY after a confirmed 2xx booking succeeds.
 *  - Resend preserves the exact same Idempotency-Key (both after errors and for replay demos).
 */
export function useBookingForm(
  apiClient: LedgerApiClient,
  onBookingCommitted: () => Promise<void>,
  onUnauthorized: () => void,
  generateKey: () => string = () => crypto.randomUUID()
): BookingFormController {
  const isOpen = ref<boolean>(false);
  const bookingType = ref<BookingType>('CREDIT');
  const amount = ref<string>('25.00');
  const reference = ref<string>('Überweisung');
  const idempotencyKey = ref<string>(generateKey());
  const lastSubmittedKey = ref<string | null>(null);
  const lastSubmittedPayload = ref<CreateBookingPayload | null>(null);
  const inFlightCount = ref<number>(0);
  const errorState = ref<ApiErrorState | null>(null);
  const lastResult = ref<BookingResponseEnvelope | null>(null);
  const duplicateProtocol = ref<DuplicateSendProtocol | null>(null);

  const rotateKeyAfterSuccess = (): void => {
    idempotencyKey.value = generateKey();
  };

  const openForm = (): void => {
    isOpen.value = true;
    errorState.value = null;
    duplicateProtocol.value = null;
    idempotencyKey.value = generateKey();
  };

  const closeForm = (): void => {
    isOpen.value = false;
  };

  const setBookingType = (next: BookingType): void => {
    bookingType.value = next;
  };

  const setAmount = (next: string): void => {
    amount.value = next;
  };

  const setReference = (next: string): void => {
    reference.value = next;
  };

  const buildPayload = (currency: string): CreateBookingPayload => ({
    type: bookingType.value,
    amount: normalizeDecimalInput(amount.value),
    currency,
    reference: reference.value.trim()
  });

  const handleApiError = (error: unknown): void => {
    if (error instanceof LedgerApiError) {
      errorState.value = error.state;
      if (error.state.kind === 'unauthorized') {
        onUnauthorized();
      }
      return;
    }
    errorState.value = {
      kind: 'network',
      status: null,
      message: 'Unbekannter Netzwerkfehler bei der Buchung. Bitte erneut versuchen.'
    };
  };

  const submitBooking = async (accountId: string, currency: string): Promise<BookingResponseEnvelope | null> => {
    const activeKey = idempotencyKey.value;
    const payload = buildPayload(currency);

    inFlightCount.value += 1;
    errorState.value = null;
    duplicateProtocol.value = null;

    try {
      const envelope = await apiClient.createBooking(accountId, payload, activeKey);
      lastResult.value = envelope;
      lastSubmittedKey.value = activeKey;
      lastSubmittedPayload.value = payload;

      if (envelope.replayed) {
        errorState.value = {
          kind: 'conflict',
          status: 409,
          errorCode: 'IDEMPOTENCY_REPLAYED',
          message: 'Bereits gebucht — identische Antwort vom Server erhalten (Idempotency-Replayed: true).'
        };
      }

      // Draw a fresh key ONLY after a confirmed 2xx booking and ONLY if no newer key replaced it
      if (idempotencyKey.value === activeKey) {
        rotateKeyAfterSuccess();
      }
      await onBookingCommitted();
      return envelope;
    } catch (error: unknown) {
      // Do NOT rotate idempotencyKey on error — retry/resend keeps the exact same key
      handleApiError(error);
      return null;
    } finally {
      inFlightCount.value = Math.max(0, inFlightCount.value - 1);
    }
  };

  const resendWithSameKey = async (accountId: string, currency: string): Promise<BookingResponseEnvelope | null> => {
    const keyToReuse = errorState.value !== null ? idempotencyKey.value : (lastSubmittedKey.value ?? idempotencyKey.value);
    const payloadToReuse =
      errorState.value !== null ? buildPayload(currency) : (lastSubmittedPayload.value ?? buildPayload(currency));

    inFlightCount.value += 1;
    errorState.value = null;
    duplicateProtocol.value = null;

    try {
      const envelope = await apiClient.createBooking(accountId, payloadToReuse, keyToReuse);
      lastResult.value = envelope;
      lastSubmittedKey.value = keyToReuse;
      lastSubmittedPayload.value = payloadToReuse;

      if (envelope.replayed) {
        errorState.value = {
          kind: 'conflict',
          status: 409,
          errorCode: 'IDEMPOTENCY_REPLAYED',
          message: 'Bereits gebucht — identische Antwort erhalten (Idempotency-Replayed: true).'
        };
      }
      await onBookingCommitted();
      return envelope;
    } catch (error: unknown) {
      handleApiError(error);
      return null;
    } finally {
      inFlightCount.value = Math.max(0, inFlightCount.value - 1);
    }
  };

  const submitIntentionalDuplicate = async (
    accountId: string,
    currency: string
  ): Promise<DuplicateSendProtocol | null> => {
    const sharedKey = idempotencyKey.value;
    const payload = buildPayload(currency);

    inFlightCount.value += 2;
    errorState.value = null;
    duplicateProtocol.value = null;

    try {
      const [first, second] = await Promise.all([
        apiClient.createBooking(accountId, payload, sharedKey),
        apiClient.createBooking(accountId, payload, sharedKey)
      ]);

      const protocol: DuplicateSendProtocol = {
        idempotencyKey: sharedKey,
        executedAt: new Date().toISOString(),
        first,
        second,
        bodiesByteIdentical: first.rawBody === second.rawBody,
        sameBookingId: first.booking.id === second.booking.id
      };

      duplicateProtocol.value = protocol;
      lastResult.value = second;
      lastSubmittedKey.value = sharedKey;
      lastSubmittedPayload.value = payload;

      if (idempotencyKey.value === sharedKey) {
        rotateKeyAfterSuccess();
      }
      await onBookingCommitted();
      return protocol;
    } catch (error: unknown) {
      handleApiError(error);
      return null;
    } finally {
      inFlightCount.value = Math.max(0, inFlightCount.value - 2);
    }
  };

  return {
    isOpen,
    bookingType,
    amount,
    reference,
    idempotencyKey,
    lastSubmittedKey,
    inFlightCount,
    errorState,
    lastResult,
    duplicateProtocol,
    openForm,
    closeForm,
    setBookingType,
    setAmount,
    setReference,
    submitBooking,
    resendWithSameKey,
    submitIntentionalDuplicate
  };
}
