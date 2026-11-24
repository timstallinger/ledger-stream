import { describe, expect, it } from 'vitest';
import { LedgerApiError, type LedgerApiClient } from '../api/ledgerClient';
import type { BookingDto, BookingResponseEnvelope, CreateBookingPayload } from '../types/ledger';
import { useBookingForm } from './useBookingForm';

function createStubClient(
  onCreateBooking: (
    accountId: string,
    payload: CreateBookingPayload,
    idempotencyKey: string
  ) => Promise<BookingResponseEnvelope>
): LedgerApiClient {
  return {
    listAccounts: async () => [],
    getAccount: async () => {
      throw new Error('not used');
    },
    createAccount: async () => {
      throw new Error('not used');
    },
    listBookings: async () => [],
    createBooking: onCreateBooking,
    cancelBooking: async () => {
      throw new Error('not used');
    }
  };
}

describe('useBookingForm Idempotency-Key lifecycle & duplicate-send protocol', () => {
  it('creates key on open, preserves key on failure/resend, rotates ONLY after 2xx success', async () => {
    let keyCounter = 0;
    const keys = ['key-initial', 'key-on-open', 'key-after-success'];
    const sentKeys: string[] = [];
    let shouldFail = true;

    const client = createStubClient(async (accountId, payload, idempotencyKey) => {
      sentKeys.push(idempotencyKey);
      if (shouldFail) {
        throw new LedgerApiError({
          kind: 'validation',
          status: 422,
          errorCode: 'INSUFFICIENT_BALANCE',
          message: 'Saldo zu niedrig',
          fieldErrors: [{ field: 'amount', message: 'übersteigt Saldo' }]
        });
      }
      const booking: BookingDto = {
        id: 'booking-1',
        accountId,
        type: payload.type,
        amount: payload.amount,
        currency: payload.currency,
        reference: payload.reference,
        status: 'POSTED',
        idempotencyKey,
        originalBookingId: null,
        createdAt: '2026-11-24T08:00:00Z'
      };
      return {
        booking,
        status: 201,
        replayed: false,
        rawBody: JSON.stringify(booking),
        idempotencyKey
      };
    });

    const form = useBookingForm(
      client,
      async () => {},
      () => {},
      () => {
        const next = keys[keyCounter] ?? `key-extra-${keyCounter}`;
        keyCounter += 1;
        return next;
      }
    );

    form.openForm();
    expect(form.idempotencyKey.value).toBe('key-on-open');

    // Failed submit -> key MUST NOT rotate
    const failed = await form.submitBooking('acc-1', 'EUR');
    expect(failed).toBeNull();
    expect(form.errorState.value?.kind).toBe('validation');
    expect(form.idempotencyKey.value).toBe('key-on-open');

    // Resend after error -> sends the EXACT same key
    shouldFail = false;
    const retried = await form.resendWithSameKey('acc-1', 'EUR');
    expect(retried?.booking.id).toBe('booking-1');
    expect(sentKeys).toEqual(['key-on-open', 'key-on-open']);

    // Next fresh submit succeeds -> NOW key rotates to key-after-success
    const succeeded = await form.submitBooking('acc-1', 'EUR');
    expect(succeeded?.status).toBe(201);
    expect(form.idempotencyKey.value).toBe('key-after-success');
  });

  it('sends 2 parallel requests with the exact same key and records byte-identical responses', async () => {
    const receivedKeys: string[] = [];
    const createdBookings = new Map<string, string>();
    let callIndex = 0;

    const client = createStubClient(async (accountId, payload, idempotencyKey) => {
      receivedKeys.push(idempotencyKey);
      const currentCall = callIndex;
      callIndex += 1;
      const existingRaw = createdBookings.get(idempotencyKey);
      if (existingRaw !== undefined) {
        const parsed: BookingDto = {
          id: 'booking-single-effect',
          accountId,
          type: payload.type,
          amount: payload.amount,
          currency: payload.currency,
          reference: payload.reference,
          status: 'POSTED',
          idempotencyKey,
          originalBookingId: null,
          createdAt: '2026-11-24T08:00:00Z'
        };
        return {
          booking: parsed,
          status: 201,
          replayed: true,
          rawBody: existingRaw,
          idempotencyKey
        };
      }
      const booking: BookingDto = {
        id: 'booking-single-effect',
        accountId,
        type: payload.type,
        amount: payload.amount,
        currency: payload.currency,
        reference: payload.reference,
        status: 'POSTED',
        idempotencyKey,
        originalBookingId: null,
        createdAt: '2026-11-24T08:00:00Z'
      };
      const rawBody = JSON.stringify(booking);
      createdBookings.set(idempotencyKey, rawBody);
      return {
        booking,
        status: 201,
        replayed: currentCall > 0,
        rawBody,
        idempotencyKey
      };
    });

    let seq = 0;
    const form = useBookingForm(
      client,
      async () => {},
      () => {},
      () => `idem-dup-${++seq}`
    );

    form.openForm();
    const activeKey = form.idempotencyKey.value;
    const protocol = await form.submitIntentionalDuplicate('acc-42', 'EUR');

    expect(receivedKeys).toEqual([activeKey, activeKey]);
    expect(createdBookings.size).toBe(1);
    expect(protocol).not.toBeNull();
    expect(protocol?.bodiesByteIdentical).toBe(true);
    expect(protocol?.sameBookingId).toBe(true);
    expect(protocol?.first.rawBody).toBe(protocol?.second.rawBody);
    // Rotated only AFTER parallel duplicate requests succeeded
    expect(form.idempotencyKey.value).not.toBe(activeKey);
  });
});
