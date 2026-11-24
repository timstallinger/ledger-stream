import { mount } from '@vue/test-utils';
import { describe, expect, it } from 'vitest';
import type { LedgerApiClient } from '../api/ledgerClient';
import type { BookingDto } from '../types/ledger';
import BookingForm from './BookingForm.vue';
import { useBookingForm } from '../composables/useBookingForm';

describe('BookingForm component', () => {
  it('displays visible idempotency key, keeps submit button enabled with aria-busy, and exposes duplicate-send button', async () => {
    const client: LedgerApiClient = {
      listAccounts: async () => [],
      getAccount: async () => {
        throw new Error('unused');
      },
      createAccount: async () => {
        throw new Error('unused');
      },
      listBookings: async () => [],
      createBooking: async (accountId, payload, idempotencyKey) => {
        const booking: BookingDto = {
          id: 'b-1',
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
      },
      cancelBooking: async () => {
        throw new Error('unused');
      }
    };

    const form = useBookingForm(client, async () => {}, () => {}, () => 'uuid-visible-key');
    form.openForm();

    const wrapper = mount(BookingForm, {
      props: {
        accountId: 'acc-1',
        currency: 'EUR',
        form
      }
    });

    expect(wrapper.get('[data-testid="idempotency-key-display"]').text()).toBe('uuid-visible-key');
    const submitBtn = wrapper.get('[data-testid="submit-booking-btn"]');
    expect(submitBtn.attributes('disabled')).toBeUndefined();
    expect(wrapper.find('[data-testid="duplicate-send-btn"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="resend-same-key-btn"]').exists()).toBe(true);
  });
});
