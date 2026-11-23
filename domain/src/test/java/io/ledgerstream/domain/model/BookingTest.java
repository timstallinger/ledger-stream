package io.ledgerstream.domain.model;

import io.ledgerstream.domain.event.BookingCancelledEvent;
import io.ledgerstream.domain.event.BookingCreatedEvent;
import io.ledgerstream.domain.exception.BookingAlreadyCancelledException;
import io.ledgerstream.domain.exception.InvalidDomainValueException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class BookingTest {

    private static final CurrencyCode EUR = CurrencyCode.of("EUR");
    private static final Instant T0 = Instant.parse("2026-01-01T10:00:00Z");
    private static final Instant T1 = Instant.parse("2026-01-01T10:05:00Z");

    @Test
    void rejectsZeroOrNegativeBookingAmount() {
        AccountId accountId = AccountId.generate();

        assertThatThrownBy(() -> Booking.post(
                BookingId.generate(),
                accountId,
                BookingType.CREDIT,
                Money.zero(EUR),
                Reference.of("Zero"),
                IdempotencyKey.of("idem-zero"),
                T0
        )).isInstanceOf(InvalidDomainValueException.class);

        assertThatThrownBy(() -> Booking.post(
                BookingId.generate(),
                accountId,
                BookingType.DEBIT,
                Money.of(new BigDecimal("-5.00"), EUR),
                Reference.of("Negative"),
                IdempotencyKey.of("idem-neg"),
                T0
        )).isInstanceOf(InvalidDomainValueException.class);
    }

    @Test
    void enforcesIdempotencyKeyLengthLimit() {
        String tooLong = "k".repeat(129);
        assertThatThrownBy(() -> IdempotencyKey.of(tooLong))
                .isInstanceOf(InvalidDomainValueException.class);
        assertThatThrownBy(() -> IdempotencyKey.of("   "))
                .isInstanceOf(InvalidDomainValueException.class);
        assertThat(IdempotencyKey.of("k".repeat(128)).value()).hasSize(128);
    }

    @Test
    void cancellationMarksOriginalCancelledAndProducesCounterBookingWithoutDeleting() {
        AccountId accountId = AccountId.generate();
        Booking original = Booking.post(
                BookingId.generate(),
                accountId,
                BookingType.CREDIT,
                Money.of(new BigDecimal("120.00"), EUR),
                Reference.of("Invoice 42"),
                IdempotencyKey.of("idem-orig"),
                T0
        );

        Booking.CancellationPair pair = original.cancel(
                BookingId.generate(),
                IdempotencyKey.of("idem-cancel"),
                Reference.of("Reversal invoice 42"),
                T1
        );

        assertThat(pair.cancelledOriginal().id()).isEqualTo(original.id());
        assertThat(pair.cancelledOriginal().status()).isEqualTo(BookingStatus.CANCELLED);
        assertThat(pair.cancelledOriginal().amount()).isEqualTo(original.amount());

        assertThat(pair.counterBooking().type()).isEqualTo(BookingType.DEBIT);
        assertThat(pair.counterBooking().amount()).isEqualTo(original.amount());
        assertThat(pair.counterBooking().status()).isEqualTo(BookingStatus.POSTED);
        assertThat(pair.counterBooking().originalBookingId()).isEqualTo(original.id());
        assertThat(pair.counterBooking().idempotencyKey()).isEqualTo(IdempotencyKey.of("idem-cancel"));

        assertThatThrownBy(() -> pair.cancelledOriginal().cancel(
                BookingId.generate(),
                IdempotencyKey.of("idem-cancel-2"),
                Reference.of("Second cancel"),
                T1
        )).isInstanceOf(BookingAlreadyCancelledException.class);
    }

    @Test
    void createsDomainEventsWithRequiredMetadataFields() {
        AccountId accountId = AccountId.generate();
        Booking booking = Booking.post(
                BookingId.generate(),
                accountId,
                BookingType.CREDIT,
                Money.of(new BigDecimal("75.00"), EUR),
                Reference.of("Top-up"),
                IdempotencyKey.of("idem-ev"),
                T0
        );

        BookingCreatedEvent created = BookingCreatedEvent.of(UUID.randomUUID(), booking, 1L, T0);
        assertThat(created.eventId()).isNotNull();
        assertThat(created.aggregateId()).isEqualTo(accountId.value());
        assertThat(created.version()).isEqualTo(1L);
        assertThat(created.occurredAt()).isEqualTo(T0);
        assertThat(created.schemaVersion()).isEqualTo(2);
        assertThat(created.eventType()).isEqualTo("BookingCreated");

        Booking.CancellationPair pair = booking.cancel(
                BookingId.generate(),
                IdempotencyKey.of("idem-ev-cancel"),
                Reference.of("Storno"),
                T1
        );
        BookingCancelledEvent cancelled = BookingCancelledEvent.of(
                UUID.randomUUID(),
                pair.cancelledOriginal(),
                pair.counterBooking(),
                2L,
                T1
        );
        assertThat(cancelled.eventId()).isNotNull();
        assertThat(cancelled.aggregateId()).isEqualTo(accountId.value());
        assertThat(cancelled.version()).isEqualTo(2L);
        assertThat(cancelled.occurredAt()).isEqualTo(T1);
        assertThat(cancelled.schemaVersion()).isEqualTo(2);
        assertThat(cancelled.eventType()).isEqualTo("BookingCancelled");
    }
}
