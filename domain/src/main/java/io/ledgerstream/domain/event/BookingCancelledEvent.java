package io.ledgerstream.domain.event;

import io.ledgerstream.domain.model.Booking;
import io.ledgerstream.domain.model.BookingType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record BookingCancelledEvent(
        UUID eventId,
        UUID aggregateId,
        UUID originalBookingId,
        UUID counterBookingId,
        BookingType counterBookingType,
        BigDecimal amount,
        String currency,
        String reference,
        long version,
        Instant occurredAt,
        int schemaVersion
) implements DomainEvent {

    public static final String EVENT_TYPE = "BookingCancelled";
    public static final int CURRENT_SCHEMA_VERSION = 1;

    public BookingCancelledEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(aggregateId, "aggregateId must not be null");
        Objects.requireNonNull(originalBookingId, "originalBookingId must not be null");
        Objects.requireNonNull(counterBookingId, "counterBookingId must not be null");
        Objects.requireNonNull(counterBookingType, "counterBookingType must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(reference, "reference must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    }

    public static BookingCancelledEvent of(
            final UUID eventId,
            final Booking cancelledOriginal,
            final Booking counterBooking,
            final long aggregateVersion,
            final Instant occurredAt
    ) {
        return new BookingCancelledEvent(
                eventId,
                counterBooking.accountId().value(),
                cancelledOriginal.id().value(),
                counterBooking.id().value(),
                counterBooking.type(),
                counterBooking.amount().amount(),
                counterBooking.amount().currency().value(),
                counterBooking.reference().value(),
                aggregateVersion,
                occurredAt,
                CURRENT_SCHEMA_VERSION
        );
    }

    @Override
    public String eventType() {
        return EVENT_TYPE;
    }
}
