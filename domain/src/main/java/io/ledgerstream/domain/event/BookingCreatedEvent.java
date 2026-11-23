package io.ledgerstream.domain.event;

import io.ledgerstream.domain.model.Booking;
import io.ledgerstream.domain.model.BookingType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record BookingCreatedEvent(
        UUID eventId,
        UUID aggregateId,
        UUID bookingId,
        BookingType bookingType,
        BigDecimal amount,
        String currency,
        String reference,
        long version,
        Instant occurredAt,
        int schemaVersion
) implements DomainEvent {

    public static final String EVENT_TYPE = "BookingCreated";
    public static final int CURRENT_SCHEMA_VERSION = 2;

    public BookingCreatedEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(aggregateId, "aggregateId must not be null");
        Objects.requireNonNull(bookingId, "bookingId must not be null");
        Objects.requireNonNull(bookingType, "bookingType must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(reference, "reference must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    }

    public static BookingCreatedEvent of(
            final UUID eventId,
            final Booking booking,
            final long aggregateVersion,
            final Instant occurredAt
    ) {
        return new BookingCreatedEvent(
                eventId,
                booking.accountId().value(),
                booking.id().value(),
                booking.type(),
                booking.amount().amount(),
                booking.amount().currency().value(),
                booking.reference().value(),
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
