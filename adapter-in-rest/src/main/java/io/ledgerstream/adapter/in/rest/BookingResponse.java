package io.ledgerstream.adapter.in.rest;

import io.ledgerstream.domain.model.Booking;
import io.ledgerstream.domain.model.BookingStatus;
import io.ledgerstream.domain.model.BookingType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record BookingResponse(
        UUID id,
        UUID accountId,
        BookingType type,
        BigDecimal amount,
        String currency,
        String reference,
        BookingStatus status,
        String idempotencyKey,
        UUID originalBookingId,
        Instant createdAt
) {

    public static BookingResponse from(final Booking booking) {
        return new BookingResponse(
                booking.id().value(),
                booking.accountId().value(),
                booking.type(),
                booking.amount().amount(),
                booking.amount().currency().value(),
                booking.reference().value(),
                booking.status(),
                booking.idempotencyKey().value(),
                booking.originalBookingId() != null ? booking.originalBookingId().value() : null,
                booking.createdAt()
        );
    }
}
