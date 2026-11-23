package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;
import io.ledgerstream.domain.model.BookingId;

public final class BookingNotFoundException extends DomainException {

    public BookingNotFoundException(final BookingId bookingId) {
        super("BOOKING_NOT_FOUND", "Booking not found: " + bookingId.value());
    }
}
