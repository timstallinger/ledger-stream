package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;
import io.ledgerstream.domain.model.BookingId;

public final class BookingAlreadyCancelledException extends DomainException {

    public BookingAlreadyCancelledException(final BookingId bookingId) {
        super("BOOKING_ALREADY_CANCELLED", "Booking is already cancelled: " + bookingId.value());
    }
}
