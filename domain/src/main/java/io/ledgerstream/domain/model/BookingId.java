package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.InvalidDomainValueException;

import java.util.UUID;

public record BookingId(UUID value) {

    public BookingId {
        if (value == null) {
            throw new InvalidDomainValueException("BookingId must not be null");
        }
    }

    public static BookingId generate() {
        return new BookingId(UUID.randomUUID());
    }

    public static BookingId of(final UUID value) {
        return new BookingId(value);
    }
}
