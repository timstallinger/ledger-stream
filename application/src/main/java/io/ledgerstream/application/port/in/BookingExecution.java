package io.ledgerstream.application.port.in;

import io.ledgerstream.domain.event.DomainEvent;
import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.Booking;

import java.util.Objects;

public record BookingExecution(
        Booking booking,
        Account updatedAccount,
        DomainEvent event
) {

    public BookingExecution {
        Objects.requireNonNull(booking, "booking must not be null");
        Objects.requireNonNull(updatedAccount, "updatedAccount must not be null");
        Objects.requireNonNull(event, "event must not be null");
    }
}
