package io.ledgerstream.application.port.in;

import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.BookingId;
import io.ledgerstream.domain.model.IdempotencyKey;
import io.ledgerstream.domain.model.Reference;

import java.util.Objects;

public record CancelBookingCommand(
        AccountId accountId,
        BookingId bookingId,
        Reference reference,
        IdempotencyKey idempotencyKey,
        String correlationId
) {

    public CancelBookingCommand(
            final AccountId accountId,
            final BookingId bookingId,
            final Reference reference,
            final IdempotencyKey idempotencyKey
    ) {
        this(accountId, bookingId, reference, idempotencyKey, null);
    }

    public CancelBookingCommand {
        Objects.requireNonNull(accountId, "accountId must not be null");
        Objects.requireNonNull(bookingId, "bookingId must not be null");
        Objects.requireNonNull(reference, "reference must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
    }
}
