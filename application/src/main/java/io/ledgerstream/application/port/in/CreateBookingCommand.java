package io.ledgerstream.application.port.in;

import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.BookingType;
import io.ledgerstream.domain.model.IdempotencyKey;
import io.ledgerstream.domain.model.Money;
import io.ledgerstream.domain.model.Reference;

import java.util.Objects;

public record CreateBookingCommand(
        AccountId accountId,
        BookingType type,
        Money amount,
        Reference reference,
        IdempotencyKey idempotencyKey
) {

    public CreateBookingCommand {
        Objects.requireNonNull(accountId, "accountId must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(reference, "reference must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
    }
}
