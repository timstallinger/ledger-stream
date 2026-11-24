package io.ledgerstream.adapter.in.rest;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.ledgerstream.domain.model.Account;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record AccountResponse(
        UUID id,
        String accountNumber,
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        BigDecimal balance,
        String currency,
        long version,
        Instant createdAt,
        Instant updatedAt
) {

    public static AccountResponse from(final Account account) {
        return new AccountResponse(
                account.id().value(),
                account.accountNumber().value(),
                account.balance().amount(),
                account.balance().currency().value(),
                account.version(),
                account.createdAt(),
                account.updatedAt()
        );
    }
}
