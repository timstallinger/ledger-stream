package io.ledgerstream.application.port.out;

import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.AccountNumber;
import io.ledgerstream.domain.model.CurrencyCode;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record AccountSummary(
        AccountId accountId,
        AccountNumber accountNumber,
        BigDecimal balance,
        CurrencyCode currency,
        long bookingCount,
        BigDecimal creditTotal,
        BigDecimal debitTotal,
        UUID lastEventId,
        Instant lastEventAt,
        Instant updatedAt
) {

    public AccountSummary {
        Objects.requireNonNull(accountId, "accountId must not be null");
        Objects.requireNonNull(accountNumber, "accountNumber must not be null");
        Objects.requireNonNull(balance, "balance must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(creditTotal, "creditTotal must not be null");
        Objects.requireNonNull(debitTotal, "debitTotal must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }
}
