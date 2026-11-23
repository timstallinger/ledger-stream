package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.InsufficientBalanceException;
import io.ledgerstream.domain.exception.InvalidDomainValueException;

import java.time.Instant;
import java.util.Objects;

public record Account(
        AccountId id,
        AccountNumber accountNumber,
        Money balance,
        long version,
        Instant createdAt,
        Instant updatedAt
) {

    public Account {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(accountNumber, "accountNumber must not be null");
        Objects.requireNonNull(balance, "balance must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (version < 0) {
            throw new InvalidDomainValueException("Account version must not be negative");
        }
    }

    public static Account open(
            final AccountId id,
            final AccountNumber accountNumber,
            final CurrencyCode currency,
            final Instant now
    ) {
        return new Account(id, accountNumber, Money.zero(currency), 0L, now, now);
    }

    public Account apply(final Booking booking, final Instant now) {
        Objects.requireNonNull(booking, "booking must not be null");
        Objects.requireNonNull(now, "now must not be null");
        if (!this.id.equals(booking.accountId())) {
            throw new InvalidDomainValueException(
                    "Booking account " + booking.accountId().value()
                            + " does not match target account " + this.id.value()
            );
        }

        Money updatedBalance = switch (booking.type()) {
            case CREDIT -> this.balance.add(booking.amount());
            case DEBIT -> {
                Money candidate = this.balance.subtract(booking.amount());
                if (candidate.isNegative()) {
                    throw new InsufficientBalanceException(this.id, this.balance, booking.amount());
                }
                yield candidate;
            }
        };

        return new Account(
                this.id,
                this.accountNumber,
                updatedBalance,
                this.version + 1L,
                this.createdAt,
                now
        );
    }
}
