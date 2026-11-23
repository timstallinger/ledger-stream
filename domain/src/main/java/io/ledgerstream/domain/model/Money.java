package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.CurrencyMismatchException;
import io.ledgerstream.domain.exception.InvalidDomainValueException;

import java.math.BigDecimal;
import java.math.RoundingMode;

public record Money(BigDecimal amount, CurrencyCode currency) {

    public static final int SCALE = 2;

    public Money {
        if (amount == null) {
            throw new InvalidDomainValueException("Money amount must not be null");
        }
        if (currency == null) {
            throw new InvalidDomainValueException("Money currency must not be null");
        }
        BigDecimal normalized = amount.stripTrailingZeros();
        if (normalized.scale() > SCALE) {
            throw new InvalidDomainValueException(
                    "Money amount must have at most " + SCALE + " decimal places: " + amount
            );
        }
        amount = amount.setScale(SCALE, RoundingMode.UNNECESSARY);
    }

    public static Money zero(final CurrencyCode currency) {
        return new Money(BigDecimal.ZERO, currency);
    }

    public static Money of(final BigDecimal amount, final CurrencyCode currency) {
        return new Money(amount, currency);
    }

    public boolean isPositive() {
        return amount.compareTo(BigDecimal.ZERO) > 0;
    }

    public boolean isNegative() {
        return amount.compareTo(BigDecimal.ZERO) < 0;
    }

    public Money add(final Money other) {
        requireMatchingCurrency(other);
        return new Money(this.amount.add(other.amount), this.currency);
    }

    public Money subtract(final Money other) {
        requireMatchingCurrency(other);
        return new Money(this.amount.subtract(other.amount), this.currency);
    }

    private void requireMatchingCurrency(final Money other) {
        if (other == null) {
            throw new InvalidDomainValueException("Operand money must not be null");
        }
        if (!this.currency.equals(other.currency)) {
            throw new CurrencyMismatchException(this.currency, other.currency);
        }
    }
}
