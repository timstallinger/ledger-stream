package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;
import io.ledgerstream.domain.model.CurrencyCode;

public final class CurrencyMismatchException extends DomainException {

    public CurrencyMismatchException(final CurrencyCode expected, final CurrencyCode actual) {
        super("CURRENCY_MISMATCH", "Currency mismatch: expected " + expected.value() + " but got " + actual.value());
    }
}
