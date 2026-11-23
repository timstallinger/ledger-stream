package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.InvalidDomainValueException;

import java.util.Currency;
import java.util.Locale;

public record CurrencyCode(String value) {

    public CurrencyCode {
        if (value == null || value.isBlank()) {
            throw new InvalidDomainValueException("CurrencyCode must not be blank");
        }
        value = value.trim().toUpperCase(Locale.ROOT);
        try {
            Currency.getInstance(value);
        } catch (IllegalArgumentException exception) {
            throw new InvalidDomainValueException("Unsupported ISO-4217 currency code: " + value);
        }
    }

    public static CurrencyCode of(final String value) {
        return new CurrencyCode(value);
    }
}
