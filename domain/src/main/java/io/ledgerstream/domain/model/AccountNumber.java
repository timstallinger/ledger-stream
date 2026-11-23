package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.InvalidDomainValueException;

import java.util.regex.Pattern;

public record AccountNumber(String value) {

    private static final int MIN_LENGTH = 3;
    private static final int MAX_LENGTH = 64;
    private static final Pattern ALLOWED_PATTERN = Pattern.compile("^[A-Za-z0-9\\-_]+$");

    public AccountNumber {
        if (value == null || value.isBlank()) {
            throw new InvalidDomainValueException("AccountNumber must not be blank");
        }
        value = value.trim();
        if (value.length() < MIN_LENGTH || value.length() > MAX_LENGTH) {
            throw new InvalidDomainValueException(
                    "AccountNumber length must be between " + MIN_LENGTH + " and " + MAX_LENGTH
            );
        }
        if (!ALLOWED_PATTERN.matcher(value).matches()) {
            throw new InvalidDomainValueException(
                    "AccountNumber may only contain letters, digits, '-' and '_'"
            );
        }
    }

    public static AccountNumber of(final String value) {
        return new AccountNumber(value);
    }
}
