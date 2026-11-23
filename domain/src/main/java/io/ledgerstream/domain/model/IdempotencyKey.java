package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.InvalidDomainValueException;

public record IdempotencyKey(String value) {

    public static final int MAX_LENGTH = 128;

    public IdempotencyKey {
        if (value == null || value.isBlank()) {
            throw new InvalidDomainValueException("IdempotencyKey must not be blank");
        }
        value = value.trim();
        if (value.length() > MAX_LENGTH) {
            throw new InvalidDomainValueException(
                    "IdempotencyKey length must not exceed " + MAX_LENGTH + " characters"
            );
        }
    }

    public static IdempotencyKey of(final String value) {
        return new IdempotencyKey(value);
    }
}
