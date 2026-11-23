package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.InvalidDomainValueException;

public record Reference(String value) {

    public static final int MAX_LENGTH = 255;

    public Reference {
        if (value == null || value.isBlank()) {
            throw new InvalidDomainValueException("Reference must not be blank");
        }
        value = value.trim();
        if (value.length() > MAX_LENGTH) {
            throw new InvalidDomainValueException(
                    "Reference length must not exceed " + MAX_LENGTH + " characters"
            );
        }
    }

    public static Reference of(final String value) {
        return new Reference(value);
    }
}
