package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.InvalidDomainValueException;

import java.util.UUID;

public record AccountId(UUID value) {

    public AccountId {
        if (value == null) {
            throw new InvalidDomainValueException("AccountId must not be null");
        }
    }

    public static AccountId generate() {
        return new AccountId(UUID.randomUUID());
    }

    public static AccountId of(final UUID value) {
        return new AccountId(value);
    }
}
