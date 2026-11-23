package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;

public final class InvalidDomainValueException extends DomainException {

    public InvalidDomainValueException(final String message) {
        super("INVALID_DOMAIN_VALUE", message);
    }
}
