package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;

public final class LockTimeoutException extends DomainException {

    public LockTimeoutException(final String message) {
        super("LOCK_TIMEOUT", message);
    }
}
