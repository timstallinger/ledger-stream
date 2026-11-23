package io.ledgerstream.domain;

import java.util.Objects;

public class DomainException extends RuntimeException {

    private final String errorCode;

    public DomainException(final String errorCode, final String message) {
        super(Objects.requireNonNull(message, "message must not be null"));
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode must not be null");
    }

    public final String errorCode() {
        return errorCode;
    }
}
