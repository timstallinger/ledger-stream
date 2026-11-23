package io.ledgerstream.application.port.in;

import java.util.Objects;

public final class RetryableEventException extends RuntimeException {

    public RetryableEventException(final String message, final Throwable cause) {
        super(Objects.requireNonNull(message, "message must not be null"), cause);
    }

    public RetryableEventException(final String message) {
        super(Objects.requireNonNull(message, "message must not be null"));
    }
}
