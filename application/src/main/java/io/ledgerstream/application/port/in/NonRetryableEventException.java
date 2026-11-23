package io.ledgerstream.application.port.in;

import java.util.Objects;

public final class NonRetryableEventException extends RuntimeException {

    private final String reason;

    public NonRetryableEventException(final String reason, final String message) {
        super(Objects.requireNonNull(message, "message must not be null"));
        this.reason = Objects.requireNonNull(reason, "reason must not be null");
    }

    public String reason() {
        return reason;
    }
}
