package io.ledgerstream.application.port.in;

import java.util.Objects;

public record IdempotentExecutionResult(
        StoredIdempotentResponse response,
        boolean replayed
) {

    public IdempotentExecutionResult {
        Objects.requireNonNull(response, "response must not be null");
    }
}
