package io.ledgerstream.application.port.out;

import io.ledgerstream.application.port.in.StoredIdempotentResponse;

import java.util.Objects;

public record IdempotencyRecord(
        String requestFingerprint,
        StoredIdempotentResponse response
) {

    public IdempotencyRecord {
        Objects.requireNonNull(requestFingerprint, "requestFingerprint must not be null");
        Objects.requireNonNull(response, "response must not be null");
    }
}
