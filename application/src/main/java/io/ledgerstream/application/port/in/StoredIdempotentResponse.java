package io.ledgerstream.application.port.in;

import java.util.Objects;

public record StoredIdempotentResponse(
        int statusCode,
        String responseBody
) {

    public StoredIdempotentResponse {
        if (statusCode < 200 || statusCode > 299) {
            throw new IllegalArgumentException("Only 2xx responses may be cached: " + statusCode);
        }
        Objects.requireNonNull(responseBody, "responseBody must not be null");
    }
}
