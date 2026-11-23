package io.ledgerstream.application.port.out;

import java.util.Objects;
import java.util.UUID;

public record PendingOutboxCandidate(UUID eventId, UUID aggregateId) {

    public PendingOutboxCandidate {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(aggregateId, "aggregateId must not be null");
    }
}
