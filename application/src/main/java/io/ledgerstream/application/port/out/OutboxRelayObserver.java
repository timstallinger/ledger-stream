package io.ledgerstream.application.port.out;

import java.util.UUID;

public interface OutboxRelayObserver {

    default void onEventRelayed(final String eventType, final boolean success) {
        // Optional hook for relay throughput/outcome metrics
    }

    void onEventFailedPermanently(UUID eventId, UUID aggregateId, int retryCount, String errorMessage);
}
