package io.ledgerstream.application.port.out;

import java.util.UUID;

public interface OutboxRelayObserver {

    void onEventFailedPermanently(UUID eventId, UUID aggregateId, int retryCount, String errorMessage);
}
