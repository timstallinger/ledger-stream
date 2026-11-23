package io.ledgerstream.application.port.out;

import java.util.UUID;

public interface ConsumerEventObserver {

    void onProcessed(UUID eventId);

    void onDuplicateRejected(UUID eventId);

    void onPoisonRejected(String eventIdOrKey, String reason);
}
