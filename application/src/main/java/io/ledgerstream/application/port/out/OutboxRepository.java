package io.ledgerstream.application.port.out;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OutboxRepository {

    void save(OutboxEvent event);

    List<UUID> findPendingIds(int limit);

    Optional<OutboxEvent> lockPendingById(UUID eventId);

    void markSent(UUID eventId, Instant sentAt);

    void recordFailure(UUID eventId, String errorMessage);

    long countPending();
}
