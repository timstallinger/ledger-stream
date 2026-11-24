package io.ledgerstream.application.port.out;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OutboxRepository {

    void save(OutboxEvent event);

    List<PendingOutboxCandidate> findPendingCandidates(int limit, Instant now);

    Optional<OutboxEvent> lockPendingById(UUID eventId, Instant now);

    default List<OutboxEvent> lockPendingBatchByIds(final List<UUID> eventIds, final Instant now) {
        return eventIds.stream()
                .map(id -> lockPendingById(id, now))
                .flatMap(Optional::stream)
                .toList();
    }

    void markSent(UUID eventId, Instant sentAt);

    default void markSentBatch(final List<UUID> eventIds, final Instant sentAt) {
        for (UUID eventId : eventIds) {
            markSent(eventId, sentAt);
        }
    }

    int recordRetryFailure(UUID eventId, String errorMessage, Instant nextAttemptAt, int maxAttempts);

    long countPending();

    Optional<Instant> findOldestPendingCreatedAt();
}
