package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.RelayOutboxUseCase;
import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxEventPublisher;
import io.ledgerstream.application.port.out.OutboxRepository;
import io.ledgerstream.application.port.out.TransactionRunner;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class OutboxRelayService implements RelayOutboxUseCase {

    private static final int MAX_ERROR_LENGTH = 1000;

    private final OutboxRepository outboxRepository;
    private final OutboxEventPublisher eventPublisher;
    private final TransactionRunner transactionRunner;
    private final Clock clock;
    private final int batchSize;

    public OutboxRelayService(
            final OutboxRepository outboxRepository,
            final OutboxEventPublisher eventPublisher,
            final TransactionRunner transactionRunner,
            final Clock clock,
            final int batchSize
    ) {
        this.outboxRepository = Objects.requireNonNull(outboxRepository, "outboxRepository must not be null");
        this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        }
        this.batchSize = batchSize;
    }

    @Override
    public int relayPendingBatch() {
        List<UUID> pendingIds = outboxRepository.findPendingIds(batchSize);
        int publishedCount = 0;
        for (UUID eventId : pendingIds) {
            if (relaySingleEvent(eventId)) {
                publishedCount++;
            }
        }
        return publishedCount;
    }

    private boolean relaySingleEvent(final UUID eventId) {
        try {
            return Boolean.TRUE.equals(transactionRunner.executeInTransaction(() -> {
                Optional<OutboxEvent> locked = outboxRepository.lockPendingById(eventId);
                if (locked.isEmpty()) {
                    return false;
                }
                OutboxEvent event = locked.get();
                eventPublisher.publish(event);
                outboxRepository.markSent(event.id(), clock.instant());
                return true;
            }));
        } catch (RuntimeException exception) {
            recordFailureSafely(eventId, exception);
            return false;
        }
    }

    private void recordFailureSafely(final UUID eventId, final RuntimeException cause) {
        String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
        if (message.length() > MAX_ERROR_LENGTH) {
            message = message.substring(0, MAX_ERROR_LENGTH);
        }
        String truncated = message;
        transactionRunner.runInTransaction(() -> outboxRepository.recordFailure(eventId, truncated));
    }
}
