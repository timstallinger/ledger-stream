package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.RelayOutboxUseCase;
import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxEventPublisher;
import io.ledgerstream.application.port.out.OutboxRelayObserver;
import io.ledgerstream.application.port.out.OutboxRepository;
import io.ledgerstream.application.port.out.PendingOutboxCandidate;
import io.ledgerstream.application.port.out.TransactionRunner;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public final class OutboxRelayService implements RelayOutboxUseCase {

    public static final int DEFAULT_MAX_ATTEMPTS = 10;
    private static final int MAX_ERROR_LENGTH = 1000;

    private final OutboxRepository outboxRepository;
    private final OutboxEventPublisher eventPublisher;
    private final OutboxRelayObserver observer;
    private final TransactionRunner transactionRunner;
    private final Clock clock;
    private final OutboxRetryBackoff retryBackoff;
    private final int batchSize;
    private final int maxAttempts;

    public OutboxRelayService(
            final OutboxRepository outboxRepository,
            final OutboxEventPublisher eventPublisher,
            final OutboxRelayObserver observer,
            final TransactionRunner transactionRunner,
            final Clock clock,
            final OutboxRetryBackoff retryBackoff,
            final int batchSize,
            final int maxAttempts
    ) {
        this.outboxRepository = Objects.requireNonNull(outboxRepository, "outboxRepository must not be null");
        this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher must not be null");
        this.observer = Objects.requireNonNull(observer, "observer must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive: " + maxAttempts);
        }
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
    }

    /**
     * Relays due PENDING outbox events in created_at order, committing each event in its own short transaction
     * immediately after the Kafka broker ack so the failure window stays at a single event.
     *
     * Per-account ordering: on the first failure for an account, all subsequent events of that same account are
     * skipped in this cycle so a later event (e.g. BookingCancelled) can never overtake an earlier retrying event
     * (e.g. BookingCreated); unrelated accounts keep flowing without head-of-line blocking.
     *
     * Scaling note (Phase 11): FOR UPDATE SKIP LOCKED makes multiple relay instances safe against double-locking
     * the same row, but strict per-account ordering holds per instance only. Scaling beyond 1 relay replica
     * requires hash-partitioned relay ownership or single-leader relay execution.
     */
    @Override
    public int relayPendingBatch() {
        Instant now = clock.instant();
        List<PendingOutboxCandidate> candidates = outboxRepository.findPendingCandidates(batchSize, now);
        Set<UUID> blockedAccounts = new HashSet<>();
        int publishedCount = 0;

        for (PendingOutboxCandidate candidate : candidates) {
            if (blockedAccounts.contains(candidate.aggregateId())) {
                continue;
            }
            if (relaySingleEvent(candidate, now)) {
                publishedCount++;
            } else {
                blockedAccounts.add(candidate.aggregateId());
            }
        }
        return publishedCount;
    }

    private boolean relaySingleEvent(final PendingOutboxCandidate candidate, final Instant pollTime) {
        try {
            return Boolean.TRUE.equals(transactionRunner.executeInTransaction(() -> {
                Optional<OutboxEvent> locked = outboxRepository.lockPendingById(candidate.eventId(), pollTime);
                if (locked.isEmpty()) {
                    return false;
                }
                OutboxEvent event = locked.get();
                eventPublisher.publish(event);
                outboxRepository.markSent(event.id(), clock.instant());
                observer.onEventRelayed(event.eventType(), true);
                return true;
            }));
        } catch (RuntimeException exception) {
            observer.onEventRelayed("unknown", false);
            recordFailureWithBackoff(candidate, exception);
            return false;
        }
    }

    private void recordFailureWithBackoff(final PendingOutboxCandidate candidate, final RuntimeException cause) {
        String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
        String truncated = message.length() > MAX_ERROR_LENGTH ? message.substring(0, MAX_ERROR_LENGTH) : message;

        Instant now = clock.instant();
        Integer updatedRetryCount = transactionRunner.executeInTransaction(() -> {
            Optional<OutboxEvent> locked = outboxRepository.lockPendingById(candidate.eventId(), now);
            int nextAttemptNumber = locked.map(e -> e.retryCount() + 1).orElse(1);
            Duration backoff = retryBackoff.delayForAttempt(nextAttemptNumber);
            Instant nextAttemptAt = now.plus(backoff);
            return outboxRepository.recordRetryFailure(candidate.eventId(), truncated, nextAttemptAt, maxAttempts);
        });

        if (updatedRetryCount != null && updatedRetryCount >= maxAttempts) {
            observer.onEventFailedPermanently(candidate.eventId(), candidate.aggregateId(), updatedRetryCount, truncated);
        }
    }
}
