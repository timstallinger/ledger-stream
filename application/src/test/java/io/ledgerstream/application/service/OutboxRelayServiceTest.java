package io.ledgerstream.application.service;

import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxEventPublisher;
import io.ledgerstream.application.port.out.OutboxRelayObserver;
import io.ledgerstream.application.port.out.OutboxRepository;
import io.ledgerstream.application.port.out.OutboxStatus;
import io.ledgerstream.application.port.out.PendingOutboxCandidate;
import io.ledgerstream.application.port.out.TransactionRunner;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

final class OutboxRelayServiceTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");

    @Test
    void commitsEachEventIndividuallyAndStopsOnlyFailingAccountWhileOtherAccountsContinue() {
        InMemoryOutboxRepository repository = new InMemoryOutboxRepository();
        UUID accountA = UUID.randomUUID();
        UUID accountB = UUID.randomUUID();

        UUID aEvent1 = UUID.randomUUID();
        UUID aEvent2 = UUID.randomUUID();
        UUID bEvent1 = UUID.randomUUID();

        repository.save(pendingEvent(aEvent1, accountA, T0));
        repository.save(pendingEvent(aEvent2, accountA, T0.plusMillis(10)));
        repository.save(pendingEvent(bEvent1, accountB, T0.plusMillis(20)));

        List<UUID> publishedIds = new ArrayList<>();
        OutboxEventPublisher publisher = event -> {
            if (event.id().equals(aEvent1)) {
                throw new IllegalStateException("Simulated broker timeout on account A");
            }
            publishedIds.add(event.id());
        };

        OutboxRelayService relay = new OutboxRelayService(
                repository,
                publisher,
                noopObserver(),
                directTransactionRunner(),
                Clock.fixed(T0.plusSeconds(1), ZoneOffset.UTC),
                new OutboxRetryBackoff(() -> 0.5),
                500,
                10
        );

        int sentCount = relay.relayPendingBatch();

        assertThat(sentCount).isEqualTo(1);
        assertThat(publishedIds).containsExactly(bEvent1);
        assertThat(repository.get(aEvent1).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(repository.get(aEvent1).retryCount()).isEqualTo(1);
        assertThat(repository.get(aEvent1).nextAttemptAt()).isEqualTo(T0.plusSeconds(2));
        assertThat(repository.get(aEvent2).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(repository.get(aEvent2).retryCount()).isZero();
        assertThat(repository.get(bEvent1).status()).isEqualTo(OutboxStatus.SENT);
    }

    @Test
    void marksEventFailedAndNotifiesObserverAfterTenAttempts() {
        InMemoryOutboxRepository repository = new InMemoryOutboxRepository();
        UUID eventId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        repository.save(pendingEvent(eventId, accountId, T0));

        MutableClock clock = new MutableClock(T0);
        List<UUID> permanentlyFailed = new ArrayList<>();
        OutboxRelayObserver observer = (id, aggId, retryCount, error) -> {
            if (retryCount == 10) {
                permanentlyFailed.add(id);
            }
        };

        OutboxRelayService relay = new OutboxRelayService(
                repository,
                event -> {
                    throw new IllegalStateException("Persistent broker failure");
                },
                observer,
                directTransactionRunner(),
                clock,
                new OutboxRetryBackoff(() -> 0.5),
                500,
                10
        );

        for (int attempt = 1; attempt <= 9; attempt++) {
            assertThat(relay.relayPendingBatch()).isZero();
            OutboxEvent current = repository.get(eventId);
            assertThat(current.status()).isEqualTo(OutboxStatus.PENDING);
            assertThat(current.retryCount()).isEqualTo(attempt);
            assertThat(permanentlyFailed).isEmpty();
            clock.set(current.nextAttemptAt());
        }

        assertThat(relay.relayPendingBatch()).isZero();
        OutboxEvent finalState = repository.get(eventId);
        assertThat(finalState.status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(finalState.retryCount()).isEqualTo(10);
        assertThat(permanentlyFailed).containsExactly(eventId);
        assertThat(repository.countPending()).isZero();
    }

    @Test
    void failureAfterPublishLeavesEventPendingAndNextDueRunRedeliversAtLeastOnce() {
        InMemoryOutboxRepository delegate = new InMemoryOutboxRepository();
        UUID eventId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        delegate.save(pendingEvent(eventId, accountId, T0));

        MutableClock clock = new MutableClock(T0);
        AtomicBoolean failMarkSentOnce = new AtomicBoolean(true);
        OutboxRepository flakyMarkSentRepo = new OutboxRepository() {
            @Override
            public void save(final OutboxEvent event) {
                delegate.save(event);
            }

            @Override
            public List<PendingOutboxCandidate> findPendingCandidates(final int limit, final Instant now) {
                return delegate.findPendingCandidates(limit, now);
            }

            @Override
            public Optional<OutboxEvent> lockPendingById(final UUID id, final Instant now) {
                return delegate.lockPendingById(id, now);
            }

            @Override
            public void markSent(final UUID id, final Instant sentAt) {
                if (failMarkSentOnce.compareAndSet(true, false)) {
                    throw new IllegalStateException("Simulated DB failure after broker ack");
                }
                delegate.markSent(id, sentAt);
            }

            @Override
            public int recordRetryFailure(
                    final UUID id,
                    final String errorMessage,
                    final Instant nextAttemptAt,
                    final int maxAttempts
            ) {
                return delegate.recordRetryFailure(id, errorMessage, nextAttemptAt, maxAttempts);
            }

            @Override
            public long countPending() {
                return delegate.countPending();
            }

            @Override
            public Optional<Instant> findOldestPendingCreatedAt() {
                return delegate.findOldestPendingCreatedAt();
            }
        };

        List<UUID> publishedIds = new ArrayList<>();
        OutboxRelayService relay = new OutboxRelayService(
                flakyMarkSentRepo,
                event -> publishedIds.add(event.id()),
                noopObserver(),
                directTransactionRunner(),
                clock,
                new OutboxRetryBackoff(() -> 0.5),
                500,
                10
        );

        assertThat(relay.relayPendingBatch()).isZero();
        assertThat(publishedIds).containsExactly(eventId);
        assertThat(delegate.get(eventId).status()).isEqualTo(OutboxStatus.PENDING);

        // Before backoff elapses, event must not be retried prematurely
        assertThat(relay.relayPendingBatch()).isZero();
        assertThat(publishedIds).hasSize(1);

        clock.advance(Duration.ofSeconds(2));
        assertThat(relay.relayPendingBatch()).isEqualTo(1);
        assertThat(publishedIds).containsExactly(eventId, eventId);
        assertThat(delegate.get(eventId).status()).isEqualTo(OutboxStatus.SENT);
        assertThat(delegate.countPending()).isZero();
    }

    @Test
    void respectsConfiguredBatchSizeLimit() {
        InMemoryOutboxRepository repository = new InMemoryOutboxRepository();
        for (int i = 0; i < 5; i++) {
            repository.save(pendingEvent(UUID.randomUUID(), UUID.randomUUID(), T0.plusMillis(i)));
        }

        OutboxRelayService relay = new OutboxRelayService(
                repository,
                event -> {
                },
                noopObserver(),
                directTransactionRunner(),
                Clock.fixed(T0.plusSeconds(1), ZoneOffset.UTC),
                new OutboxRetryBackoff(() -> 0.5),
                3,
                10
        );

        assertThat(relay.relayPendingBatch()).isEqualTo(3);
        assertThat(repository.countPending()).isEqualTo(2L);
        assertThat(relay.relayPendingBatch()).isEqualTo(2);
        assertThat(repository.countPending()).isZero();
    }

    private static OutboxRelayObserver noopObserver() {
        return (eventId, aggregateId, retryCount, errorMessage) -> {
        };
    }

    private static OutboxEvent pendingEvent(final UUID id, final UUID accountId, final Instant createdAt) {
        return new OutboxEvent(
                id,
                "Account",
                accountId,
                "BookingCreated",
                1,
                "{}",
                null,
                OutboxStatus.PENDING,
                createdAt,
                createdAt,
                null,
                0,
                null,
                createdAt
        );
    }

    private static TransactionRunner directTransactionRunner() {
        return new TransactionRunner() {
            @Override
            public <T> T executeInTransaction(final Supplier<T> action) {
                return action.get();
            }
        };
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> current;

        private MutableClock(final Instant initial) {
            this.current = new AtomicReference<>(initial);
        }

        void set(final Instant instant) {
            current.set(instant);
        }

        void advance(final Duration duration) {
            current.updateAndGet(i -> i.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return current.get();
        }
    }

    private static final class InMemoryOutboxRepository implements OutboxRepository {
        private final Map<UUID, OutboxEvent> events = new LinkedHashMap<>();

        @Override
        public void save(final OutboxEvent event) {
            events.put(event.id(), event);
        }

        @Override
        public List<PendingOutboxCandidate> findPendingCandidates(final int limit, final Instant now) {
            return events.values().stream()
                    .filter(e -> e.status() == OutboxStatus.PENDING && !e.nextAttemptAt().isAfter(now))
                    .map(e -> new PendingOutboxCandidate(e.id(), e.aggregateId()))
                    .limit(limit)
                    .toList();
        }

        @Override
        public Optional<OutboxEvent> lockPendingById(final UUID eventId, final Instant now) {
            OutboxEvent event = events.get(eventId);
            if (event == null || event.status() != OutboxStatus.PENDING || event.nextAttemptAt().isAfter(now)) {
                return Optional.empty();
            }
            return Optional.of(event);
        }

        @Override
        public void markSent(final UUID eventId, final Instant sentAt) {
            OutboxEvent current = events.get(eventId);
            events.put(eventId, new OutboxEvent(
                    current.id(),
                    current.aggregateType(),
                    current.aggregateId(),
                    current.eventType(),
                    current.schemaVersion(),
                    current.payload(),
                    current.correlationId(),
                    OutboxStatus.SENT,
                    current.occurredAt(),
                    current.createdAt(),
                    sentAt,
                    current.retryCount(),
                    null,
                    current.nextAttemptAt()
            ));
        }

        @Override
        public int recordRetryFailure(
                final UUID eventId,
                final String errorMessage,
                final Instant nextAttemptAt,
                final int maxAttempts
        ) {
            OutboxEvent current = events.get(eventId);
            int nextRetries = current.retryCount() + 1;
            OutboxStatus nextStatus = nextRetries >= maxAttempts ? OutboxStatus.FAILED : OutboxStatus.PENDING;
            events.put(eventId, new OutboxEvent(
                    current.id(),
                    current.aggregateType(),
                    current.aggregateId(),
                    current.eventType(),
                    current.schemaVersion(),
                    current.payload(),
                    current.correlationId(),
                    nextStatus,
                    current.occurredAt(),
                    current.createdAt(),
                    current.sentAt(),
                    nextRetries,
                    errorMessage,
                    nextAttemptAt
            ));
            return nextRetries;
        }

        @Override
        public long countPending() {
            return events.values().stream().filter(e -> e.status() == OutboxStatus.PENDING).count();
        }

        @Override
        public Optional<Instant> findOldestPendingCreatedAt() {
            return events.values().stream()
                    .filter(e -> e.status() == OutboxStatus.PENDING)
                    .map(OutboxEvent::createdAt)
                    .min(Instant::compareTo);
        }

        OutboxEvent get(final UUID id) {
            return events.get(id);
        }
    }
}
