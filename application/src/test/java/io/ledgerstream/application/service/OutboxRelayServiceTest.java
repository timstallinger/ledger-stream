package io.ledgerstream.application.service;

import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxEventPublisher;
import io.ledgerstream.application.port.out.OutboxRepository;
import io.ledgerstream.application.port.out.OutboxStatus;
import io.ledgerstream.application.port.out.TransactionRunner;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

final class OutboxRelayServiceTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(T0, ZoneOffset.UTC);

    @Test
    void commitsEachEventIndividuallySoFailureOnSecondDoesNotUndoFirst() {
        InMemoryOutboxRepository repository = new InMemoryOutboxRepository();
        UUID firstId = UUID.randomUUID();
        UUID secondId = UUID.randomUUID();
        repository.save(pendingEvent(firstId));
        repository.save(pendingEvent(secondId));

        List<UUID> publishedIds = new ArrayList<>();
        OutboxEventPublisher publisher = event -> {
            if (event.id().equals(secondId)) {
                throw new IllegalStateException("Simulated broker timeout");
            }
            publishedIds.add(event.id());
        };

        OutboxRelayService relay = new OutboxRelayService(
                repository,
                publisher,
                directTransactionRunner(),
                FIXED_CLOCK,
                500
        );

        int sentCount = relay.relayPendingBatch();

        assertThat(sentCount).isEqualTo(1);
        assertThat(publishedIds).containsExactly(firstId);
        assertThat(repository.get(firstId).status()).isEqualTo(OutboxStatus.SENT);
        assertThat(repository.get(secondId).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(repository.get(secondId).retryCount()).isEqualTo(1);
        assertThat(repository.get(secondId).lastError()).contains("Simulated broker timeout");
        assertThat(repository.countPending()).isEqualTo(1L);
    }

    @Test
    void failureAfterPublishLeavesEventPendingAndNextRunRedeliversAtLeastOnce() {
        InMemoryOutboxRepository delegate = new InMemoryOutboxRepository();
        UUID eventId = UUID.randomUUID();
        delegate.save(pendingEvent(eventId));

        AtomicBoolean failMarkSentOnce = new AtomicBoolean(true);
        OutboxRepository flakyMarkSentRepo = new OutboxRepository() {
            @Override
            public void save(final OutboxEvent event) {
                delegate.save(event);
            }

            @Override
            public List<UUID> findPendingIds(final int limit) {
                return delegate.findPendingIds(limit);
            }

            @Override
            public Optional<OutboxEvent> lockPendingById(final UUID id) {
                return delegate.lockPendingById(id);
            }

            @Override
            public void markSent(final UUID id, final Instant sentAt) {
                if (failMarkSentOnce.compareAndSet(true, false)) {
                    throw new IllegalStateException("Simulated DB failure after broker ack");
                }
                delegate.markSent(id, sentAt);
            }

            @Override
            public void recordFailure(final UUID id, final String errorMessage) {
                delegate.recordFailure(id, errorMessage);
            }

            @Override
            public long countPending() {
                return delegate.countPending();
            }
        };

        List<UUID> publishedIds = new ArrayList<>();
        OutboxRelayService relay = new OutboxRelayService(
                flakyMarkSentRepo,
                event -> publishedIds.add(event.id()),
                directTransactionRunner(),
                FIXED_CLOCK,
                500
        );

        int firstRun = relay.relayPendingBatch();
        assertThat(firstRun).isZero();
        assertThat(publishedIds).containsExactly(eventId);
        assertThat(delegate.get(eventId).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(delegate.countPending()).isEqualTo(1L);

        int secondRun = relay.relayPendingBatch();
        assertThat(secondRun).isEqualTo(1);
        assertThat(publishedIds).containsExactly(eventId, eventId);
        assertThat(delegate.get(eventId).status()).isEqualTo(OutboxStatus.SENT);
        assertThat(delegate.countPending()).isZero();
    }

    @Test
    void respectsConfiguredBatchSizeLimit() {
        InMemoryOutboxRepository repository = new InMemoryOutboxRepository();
        for (int i = 0; i < 5; i++) {
            repository.save(pendingEvent(UUID.randomUUID()));
        }

        OutboxRelayService relay = new OutboxRelayService(
                repository,
                event -> {
                },
                directTransactionRunner(),
                FIXED_CLOCK,
                3
        );

        assertThat(relay.relayPendingBatch()).isEqualTo(3);
        assertThat(repository.countPending()).isEqualTo(2L);
        assertThat(relay.relayPendingBatch()).isEqualTo(2);
        assertThat(repository.countPending()).isZero();
    }

    private static OutboxEvent pendingEvent(final UUID id) {
        return new OutboxEvent(
                id,
                "Account",
                UUID.randomUUID(),
                "BookingCreated",
                1,
                "{}",
                null,
                OutboxStatus.PENDING,
                T0,
                T0,
                null,
                0,
                null
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

    private static final class InMemoryOutboxRepository implements OutboxRepository {
        private final Map<UUID, OutboxEvent> events = new LinkedHashMap<>();

        @Override
        public void save(final OutboxEvent event) {
            events.put(event.id(), event);
        }

        @Override
        public List<UUID> findPendingIds(final int limit) {
            return events.values().stream()
                    .filter(e -> e.status() == OutboxStatus.PENDING)
                    .map(OutboxEvent::id)
                    .limit(limit)
                    .toList();
        }

        @Override
        public Optional<OutboxEvent> lockPendingById(final UUID eventId) {
            OutboxEvent event = events.get(eventId);
            if (event == null || event.status() != OutboxStatus.PENDING) {
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
                    null
            ));
        }

        @Override
        public void recordFailure(final UUID eventId, final String errorMessage) {
            OutboxEvent current = events.get(eventId);
            events.put(eventId, new OutboxEvent(
                    current.id(),
                    current.aggregateType(),
                    current.aggregateId(),
                    current.eventType(),
                    current.schemaVersion(),
                    current.payload(),
                    current.correlationId(),
                    current.status(),
                    current.occurredAt(),
                    current.createdAt(),
                    current.sentAt(),
                    current.retryCount() + 1,
                    errorMessage
            ));
        }

        @Override
        public long countPending() {
            return events.values().stream().filter(e -> e.status() == OutboxStatus.PENDING).count();
        }

        OutboxEvent get(final UUID id) {
            return events.get(id);
        }
    }
}
