package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.ReplayDlqCommand;
import io.ledgerstream.application.port.out.DlqEntry;
import io.ledgerstream.application.port.out.DlqReplayAttempt;
import io.ledgerstream.application.port.out.DlqRepository;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.domain.exception.DlqEntryAlreadyReplayedException;
import io.ledgerstream.domain.exception.DlqEntryNotFoundException;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class DlqReplayServiceTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");

    @Test
    void replaysUnhandledEntryOnceRecordsImmutableAttemptAndRejectsSecondReplayAndUnknownId() {
        InMemoryDlqRepository repository = new InMemoryDlqRepository();
        UUID entryId = UUID.randomUUID();
        byte[] rawPayload = new byte[]{0x00, 0x00, 0x00, 0x00, 0x01, 0x42};
        repository.save(new DlqEntry(
                entryId,
                "ledger.events",
                0,
                12L,
                "acc-1",
                UUID.randomUUID(),
                "java.lang.IllegalStateException",
                "DB timeout",
                5,
                rawPayload,
                Map.of("correlationId", "corr-1"),
                T0,
                null,
                null
        ));

        List<byte[]> publishedPayloads = new ArrayList<>();
        List<UUID> observedReplays = new ArrayList<>();
        DlqReplayService service = new DlqReplayService(
                repository,
                (topic, key, payload, headers) -> publishedPayloads.add(payload),
                (id, topic, key) -> observedReplays.add(id),
                new TransactionRunner() {
                    @Override
                    public <T> T executeInTransaction(final Supplier<T> action) {
                        return action.get();
                    }
                },
                Clock.fixed(T0.plusSeconds(30), ZoneOffset.UTC)
        );

        DlqEntry replayed = service.replay(new ReplayDlqCommand(entryId, "ops-user", "Fixed DB"));

        assertThat(replayed.replayedAt()).isEqualTo(T0.plusSeconds(30));
        assertThat(replayed.lastReplayedBy()).isEqualTo("ops-user");
        assertThat(publishedPayloads).hasSize(1);
        assertThat(publishedPayloads.getFirst()).isEqualTo(rawPayload);
        assertThat(observedReplays).containsExactly(entryId);
        assertThat(repository.countUnhandled()).isZero();
        assertThat(repository.findReplayAttempts(entryId)).hasSize(1);
        DlqReplayAttempt attempt = repository.findReplayAttempts(entryId).getFirst();
        assertThat(attempt.attemptedBy()).isEqualTo("ops-user");
        assertThat(attempt.outcome()).isEqualTo("DISPATCHED");
        assertThat(attempt.errorClass()).isNull();
        assertThat(attempt.note()).isEqualTo("Fixed DB");

        assertThatThrownBy(() -> service.replay(new ReplayDlqCommand(entryId, "ops-user", "Retry")))
                .isInstanceOf(DlqEntryAlreadyReplayedException.class);
        assertThatThrownBy(() -> service.replay(new ReplayDlqCommand(UUID.randomUUID(), "ops-user", "Missing")))
                .isInstanceOf(DlqEntryNotFoundException.class);
    }

    private static final class InMemoryDlqRepository implements DlqRepository {
        private final Map<UUID, DlqEntry> entries = new HashMap<>();
        private final List<DlqReplayAttempt> attempts = new ArrayList<>();

        @Override
        public void save(final DlqEntry entry) {
            entries.put(entry.id(), entry);
        }

        @Override
        public Optional<DlqEntry> findById(final UUID id) {
            return Optional.ofNullable(entries.get(id));
        }

        @Override
        public List<DlqEntry> findUnhandled() {
            return entries.values().stream().filter(e -> e.replayedAt() == null).toList();
        }

        @Override
        public boolean markReplayed(final UUID id, final Instant replayedAt, final String lastReplayedBy) {
            DlqEntry current = entries.get(id);
            if (current == null || current.replayedAt() != null) {
                return false;
            }
            entries.put(id, current.withReplayState(replayedAt, lastReplayedBy));
            return true;
        }

        @Override
        public void recordReplayAttempt(final DlqReplayAttempt attempt) {
            attempts.add(attempt);
        }

        @Override
        public List<DlqReplayAttempt> findReplayAttempts(final UUID dlqEntryId) {
            return attempts.stream().filter(a -> a.dlqEntryId().equals(dlqEntryId)).toList();
        }

        @Override
        public long countUnhandled() {
            return entries.values().stream().filter(e -> e.replayedAt() == null).count();
        }
    }
}
