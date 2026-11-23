package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.ReplayDlqCommand;
import io.ledgerstream.application.port.out.DlqEntry;
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
    void replaysUnhandledEntryOnceAndRejectsSecondReplayAndUnknownId() {
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
        assertThat(replayed.replayedBy()).isEqualTo("ops-user");
        assertThat(replayed.note()).isEqualTo("Fixed DB");
        assertThat(publishedPayloads).hasSize(1);
        assertThat(publishedPayloads.getFirst()).isEqualTo(rawPayload);
        assertThat(observedReplays).containsExactly(entryId);
        assertThat(repository.countUnhandled()).isZero();

        assertThatThrownBy(() -> service.replay(new ReplayDlqCommand(entryId, "ops-user", "Retry")))
                .isInstanceOf(DlqEntryAlreadyReplayedException.class);
        assertThatThrownBy(() -> service.replay(new ReplayDlqCommand(UUID.randomUUID(), "ops-user", "Missing")))
                .isInstanceOf(DlqEntryNotFoundException.class);
    }

    private static final class InMemoryDlqRepository implements DlqRepository {
        private final Map<UUID, DlqEntry> entries = new HashMap<>();

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
        public boolean markReplayed(final UUID id, final Instant replayedAt, final String replayedBy, final String note) {
            DlqEntry current = entries.get(id);
            if (current == null || current.replayedAt() != null) {
                return false;
            }
            entries.put(id, current.withReplayMetadata(replayedAt, replayedBy, note));
            return true;
        }

        @Override
        public long countUnhandled() {
            return entries.values().stream().filter(e -> e.replayedAt() == null).count();
        }
    }
}
