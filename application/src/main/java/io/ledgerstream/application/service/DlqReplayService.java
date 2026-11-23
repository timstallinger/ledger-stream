package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.ListDlqUseCase;
import io.ledgerstream.application.port.in.ReplayDlqCommand;
import io.ledgerstream.application.port.in.ReplayDlqUseCase;
import io.ledgerstream.application.port.out.DlqEntry;
import io.ledgerstream.application.port.out.DlqObserver;
import io.ledgerstream.application.port.out.DlqRepository;
import io.ledgerstream.application.port.out.RawEventPublisher;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.domain.exception.DlqEntryAlreadyReplayedException;
import io.ledgerstream.domain.exception.DlqEntryNotFoundException;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DlqReplayService implements ReplayDlqUseCase, ListDlqUseCase {

    private static final String HEADER_REPLAYED_FROM = "x-dlq-replayed-from";
    private static final String HEADER_CORRELATION_ID = "X-Correlation-Id";

    private final DlqRepository dlqRepository;
    private final RawEventPublisher rawEventPublisher;
    private final DlqObserver dlqObserver;
    private final TransactionRunner transactionRunner;
    private final Clock clock;

    public DlqReplayService(
            final DlqRepository dlqRepository,
            final RawEventPublisher rawEventPublisher,
            final DlqObserver dlqObserver,
            final TransactionRunner transactionRunner,
            final Clock clock
    ) {
        this.dlqRepository = Objects.requireNonNull(dlqRepository, "dlqRepository must not be null");
        this.rawEventPublisher = Objects.requireNonNull(rawEventPublisher, "rawEventPublisher must not be null");
        this.dlqObserver = Objects.requireNonNull(dlqObserver, "dlqObserver must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public DlqEntry replay(final ReplayDlqCommand command) {
        Objects.requireNonNull(command, "command must not be null");

        DlqEntry replayed = transactionRunner.executeInTransaction(() -> {
            DlqEntry existing = dlqRepository.findById(command.dlqEntryId())
                    .orElseThrow(() -> new DlqEntryNotFoundException(command.dlqEntryId()));
            if (existing.replayedAt() != null) {
                throw new DlqEntryAlreadyReplayedException(command.dlqEntryId());
            }

            Instant now = clock.instant();
            boolean transitioned = dlqRepository.markReplayed(
                    existing.id(),
                    now,
                    command.replayedBy(),
                    command.note()
            );
            if (!transitioned) {
                throw new DlqEntryAlreadyReplayedException(command.dlqEntryId());
            }

            Map<String, String> replayHeaders = new LinkedHashMap<>();
            replayHeaders.put(HEADER_REPLAYED_FROM, existing.id().toString());
            String correlationId = existing.headers().get("correlationId");
            if (correlationId != null && !correlationId.isBlank()) {
                replayHeaders.put(HEADER_CORRELATION_ID, correlationId);
            }

            rawEventPublisher.publishRaw(existing.topic(), existing.recordKey(), existing.payload(), replayHeaders);
            return existing.withReplayMetadata(now, command.replayedBy(), command.note());
        });

        dlqObserver.onDlqReplayed(replayed.id(), replayed.topic(), replayed.recordKey());
        return replayed;
    }

    @Override
    public List<DlqEntry> listUnhandled() {
        return dlqRepository.findUnhandled();
    }
}
