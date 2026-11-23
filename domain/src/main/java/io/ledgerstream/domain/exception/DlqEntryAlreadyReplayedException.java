package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;

import java.util.UUID;

public final class DlqEntryAlreadyReplayedException extends DomainException {

    public DlqEntryAlreadyReplayedException(final UUID dlqEntryId) {
        super("DLQ_ENTRY_ALREADY_REPLAYED", "DLQ entry has already been replayed: " + dlqEntryId);
    }
}
