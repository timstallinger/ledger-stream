package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;

import java.util.UUID;

public final class DlqEntryNotFoundException extends DomainException {

    public DlqEntryNotFoundException(final UUID dlqEntryId) {
        super("DLQ_ENTRY_NOT_FOUND", "DLQ entry not found: " + dlqEntryId);
    }
}
