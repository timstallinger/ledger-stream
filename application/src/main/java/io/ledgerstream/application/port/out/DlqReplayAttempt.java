package io.ledgerstream.application.port.out;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record DlqReplayAttempt(
        UUID id,
        UUID dlqEntryId,
        Instant attemptedAt,
        String attemptedBy,
        String outcome,
        String errorClass,
        String note
) {

    public DlqReplayAttempt {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(dlqEntryId, "dlqEntryId must not be null");
        Objects.requireNonNull(attemptedAt, "attemptedAt must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
    }
}
