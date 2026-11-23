package io.ledgerstream.application.port.in;

import java.util.Objects;
import java.util.UUID;

public record ReplayDlqCommand(
        UUID dlqEntryId,
        String replayedBy,
        String note
) {

    public ReplayDlqCommand {
        Objects.requireNonNull(dlqEntryId, "dlqEntryId must not be null");
        if (replayedBy == null || replayedBy.isBlank()) {
            replayedBy = "operator";
        }
    }
}
