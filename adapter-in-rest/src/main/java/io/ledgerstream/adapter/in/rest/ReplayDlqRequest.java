package io.ledgerstream.adapter.in.rest;

import jakarta.validation.constraints.Size;

public record ReplayDlqRequest(
        @Size(max = 128, message = "replayedBy length must not exceed 128 characters")
        String replayedBy,

        @Size(max = 1000, message = "note length must not exceed 1000 characters")
        String note
) {
}
