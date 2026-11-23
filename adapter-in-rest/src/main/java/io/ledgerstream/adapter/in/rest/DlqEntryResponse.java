package io.ledgerstream.adapter.in.rest;

import io.ledgerstream.application.port.out.DlqEntry;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record DlqEntryResponse(
        UUID id,
        String topic,
        int partition,
        long offset,
        String recordKey,
        UUID eventId,
        String errorClass,
        String errorMessage,
        int attemptCount,
        int payloadSizeBytes,
        Map<String, String> headers,
        Instant createdAt,
        Instant replayedAt,
        String replayedBy,
        String note
) {

    public static DlqEntryResponse from(final DlqEntry entry) {
        return new DlqEntryResponse(
                entry.id(),
                entry.topic(),
                entry.partition(),
                entry.offset(),
                entry.recordKey(),
                entry.eventId(),
                entry.errorClass(),
                entry.errorMessage(),
                entry.attemptCount(),
                entry.payload().length,
                entry.headers(),
                entry.createdAt(),
                entry.replayedAt(),
                entry.replayedBy(),
                entry.note()
        );
    }
}
