package io.ledgerstream.application.port.out;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class DlqEntry {

    private final UUID id;
    private final String topic;
    private final int partition;
    private final long offset;
    private final String recordKey;
    private final UUID eventId;
    private final String errorClass;
    private final String errorMessage;
    private final int attemptCount;
    private final byte[] payload;
    private final Map<String, String> headers;
    private final Instant createdAt;
    private final Instant replayedAt;
    private final String replayedBy;
    private final String note;

    public DlqEntry(
            final UUID id,
            final String topic,
            final int partition,
            final long offset,
            final String recordKey,
            final UUID eventId,
            final String errorClass,
            final String errorMessage,
            final int attemptCount,
            final byte[] payload,
            final Map<String, String> headers,
            final Instant createdAt,
            final Instant replayedAt,
            final String replayedBy,
            final String note
    ) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.partition = partition;
        this.offset = offset;
        this.recordKey = recordKey;
        this.eventId = eventId;
        this.errorClass = Objects.requireNonNull(errorClass, "errorClass must not be null");
        this.errorMessage = Objects.requireNonNull(errorMessage, "errorMessage must not be null");
        this.attemptCount = attemptCount;
        this.payload = payload != null ? Arrays.copyOf(payload, payload.length) : new byte[0];
        this.headers = headers != null
                ? Collections.unmodifiableMap(new LinkedHashMap<>(headers))
                : Map.of();
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        this.replayedAt = replayedAt;
        this.replayedBy = replayedBy;
        this.note = note;
    }

    public UUID id() {
        return id;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public String recordKey() {
        return recordKey;
    }

    public UUID eventId() {
        return eventId;
    }

    public String errorClass() {
        return errorClass;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public int attemptCount() {
        return attemptCount;
    }

    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }

    public Map<String, String> headers() {
        return headers;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant replayedAt() {
        return replayedAt;
    }

    public String replayedBy() {
        return replayedBy;
    }

    public String note() {
        return note;
    }

    public DlqEntry withReplayMetadata(final Instant replayedAtValue, final String replayedByValue, final String noteValue) {
        return new DlqEntry(
                id, topic, partition, offset, recordKey, eventId,
                errorClass, errorMessage, attemptCount, payload, headers,
                createdAt, replayedAtValue, replayedByValue, noteValue
        );
    }
}
