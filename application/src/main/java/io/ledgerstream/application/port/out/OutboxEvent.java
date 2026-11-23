package io.ledgerstream.application.port.out;

import io.ledgerstream.domain.event.DomainEvent;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record OutboxEvent(
        UUID id,
        String aggregateType,
        UUID aggregateId,
        String eventType,
        int schemaVersion,
        String payload,
        String correlationId,
        OutboxStatus status,
        Instant occurredAt,
        Instant createdAt,
        Instant sentAt,
        int retryCount,
        String lastError,
        Instant nextAttemptAt
) {

    public static final String AGGREGATE_TYPE_ACCOUNT = "Account";

    public OutboxEvent {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(aggregateType, "aggregateType must not be null");
        Objects.requireNonNull(aggregateId, "aggregateId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(nextAttemptAt, "nextAttemptAt must not be null");
    }

    public static OutboxEvent pending(
            final DomainEvent event,
            final String payload,
            final String correlationId,
            final Instant createdAt
    ) {
        Objects.requireNonNull(event, "event must not be null");
        return new OutboxEvent(
                event.eventId(),
                AGGREGATE_TYPE_ACCOUNT,
                event.aggregateId(),
                event.eventType(),
                event.schemaVersion(),
                payload,
                correlationId,
                OutboxStatus.PENDING,
                event.occurredAt(),
                createdAt,
                null,
                0,
                null,
                createdAt
        );
    }
}
