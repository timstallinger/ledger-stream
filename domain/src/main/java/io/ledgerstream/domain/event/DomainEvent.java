package io.ledgerstream.domain.event;

import java.time.Instant;
import java.util.UUID;

public sealed interface DomainEvent permits BookingCreatedEvent, BookingCancelledEvent {

    UUID eventId();

    UUID aggregateId();

    long version();

    Instant occurredAt();

    int schemaVersion();

    String eventType();
}
