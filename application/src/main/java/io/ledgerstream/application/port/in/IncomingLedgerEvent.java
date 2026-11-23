package io.ledgerstream.application.port.in;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record IncomingLedgerEvent(
        UUID eventId,
        String eventType,
        UUID aggregateId,
        UUID bookingId,
        UUID originalBookingId,
        String bookingType,
        BigDecimal amount,
        String currency,
        String reference,
        long version,
        Instant occurredAt,
        int schemaVersion,
        String initiationChannel
) {

    public static final String DEFAULT_INITIATION_CHANNEL = "API";

    public IncomingLedgerEvent {
        if (initiationChannel == null || initiationChannel.isBlank()) {
            initiationChannel = DEFAULT_INITIATION_CHANNEL;
        }
    }

    public IncomingLedgerEvent(
            final UUID eventId,
            final String eventType,
            final UUID aggregateId,
            final UUID bookingId,
            final UUID originalBookingId,
            final String bookingType,
            final BigDecimal amount,
            final String currency,
            final String reference,
            final long version,
            final Instant occurredAt,
            final int schemaVersion
    ) {
        this(
                eventId, eventType, aggregateId, bookingId, originalBookingId,
                bookingType, amount, currency, reference, version, occurredAt,
                schemaVersion, DEFAULT_INITIATION_CHANNEL
        );
    }
}
