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
        int schemaVersion
) {
}
