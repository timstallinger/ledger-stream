package io.ledgerstream.adapter.out.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.application.port.out.OutboxPayloadSerializer;
import io.ledgerstream.domain.event.BookingCancelledEvent;
import io.ledgerstream.domain.event.BookingCreatedEvent;
import io.ledgerstream.domain.event.DomainEvent;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class JacksonOutboxPayloadSerializer implements OutboxPayloadSerializer {

    private final ObjectMapper objectMapper;

    public JacksonOutboxPayloadSerializer(final ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    @Override
    public String serialize(final DomainEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        Map<String, Object> envelope = switch (event) {
            case BookingCreatedEvent created -> toMap(
                    created.eventId().toString(),
                    created.eventType(),
                    created.aggregateId().toString(),
                    created.bookingId().toString(),
                    null,
                    created.bookingType().name(),
                    created.amount().toPlainString(),
                    created.currency(),
                    created.reference(),
                    created.version(),
                    created.occurredAt().toString(),
                    created.schemaVersion()
            );
            case BookingCancelledEvent cancelled -> toMap(
                    cancelled.eventId().toString(),
                    cancelled.eventType(),
                    cancelled.aggregateId().toString(),
                    cancelled.counterBookingId().toString(),
                    cancelled.originalBookingId().toString(),
                    cancelled.counterBookingType().name(),
                    cancelled.amount().toPlainString(),
                    cancelled.currency(),
                    cancelled.reference(),
                    cancelled.version(),
                    cancelled.occurredAt().toString(),
                    cancelled.schemaVersion()
            );
        };
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize domain event for outbox: " + event.eventId(), exception);
        }
    }

    private static Map<String, Object> toMap(
            final String eventId,
            final String eventType,
            final String aggregateId,
            final String bookingId,
            final String originalBookingId,
            final String bookingType,
            final String amount,
            final String currency,
            final String reference,
            final long version,
            final String occurredAt,
            final int schemaVersion
    ) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("eventId", eventId);
        map.put("eventType", eventType);
        map.put("aggregateId", aggregateId);
        map.put("bookingId", bookingId);
        map.put("originalBookingId", originalBookingId);
        map.put("bookingType", bookingType);
        map.put("amount", amount);
        map.put("currency", currency);
        map.put("reference", reference);
        map.put("version", version);
        map.put("occurredAt", occurredAt);
        map.put("schemaVersion", schemaVersion);
        map.put("initiationChannel", "API");
        return map;
    }
}
