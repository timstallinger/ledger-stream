package io.ledgerstream.adapter.in.messaging;

import io.ledgerstream.application.port.in.IncomingLedgerEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public final class LedgerEventRecordMapper {

    public Optional<IncomingLedgerEvent> map(final GenericRecord record) {
        if (record == null) {
            return Optional.empty();
        }
        try {
            UUID eventId = parseUuid(record.get("eventId"));
            String eventType = asString(record.get("eventType"));
            UUID aggregateId = parseUuid(record.get("aggregateId"));
            UUID bookingId = parseUuid(record.get("bookingId"));
            UUID originalBookingId = parseOptionalUuid(record.get("originalBookingId"));
            String bookingType = asString(record.get("bookingType"));
            BigDecimal amount = parseBigDecimal(record.get("amount"));
            String currency = asString(record.get("currency"));
            String reference = asString(record.get("reference"));
            long version = parseLong(record.get("version"));
            Instant occurredAt = parseInstant(record.get("occurredAt"));
            int schemaVersion = parseInt(record.get("schemaVersion"), 1);
            String initiationChannel = readOptionalFieldWithDefault(
                    record,
                    "initiationChannel",
                    IncomingLedgerEvent.DEFAULT_INITIATION_CHANNEL
            );

            return Optional.of(new IncomingLedgerEvent(
                    eventId,
                    eventType,
                    aggregateId,
                    bookingId,
                    originalBookingId,
                    bookingType,
                    amount,
                    currency,
                    reference,
                    version,
                    occurredAt,
                    schemaVersion,
                    initiationChannel
            ));
        } catch (RuntimeException exception) {
            return Optional.empty();
        }
    }

    private static String readOptionalFieldWithDefault(
            final GenericRecord record,
            final String fieldName,
            final String defaultValue
    ) {
        Schema.Field field = record.getSchema().getField(fieldName);
        if (field == null) {
            return defaultValue;
        }
        Object rawValue = record.get(fieldName);
        if (rawValue == null) {
            Object schemaDefault = field.defaultVal();
            return schemaDefault != null ? schemaDefault.toString() : defaultValue;
        }
        String text = rawValue.toString();
        return text.isBlank() ? defaultValue : text;
    }

    private static String asString(final Object value) {
        return value != null ? value.toString() : null;
    }

    private static UUID parseUuid(final Object value) {
        return value != null ? UUID.fromString(value.toString()) : null;
    }

    private static UUID parseOptionalUuid(final Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        return text.isBlank() ? null : UUID.fromString(text);
    }

    private static BigDecimal parseBigDecimal(final Object value) {
        return value != null ? new BigDecimal(value.toString()) : null;
    }

    private static Instant parseInstant(final Object value) {
        return value != null ? Instant.parse(value.toString()) : null;
    }

    private static long parseLong(final Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return value != null ? Long.parseLong(value.toString()) : 0L;
    }

    private static int parseInt(final Object value, final int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString());
    }
}
