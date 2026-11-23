package io.ledgerstream.adapter.in.messaging;

import io.ledgerstream.application.port.out.DlqEntry;
import io.ledgerstream.application.port.out.DlqRepository;
import io.ledgerstream.application.port.out.TransactionRunner;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class DlqRecordPersister {

    public static final String HEADER_ERROR_CLASS = "errorClass";
    public static final String HEADER_ERROR_MESSAGE = "errorMessage";
    public static final String HEADER_ORIGINAL_TOPIC = "originalTopic";
    public static final String HEADER_ORIGINAL_PARTITION = "originalPartition";
    public static final String HEADER_ORIGINAL_OFFSET = "originalOffset";
    public static final String HEADER_ATTEMPT_COUNT = "attemptCount";
    public static final String HEADER_FAILED_AT = "failedAt";
    public static final String HEADER_CORRELATION_ID = "correlationId";
    public static final String HEADER_SCHEMA_ID = "schemaId";

    private static final Logger LOG = LoggerFactory.getLogger(DlqRecordPersister.class);

    private final DlqRepository dlqRepository;
    private final TransactionRunner transactionRunner;
    private final Clock clock;

    public DlqRecordPersister(
            final DlqRepository dlqRepository,
            final TransactionRunner transactionRunner,
            final Clock clock
    ) {
        this.dlqRepository = Objects.requireNonNull(dlqRepository, "dlqRepository must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    public DlqEntry persist(final ConsumerRecord<String, LedgerConsumerMessage> record) {
        Objects.requireNonNull(record, "record must not be null");

        Headers headers = record.headers();
        LedgerConsumerMessage message = record.value();
        byte[] rawPayload = message != null ? message.rawPayload() : new byte[0];

        String originalTopic = firstNonBlank(
                readStringHeader(headers, HEADER_ORIGINAL_TOPIC),
                readStringHeader(headers, KafkaHeaders.ORIGINAL_TOPIC),
                record.topic()
        );
        int originalPartition = readIntHeader(
                headers,
                HEADER_ORIGINAL_PARTITION,
                readBinaryOrStringIntHeader(headers, KafkaHeaders.ORIGINAL_PARTITION, record.partition())
        );
        long originalOffset = readLongHeader(
                headers,
                HEADER_ORIGINAL_OFFSET,
                readBinaryOrStringLongHeader(headers, KafkaHeaders.ORIGINAL_OFFSET, record.offset())
        );
        int attemptCount = readIntHeader(
                headers,
                HEADER_ATTEMPT_COUNT,
                readBinaryOrStringIntHeader(headers, RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, 1)
        );

        String errorClass = firstNonBlank(
                readStringHeader(headers, HEADER_ERROR_CLASS),
                readStringHeader(headers, RawPayloadPreservingAvroDeserializer.HEADER_EXCEPTION_CLASS),
                readStringHeader(headers, KafkaHeaders.EXCEPTION_FQCN),
                message != null ? message.deserializationErrorClass() : null,
                "UnknownError"
        );
        String errorMessage = firstNonBlank(
                readStringHeader(headers, HEADER_ERROR_MESSAGE),
                readStringHeader(headers, RawPayloadPreservingAvroDeserializer.HEADER_EXCEPTION_MESSAGE),
                readStringHeader(headers, KafkaHeaders.EXCEPTION_MESSAGE),
                message != null ? message.deserializationErrorMessage() : null,
                errorClass
        );

        UUID eventId = extractEventId(message, headers);
        String derivedKey = deriveAccountKey(message, record.key());

        Map<String, String> headerMap = new LinkedHashMap<>();
        putIfPresent(headerMap, HEADER_ERROR_CLASS, errorClass);
        putIfPresent(headerMap, HEADER_ERROR_MESSAGE, errorMessage);
        putIfPresent(headerMap, HEADER_ORIGINAL_TOPIC, originalTopic);
        headerMap.put(HEADER_ORIGINAL_PARTITION, Integer.toString(originalPartition));
        headerMap.put(HEADER_ORIGINAL_OFFSET, Long.toString(originalOffset));
        headerMap.put(HEADER_ATTEMPT_COUNT, Integer.toString(attemptCount));
        putIfPresent(headerMap, HEADER_FAILED_AT, firstNonBlank(
                readStringHeader(headers, HEADER_FAILED_AT),
                clock.instant().toString()
        ));
        putIfPresent(headerMap, HEADER_CORRELATION_ID, firstNonBlank(
                readStringHeader(headers, HEADER_CORRELATION_ID),
                readStringHeader(headers, "X-Correlation-Id")
        ));
        String schemaIdHeader = readStringHeader(headers, HEADER_SCHEMA_ID);
        if (schemaIdHeader != null && !schemaIdHeader.isBlank()) {
            headerMap.put(HEADER_SCHEMA_ID, schemaIdHeader);
        } else if (message != null && message.schemaId() != null) {
            headerMap.put(HEADER_SCHEMA_ID, Integer.toString(message.schemaId()));
        }
        putIfPresent(
                headerMap,
                RawPayloadPreservingAvroDeserializer.HEADER_ORIGINAL_PAYLOAD,
                readStringHeader(headers, RawPayloadPreservingAvroDeserializer.HEADER_ORIGINAL_PAYLOAD)
        );

        DlqEntry entry = new DlqEntry(
                UUID.randomUUID(),
                originalTopic,
                originalPartition,
                originalOffset,
                derivedKey,
                eventId,
                errorClass,
                errorMessage,
                attemptCount,
                rawPayload,
                headerMap,
                clock.instant(),
                null,
                null,
                null
        );

        transactionRunner.runInTransaction(() -> dlqRepository.save(entry));
        LOG.warn(
                "Persisted DLQ entry: id={}, originalTopic={}, partition={}, offset={}, attemptCount={}, errorClass={}",
                entry.id(),
                entry.topic(),
                entry.partition(),
                entry.offset(),
                entry.attemptCount(),
                entry.errorClass()
        );
        return entry;
    }

    private static UUID extractEventId(final LedgerConsumerMessage message, final Headers headers) {
        if (message != null && !message.isDeserializationFailure() && message.record() != null) {
            Object field = message.record().get("eventId");
            if (field != null) {
                try {
                    return UUID.fromString(field.toString());
                } catch (IllegalArgumentException ignored) {
                    // Fall back to header below
                }
            }
        }
        String headerEventId = readStringHeader(headers, "eventId");
        if (headerEventId != null && !headerEventId.isBlank()) {
            try {
                return UUID.fromString(headerEventId);
            } catch (IllegalArgumentException ignored) {
                return null;
            }
        }
        return null;
    }

    private static String deriveAccountKey(final LedgerConsumerMessage message, final String fallbackKey) {
        if (message != null && !message.isDeserializationFailure() && message.record() != null) {
            GenericRecord record = message.record();
            Object aggregateId = record.get("aggregateId");
            if (aggregateId != null && !aggregateId.toString().isBlank()) {
                return aggregateId.toString();
            }
        }
        return fallbackKey;
    }

    private static void putIfPresent(final Map<String, String> target, final String key, final String value) {
        if (value != null && !value.isBlank()) {
            target.put(key, value);
        }
    }

    private static String firstNonBlank(final String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                return candidate;
            }
        }
        return null;
    }

    private static String readStringHeader(final Headers headers, final String name) {
        if (headers == null) {
            return null;
        }
        Header header = headers.lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int readIntHeader(final Headers headers, final String name, final int fallback) {
        String value = readStringHeader(headers, name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }

    private static long readLongHeader(final Headers headers, final String name, final long fallback) {
        String value = readStringHeader(headers, name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }

    private static int readBinaryOrStringIntHeader(final Headers headers, final String name, final int fallback) {
        if (headers == null) {
            return fallback;
        }
        Header header = headers.lastHeader(name);
        if (header == null || header.value() == null) {
            return fallback;
        }
        byte[] bytes = header.value();
        if (bytes.length == 4) {
            return ByteBuffer.wrap(bytes).getInt();
        }
        return readIntHeader(headers, name, fallback);
    }

    private static long readBinaryOrStringLongHeader(final Headers headers, final String name, final long fallback) {
        if (headers == null) {
            return fallback;
        }
        Header header = headers.lastHeader(name);
        if (header == null || header.value() == null) {
            return fallback;
        }
        byte[] bytes = header.value();
        if (bytes.length == 8) {
            return ByteBuffer.wrap(bytes).getLong();
        }
        return readLongHeader(headers, name, fallback);
    }
}
