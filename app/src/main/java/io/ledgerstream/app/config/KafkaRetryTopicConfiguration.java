package io.ledgerstream.app.config;

import io.ledgerstream.adapter.in.messaging.DlqRecordPersister;
import io.ledgerstream.adapter.in.messaging.LedgerConsumerMessage;
import io.ledgerstream.adapter.in.messaging.RawPayloadPreservingAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Configuration
public class KafkaRetryTopicConfiguration extends RetryTopicConfigurationSupport {

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("ledger-scheduler-");
        return scheduler;
    }

    @Override
    protected void configureCustomizers(final CustomizersConfigurer customizersConfigurer) {
        customizersConfigurer.customizeDeadLetterPublishingRecoverer(recoverer ->
                recoverer.addHeadersFunction(this::buildDlqMetadataHeaders)
        );
    }

    private Headers buildDlqMetadataHeaders(final ConsumerRecord<?, ?> record, final Exception exception) {
        RecordHeaders out = new RecordHeaders();
        Throwable root = unwrapListenerException(exception);

        String errorClass = root.getClass().getName();
        String errorMessage = root.getMessage() != null ? root.getMessage() : errorClass;
        Integer schemaId = null;

        if (record.value() instanceof LedgerConsumerMessage message) {
            schemaId = message.schemaId();
            if (message.isDeserializationFailure() && message.deserializationErrorClass() != null) {
                errorClass = message.deserializationErrorClass();
                errorMessage = message.deserializationErrorMessage() != null
                        ? message.deserializationErrorMessage()
                        : errorClass;
            }
        } else if (record.value() instanceof byte[] rawBytes) {
            schemaId = RawPayloadPreservingAvroDeserializer.extractConfluentSchemaId(rawBytes);
        }

        Headers incoming = record.headers();
        String originalTopic = firstNonBlank(
                readStringHeader(incoming, KafkaHeaders.ORIGINAL_TOPIC),
                record.topic()
        );
        int originalPartition = readIntHeader(incoming, KafkaHeaders.ORIGINAL_PARTITION, record.partition());
        long originalOffset = readLongHeader(incoming, KafkaHeaders.ORIGINAL_OFFSET, record.offset());
        int attemptCount = readIntHeader(incoming, RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, 1);
        String correlationId = firstNonBlank(
                readStringHeader(incoming, "X-Correlation-Id"),
                readStringHeader(incoming, DlqRecordPersister.HEADER_CORRELATION_ID),
                ""
        );

        addUtf8(out, DlqRecordPersister.HEADER_ERROR_CLASS, errorClass);
        addUtf8(out, DlqRecordPersister.HEADER_ERROR_MESSAGE, errorMessage);
        addUtf8(out, DlqRecordPersister.HEADER_ORIGINAL_TOPIC, originalTopic);
        addUtf8(out, DlqRecordPersister.HEADER_ORIGINAL_PARTITION, Integer.toString(originalPartition));
        addUtf8(out, DlqRecordPersister.HEADER_ORIGINAL_OFFSET, Long.toString(originalOffset));
        addUtf8(out, DlqRecordPersister.HEADER_ATTEMPT_COUNT, Integer.toString(attemptCount));
        addUtf8(out, DlqRecordPersister.HEADER_FAILED_AT, Instant.now().toString());
        addUtf8(out, DlqRecordPersister.HEADER_CORRELATION_ID, correlationId);
        if (schemaId != null) {
            addUtf8(out, DlqRecordPersister.HEADER_SCHEMA_ID, Integer.toString(schemaId));
        }
        return out;
    }

    private static Throwable unwrapListenerException(final Exception exception) {
        Throwable current = exception;
        while ((current instanceof TimestampedException || current instanceof ListenerExecutionFailedException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current != null ? current : new IllegalStateException("Unknown consumer failure");
    }

    private static void addUtf8(final RecordHeaders target, final String name, final String value) {
        target.add(name, (value != null ? value : "").getBytes(StandardCharsets.UTF_8));
    }

    private static String firstNonBlank(final String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
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
        try {
            return Integer.parseInt(new String(bytes, StandardCharsets.UTF_8).trim());
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }

    private static long readLongHeader(final Headers headers, final String name, final long fallback) {
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
        try {
            return Long.parseLong(new String(bytes, StandardCharsets.UTF_8).trim());
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }
}
