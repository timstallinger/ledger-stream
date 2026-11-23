package io.ledgerstream.adapter.out.messaging;

import io.ledgerstream.application.port.out.RawEventPublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class KafkaRawEventPublisher implements RawEventPublisher {

    private final KafkaTemplate<String, byte[]> rawBytesKafkaTemplate;
    private final long publishTimeoutMs;

    public KafkaRawEventPublisher(
            final KafkaTemplate<String, byte[]> rawBytesKafkaTemplate,
            final long publishTimeoutMs
    ) {
        this.rawBytesKafkaTemplate = Objects.requireNonNull(
                rawBytesKafkaTemplate,
                "rawBytesKafkaTemplate must not be null"
        );
        if (publishTimeoutMs <= 0) {
            throw new IllegalArgumentException("publishTimeoutMs must be positive");
        }
        this.publishTimeoutMs = publishTimeoutMs;
    }

    @Override
    public void publishRaw(
            final String topic,
            final String key,
            final byte[] payload,
            final Map<String, String> headers
    ) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(payload, "payload must not be null");

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, payload);
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    record.headers().add(new RecordHeader(
                            entry.getKey(),
                            entry.getValue().getBytes(StandardCharsets.UTF_8)
                    ));
                }
            }
        }

        try {
            rawBytesKafkaTemplate.send(record).get(publishTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while replaying raw event to topic " + topic, exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Failed to replay raw event to topic " + topic, exception);
        }
    }
}
