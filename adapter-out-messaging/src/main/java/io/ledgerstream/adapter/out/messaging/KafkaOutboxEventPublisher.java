package io.ledgerstream.adapter.out.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.adapter.out.messaging.avro.LedgerBookingEvent;
import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxEventPublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class KafkaOutboxEventPublisher implements OutboxEventPublisher {

    private final KafkaTemplate<String, LedgerBookingEvent> kafkaTemplate;
    private final KafkaTopicProperties topicProperties;
    private final ObjectMapper objectMapper;
    private final long publishTimeoutMs;

    public KafkaOutboxEventPublisher(
            final KafkaTemplate<String, LedgerBookingEvent> kafkaTemplate,
            final KafkaTopicProperties topicProperties,
            final ObjectMapper objectMapper,
            final long publishTimeoutMs
    ) {
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate must not be null");
        this.topicProperties = Objects.requireNonNull(topicProperties, "topicProperties must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        if (publishTimeoutMs <= 0) {
            throw new IllegalArgumentException("publishTimeoutMs must be positive");
        }
        this.publishTimeoutMs = publishTimeoutMs;
    }

    @Override
    public void publish(final OutboxEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        String accountPartitionKey = Objects.requireNonNull(
                event.aggregateId(),
                "aggregateId (accountId) must not be null for partition ordering"
        ).toString();
        LedgerBookingEvent avroEvent = toAvroRecord(event);
        ProducerRecord<String, LedgerBookingEvent> record = new ProducerRecord<>(
                topicProperties.ledgerEvents(),
                accountPartitionKey,
                avroEvent
        );
        addHeader(record, "eventId", event.id().toString());
        addHeader(record, "eventType", event.eventType());
        addHeader(record, "schemaVersion", Integer.toString(event.schemaVersion()));
        if (event.correlationId() != null && !event.correlationId().isBlank()) {
            addHeader(record, "X-Correlation-Id", event.correlationId());
        }

        try {
            kafkaTemplate.send(record).get(publishTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing outbox event " + event.id(), exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Failed to publish outbox event " + event.id() + " to Kafka", exception);
        }
    }

    private LedgerBookingEvent toAvroRecord(final OutboxEvent event) {
        try {
            JsonNode root = objectMapper.readTree(event.payload());
            JsonNode originalNode = root.get("originalBookingId");
            String originalBookingId = (originalNode == null || originalNode.isNull()) ? null : originalNode.asText();
            JsonNode channelNode = root.get("initiationChannel");
            String initiationChannel = (channelNode == null || channelNode.isNull() || channelNode.asText().isBlank())
                    ? "API"
                    : channelNode.asText();
            return LedgerBookingEvent.newBuilder()
                    .setEventId(root.get("eventId").asText())
                    .setEventType(root.get("eventType").asText())
                    .setAggregateId(root.get("aggregateId").asText())
                    .setBookingId(root.get("bookingId").asText())
                    .setOriginalBookingId(originalBookingId)
                    .setBookingType(root.get("bookingType").asText())
                    .setAmount(root.get("amount").asText())
                    .setCurrency(root.get("currency").asText())
                    .setReference(root.get("reference").asText())
                    .setVersion(root.get("version").asLong())
                    .setOccurredAt(root.get("occurredAt").asText())
                    .setSchemaVersion(root.get("schemaVersion").asInt())
                    .setInitiationChannel(initiationChannel)
                    .build();
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to map outbox event " + event.id() + " to Avro record", exception);
        }
    }

    private static void addHeader(
            final ProducerRecord<String, LedgerBookingEvent> record,
            final String key,
            final String value
    ) {
        record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }
}
