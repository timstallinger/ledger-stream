package io.ledgerstream.loadgen;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Transport-level fault injector (Cohort 3): publishes schema-valid Avro v2 poison events
 * directly to ledger.events (alternating unknown_account and non_positive_amount).
 */
public final class PoisonEventProducer implements AutoCloseable {

    private final KafkaProducer<String, GenericRecord> producer;
    private final Schema avroSchema;
    private final String topic;

    public PoisonEventProducer(
            final String bootstrapServers,
            final String schemaRegistryUrl,
            final String topic
    ) {
        this.topic = topic;
        this.avroSchema = loadSchema();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        this.producer = new KafkaProducer<>(props);
    }

    public void sendPoisonEvent(
            final UUID eventId,
            final UUID aggregateId,
            final UUID bookingId,
            final String amount,
            final String correlationId,
            final String reference
    ) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("eventId", eventId.toString());
        record.put("eventType", "BookingCreated");
        record.put("aggregateId", aggregateId.toString());
        record.put("bookingId", bookingId.toString());
        record.put("originalBookingId", null);
        record.put("bookingType", "CREDIT");
        record.put("amount", amount);
        record.put("currency", "EUR");
        record.put("reference", reference);
        record.put("version", 1L);
        record.put("occurredAt", Instant.now().toString());
        record.put("schemaVersion", 2);
        record.put("initiationChannel", "LOADGEN_POISON");

        ProducerRecord<String, GenericRecord> kafkaRecord = new ProducerRecord<>(
                topic,
                aggregateId.toString(),
                record
        );
        kafkaRecord.headers().add(new RecordHeader("eventId", eventId.toString().getBytes(StandardCharsets.UTF_8)));
        kafkaRecord.headers().add(new RecordHeader("eventType", "BookingCreated".getBytes(StandardCharsets.UTF_8)));
        kafkaRecord.headers().add(new RecordHeader("schemaVersion", "2".getBytes(StandardCharsets.UTF_8)));
        kafkaRecord.headers().add(new RecordHeader("X-Correlation-Id", correlationId.getBytes(StandardCharsets.UTF_8)));

        try {
            producer.send(kafkaRecord).get(15, TimeUnit.SECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to publish poison event " + eventId + " to " + topic, exception);
        }
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private static Schema loadSchema() {
        try (InputStream stream = PoisonEventProducer.class.getResourceAsStream("/avro/LedgerBookingEvent.avsc")) {
            if (stream == null) {
                throw new IllegalStateException("Missing classpath resource /avro/LedgerBookingEvent.avsc");
            }
            return new Schema.Parser().parse(stream);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to load LedgerBookingEvent.avsc", exception);
        }
    }
}
