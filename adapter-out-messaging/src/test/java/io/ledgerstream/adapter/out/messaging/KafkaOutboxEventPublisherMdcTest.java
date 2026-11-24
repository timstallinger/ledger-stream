package io.ledgerstream.adapter.out.messaging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.adapter.out.messaging.avro.LedgerBookingEvent;
import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxStatus;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies L5: on a reused @Scheduled thread, KafkaOutboxEventPublisher isolates MDC per event
 * and clears/restores correlationId and eventId in finally so no correlationId ever leaks across events.
 */
final class KafkaOutboxEventPublisherMdcTest {

    @Test
    @SuppressWarnings("unchecked")
    void isolatesAndClearsMdcAcrossSequentialEventsOnSameThread() {
        KafkaTemplate<String, LedgerBookingEvent> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        KafkaOutboxEventPublisher publisher = new KafkaOutboxEventPublisher(
                kafkaTemplate,
                new KafkaTopicProperties("ledger.events", "ledger.events.dlq"),
                new ObjectMapper(),
                5000L
        );

        Logger publisherLogger = (Logger) LoggerFactory.getLogger(KafkaOutboxEventPublisher.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        publisherLogger.addAppender(appender);

        MDC.clear();
        try {
            OutboxEvent first = sampleEvent(UUID.randomUUID(), "corr-relay-first-0001");
            OutboxEvent second = sampleEvent(UUID.randomUUID(), "corr-relay-second-0002");
            OutboxEvent withoutCorrelation = sampleEvent(UUID.randomUUID(), null);

            publisher.publish(first);
            assertThat(MDC.get("correlationId")).isNull();
            assertThat(MDC.get("eventId")).isNull();

            publisher.publish(second);
            assertThat(MDC.get("correlationId")).isNull();
            assertThat(MDC.get("eventId")).isNull();

            // Event without correlationId on the same thread must NOT inherit a previous correlationId
            MDC.put("correlationId", "stale-thread-cid");
            publisher.publish(withoutCorrelation);
            assertThat(MDC.get("correlationId")).isEqualTo("stale-thread-cid");
            assertThat(MDC.get("eventId")).isNull();
            MDC.clear();

            List<ILoggingEvent> relayLogs = appender.list.stream()
                    .filter(ev -> ev.getFormattedMessage().contains("stage=relay"))
                    .toList();
            assertThat(relayLogs).hasSize(3);

            assertThat(relayLogs.get(0).getMDCPropertyMap().get("correlationId")).isEqualTo("corr-relay-first-0001");
            assertThat(relayLogs.get(0).getMDCPropertyMap().get("eventId")).isEqualTo(first.id().toString());

            assertThat(relayLogs.get(1).getMDCPropertyMap().get("correlationId")).isEqualTo("corr-relay-second-0002");
            assertThat(relayLogs.get(1).getMDCPropertyMap().get("eventId")).isEqualTo(second.id().toString());
            assertThat(relayLogs.get(1).getMDCPropertyMap().get("correlationId"))
                    .isNotEqualTo(relayLogs.get(0).getMDCPropertyMap().get("correlationId"));

            assertThat(relayLogs.get(2).getMDCPropertyMap().get("correlationId")).isNull();
            assertThat(relayLogs.get(2).getMDCPropertyMap().get("eventId")).isEqualTo(withoutCorrelation.id().toString());
        } finally {
            publisherLogger.detachAppender(appender);
            appender.stop();
            MDC.clear();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void clearsMdcEvenWhenKafkaPublishFails() {
        KafkaTemplate<String, LedgerBookingEvent> kafkaTemplate = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, LedgerBookingEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("Simulated broker failure"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        KafkaOutboxEventPublisher publisher = new KafkaOutboxEventPublisher(
                kafkaTemplate,
                new KafkaTopicProperties("ledger.events", "ledger.events.dlq"),
                new ObjectMapper(),
                5000L
        );

        MDC.clear();
        OutboxEvent failingEvent = sampleEvent(UUID.randomUUID(), "corr-relay-failing-0003");
        assertThatThrownBy(() -> publisher.publish(failingEvent))
                .isInstanceOf(IllegalStateException.class);

        assertThat(MDC.get("correlationId")).isNull();
        assertThat(MDC.get("eventId")).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishBatchIsolatesAndClearsMdcForEveryEventInWave() {
        KafkaTemplate<String, LedgerBookingEvent> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        KafkaOutboxEventPublisher publisher = new KafkaOutboxEventPublisher(
                kafkaTemplate,
                new KafkaTopicProperties("ledger.events", "ledger.events.dlq"),
                new ObjectMapper(),
                5000L
        );

        Logger publisherLogger = (Logger) LoggerFactory.getLogger(KafkaOutboxEventPublisher.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        publisherLogger.addAppender(appender);

        MDC.clear();
        try {
            OutboxEvent first = sampleEvent(UUID.randomUUID(), "corr-batch-first-0001");
            OutboxEvent second = sampleEvent(UUID.randomUUID(), "corr-batch-second-0002");

            assertThat(publisher.publishBatch(List.of(first, second))).isEmpty();
            assertThat(MDC.get("correlationId")).isNull();
            assertThat(MDC.get("eventId")).isNull();

            List<ILoggingEvent> relayLogs = appender.list.stream()
                    .filter(ev -> ev.getFormattedMessage().contains("stage=relay"))
                    .toList();
            assertThat(relayLogs).hasSize(2);
            assertThat(relayLogs.get(0).getMDCPropertyMap().get("correlationId")).isEqualTo("corr-batch-first-0001");
            assertThat(relayLogs.get(1).getMDCPropertyMap().get("correlationId")).isEqualTo("corr-batch-second-0002");
        } finally {
            publisherLogger.detachAppender(appender);
            appender.stop();
            MDC.clear();
        }
    }

    private static OutboxEvent sampleEvent(final UUID eventId, final String correlationId) {
        UUID accountId = UUID.randomUUID();
        UUID bookingId = UUID.randomUUID();
        Instant now = Instant.parse("2026-11-24T10:00:00Z");
        String payload = """
                {
                  "eventId": "%s",
                  "eventType": "BookingCreated",
                  "aggregateId": "%s",
                  "bookingId": "%s",
                  "originalBookingId": null,
                  "bookingType": "CREDIT",
                  "amount": "25.00",
                  "currency": "EUR",
                  "reference": "MDC test",
                  "version": 1,
                  "occurredAt": "%s",
                  "schemaVersion": 2,
                  "initiationChannel": "API"
                }
                """.formatted(eventId, accountId, bookingId, now);
        return new OutboxEvent(
                eventId,
                OutboxEvent.AGGREGATE_TYPE_ACCOUNT,
                accountId,
                "BookingCreated",
                2,
                payload,
                correlationId,
                OutboxStatus.PENDING,
                now,
                now,
                null,
                0,
                null,
                now
        );
    }
}
