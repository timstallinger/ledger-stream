package io.ledgerstream.adapter.in.messaging;

import io.ledgerstream.application.port.in.EventProcessingOutcome;
import io.ledgerstream.application.port.in.IncomingLedgerEvent;
import io.ledgerstream.application.port.in.NonRetryableEventException;
import io.ledgerstream.application.port.in.ProjectLedgerEventUseCase;
import io.ledgerstream.application.port.out.ConsumerEventObserver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

public final class LedgerEventRecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LedgerEventRecordProcessor.class);
    private static final String HEADER_CORRELATION_ID = "X-Correlation-Id";
    private static final String MDC_CORRELATION_ID = "correlationId";
    private static final String MDC_EVENT_ID = "eventId";
    private static final Pattern VALID_CORRELATION_ID = Pattern.compile("^[A-Za-z0-9._-]{8,64}$");

    private final ProjectLedgerEventUseCase projectLedgerEventUseCase;
    private final LedgerEventRecordMapper recordMapper;
    private final ConsumerEventObserver observer;

    public LedgerEventRecordProcessor(
            final ProjectLedgerEventUseCase projectLedgerEventUseCase,
            final LedgerEventRecordMapper recordMapper,
            final ConsumerEventObserver observer
    ) {
        this.projectLedgerEventUseCase = Objects.requireNonNull(
                projectLedgerEventUseCase,
                "projectLedgerEventUseCase must not be null"
        );
        this.recordMapper = Objects.requireNonNull(recordMapper, "recordMapper must not be null");
        this.observer = Objects.requireNonNull(observer, "observer must not be null");
    }

    /**
     * Processes one Kafka record with strict manual offset acknowledgment AFTER database commit.
     *
     * Sets correlationId and eventId in MDC for structured logging and ALWAYS clears MDC in finally.
     */
    public EventProcessingOutcome processRecord(
            final ConsumerRecord<String, LedgerConsumerMessage> record,
            final Runnable acknowledgeAfterCommit
    ) {
        Objects.requireNonNull(record, "record must not be null");
        Objects.requireNonNull(acknowledgeAfterCommit, "acknowledgeAfterCommit must not be null");

        String correlationId = resolveCorrelationId(record);
        MDC.put(MDC_CORRELATION_ID, correlationId);
        try {
            LedgerConsumerMessage message = record.value();
            String keyRef = record.key() != null
                    ? record.key()
                    : "partition-" + record.partition() + "-offset-" + record.offset();

            if (message == null || message.isDeserializationFailure()) {
                String errorMsg = message != null && message.deserializationErrorMessage() != null
                        ? message.deserializationErrorMessage()
                        : "Null or undeserializable record value";
                LOG.warn(
                        "Deserialization failure routed directly to DLQ without retry: stage=consumer topic={} partition={} offset={}",
                        record.topic(),
                        record.partition(),
                        record.offset()
                );
                observer.onPoisonRejected(keyRef, "deserialization_error");
                throw new NonRetryableEventException("deserialization_error", errorMsg);
            }

            Optional<IncomingLedgerEvent> mapped = recordMapper.map(message.record());
            if (mapped.isEmpty()) {
                LOG.warn(
                        "Malformed Avro payload routed directly to DLQ without retry: stage=consumer topic={} partition={} offset={}",
                        record.topic(),
                        record.partition(),
                        record.offset()
                );
                observer.onPoisonRejected(keyRef, "malformed_payload");
                throw new NonRetryableEventException("malformed_payload", "Malformed Avro payload fields");
            }

            IncomingLedgerEvent incoming = mapped.get();
            if (incoming.eventId() != null) {
                MDC.put(MDC_EVENT_ID, incoming.eventId().toString());
            }

            EventProcessingOutcome outcome = projectLedgerEventUseCase.process(incoming);
            acknowledgeAfterCommit.run();
            LOG.info(
                    "Projected ledger event in consumer: stage=consumer eventType={} outcome={} topic={} partition={} offset={}",
                    incoming.eventType(),
                    outcome,
                    record.topic(),
                    record.partition(),
                    record.offset()
            );
            return outcome;
        } finally {
            MDC.clear();
        }
    }

    private static String resolveCorrelationId(final ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(HEADER_CORRELATION_ID);
        if (header == null || header.value() == null) {
            return UUID.randomUUID().toString();
        }
        String raw = new String(header.value(), StandardCharsets.UTF_8);
        if (VALID_CORRELATION_ID.matcher(raw).matches()) {
            return raw;
        }
        String fresh = UUID.randomUUID().toString();
        MDC.put(MDC_CORRELATION_ID, fresh);
        LOG.warn(
                "Invalid Kafka X-Correlation-Id header rejected; generated fresh correlationId: correlationIdRejected=true topic={} partition={} offset={}",
                record.topic(),
                record.partition(),
                record.offset()
        );
        return fresh;
    }
}
