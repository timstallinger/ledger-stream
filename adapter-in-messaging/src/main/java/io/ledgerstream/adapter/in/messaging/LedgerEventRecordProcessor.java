package io.ledgerstream.adapter.in.messaging;

import io.ledgerstream.application.port.in.EventProcessingOutcome;
import io.ledgerstream.application.port.in.IncomingLedgerEvent;
import io.ledgerstream.application.port.in.NonRetryableEventException;
import io.ledgerstream.application.port.in.ProjectLedgerEventUseCase;
import io.ledgerstream.application.port.out.ConsumerEventObserver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

public final class LedgerEventRecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LedgerEventRecordProcessor.class);

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
     * Error classification:
     * - Deserialization failures and malformed payloads throw NonRetryableEventException (0 retries -> direct DLQ)
     *   while RawPayloadPreservingAvroDeserializer keeps the exact original bytes in payload and headers.
     * - Business validation errors (unknown account, amount <= 0, invalid type) throw NonRetryableEventException
     *   (0 retries -> direct DLQ).
     * - Technical errors propagate to @RetryableTopic (non-blocking retry topics, up to 5 attempts, then DLQ).
     */
    public EventProcessingOutcome processRecord(
            final ConsumerRecord<String, LedgerConsumerMessage> record,
            final Runnable acknowledgeAfterCommit
    ) {
        Objects.requireNonNull(record, "record must not be null");
        Objects.requireNonNull(acknowledgeAfterCommit, "acknowledgeAfterCommit must not be null");

        LedgerConsumerMessage message = record.value();
        String keyRef = record.key() != null
                ? record.key()
                : "partition-" + record.partition() + "-offset-" + record.offset();

        if (message == null || message.isDeserializationFailure()) {
            String errorMsg = message != null && message.deserializationErrorMessage() != null
                    ? message.deserializationErrorMessage()
                    : "Null or undeserializable record value";
            LOG.warn(
                    "Deserialization failure routed directly to DLQ without retry: topic={}, partition={}, offset={}, key={}, error={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    errorMsg
            );
            observer.onPoisonRejected(keyRef, "deserialization_error");
            throw new NonRetryableEventException("deserialization_error", errorMsg);
        }

        Optional<IncomingLedgerEvent> mapped = recordMapper.map(message.record());
        if (mapped.isEmpty()) {
            LOG.warn(
                    "Malformed Avro payload routed directly to DLQ without retry: topic={}, partition={}, offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset()
            );
            observer.onPoisonRejected(keyRef, "malformed_payload");
            throw new NonRetryableEventException("malformed_payload", "Malformed Avro payload fields");
        }

        EventProcessingOutcome outcome = projectLedgerEventUseCase.process(mapped.get());
        acknowledgeAfterCommit.run();
        return outcome;
    }
}
