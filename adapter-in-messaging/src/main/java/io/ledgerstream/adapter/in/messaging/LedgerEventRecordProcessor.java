package io.ledgerstream.adapter.in.messaging;

import io.ledgerstream.application.port.in.EventProcessingOutcome;
import io.ledgerstream.application.port.in.IncomingLedgerEvent;
import io.ledgerstream.application.port.in.ProjectLedgerEventUseCase;
import io.ledgerstream.application.port.out.ConsumerEventObserver;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.SerializationUtils;

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
     * Poison handling (Phase 4 non-blocking contract; DLQ topic publishing added in Phase 5):
     * - Deserialization failures are NEVER retried; they are recorded as poison and acknowledged immediately
     *   so subsequent records in the same partition are never blocked.
     * - Business validation rejections (unknown account, amount <= 0, invalid payload) are recorded as poison
     *   and acknowledged immediately without modifying account_summary.
     */
    public EventProcessingOutcome processRecord(
            final ConsumerRecord<String, Object> record,
            final Runnable acknowledgeAfterCommit
    ) {
        Objects.requireNonNull(record, "record must not be null");
        Objects.requireNonNull(acknowledgeAfterCommit, "acknowledgeAfterCommit must not be null");

        if (hasDeserializationError(record) || !(record.value() instanceof GenericRecord genericRecord)) {
            String keyRef = record.key() != null ? record.key() : "partition-" + record.partition() + "-offset-" + record.offset();
            LOG.warn(
                    "Rejecting undeserializable poison record without retry (DLQ path in Phase 5): topic={}, partition={}, offset={}, key={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key()
            );
            observer.onPoisonRejected(keyRef, "deserialization_error");
            acknowledgeAfterCommit.run();
            return EventProcessingOutcome.POISON_REJECTED;
        }

        Optional<IncomingLedgerEvent> mapped = recordMapper.map(genericRecord);
        if (mapped.isEmpty()) {
            String keyRef = record.key() != null ? record.key() : "partition-" + record.partition() + "-offset-" + record.offset();
            LOG.warn(
                    "Rejecting malformed Avro payload without retry (DLQ path in Phase 5): topic={}, partition={}, offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset()
            );
            observer.onPoisonRejected(keyRef, "malformed_payload");
            acknowledgeAfterCommit.run();
            return EventProcessingOutcome.POISON_REJECTED;
        }

        EventProcessingOutcome outcome = projectLedgerEventUseCase.process(mapped.get());
        acknowledgeAfterCommit.run();
        return outcome;
    }

    private static boolean hasDeserializationError(final ConsumerRecord<String, Object> record) {
        Header valueError = record.headers().lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
        Header keyError = record.headers().lastHeader(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER);
        return valueError != null || keyError != null;
    }
}
