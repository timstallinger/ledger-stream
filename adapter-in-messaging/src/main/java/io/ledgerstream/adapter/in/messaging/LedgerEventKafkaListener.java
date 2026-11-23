package io.ledgerstream.adapter.in.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Objects;

public final class LedgerEventKafkaListener {

    private final LedgerEventRecordProcessor recordProcessor;

    public LedgerEventKafkaListener(final LedgerEventRecordProcessor recordProcessor) {
        this.recordProcessor = Objects.requireNonNull(recordProcessor, "recordProcessor must not be null");
    }

    @KafkaListener(
            id = "ledger-projection-listener",
            topics = "${ledger.kafka.topics.ledger-events}",
            groupId = "${ledger.kafka.consumer.group-id}",
            containerFactory = "ledgerKafkaListenerContainerFactory"
    )
    public void onMessage(final ConsumerRecord<String, Object> record, final Acknowledgment acknowledgment) {
        recordProcessor.processRecord(record, acknowledgment::acknowledge);
    }
}
