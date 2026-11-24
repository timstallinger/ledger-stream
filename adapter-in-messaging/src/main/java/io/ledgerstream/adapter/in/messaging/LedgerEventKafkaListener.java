package io.ledgerstream.adapter.in.messaging;

import io.ledgerstream.application.port.in.NonRetryableEventException;
import io.ledgerstream.application.port.out.ConsumerEventObserver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class LedgerEventKafkaListener {

    private final LedgerEventRecordProcessor recordProcessor;
    private final DlqRecordPersister dlqRecordPersister;
    private final ConsumerEventObserver consumerEventObserver;

    public LedgerEventKafkaListener(
            final LedgerEventRecordProcessor recordProcessor,
            final DlqRecordPersister dlqRecordPersister,
            final ConsumerEventObserver consumerEventObserver
    ) {
        this.recordProcessor = Objects.requireNonNull(recordProcessor, "recordProcessor must not be null");
        this.dlqRecordPersister = Objects.requireNonNull(dlqRecordPersister, "dlqRecordPersister must not be null");
        this.consumerEventObserver = Objects.requireNonNull(
                consumerEventObserver,
                "consumerEventObserver must not be null"
        );
    }

    @RetryableTopic(
            attempts = "${ledger.kafka.retry.max-attempts:5}",
            backoff = @Backoff(
                    delayExpression = "${ledger.kafka.retry.initial-delay-ms:1000}",
                    multiplier = 2.0,
                    maxDelayExpression = "${ledger.kafka.retry.max-delay-ms:30000}",
                    random = true
            ),
            dltTopicSuffix = ".dlq",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            exclude = {NonRetryableEventException.class},
            kafkaTemplate = "rawBytesRetryKafkaTemplate",
            listenerContainerFactory = "ledgerKafkaListenerContainerFactory",
            autoCreateTopics = "true",
            numPartitions = "6",
            replicationFactor = "1"
    )
    @KafkaListener(
            id = "ledger-projection-listener",
            topics = "${ledger.kafka.topics.ledger-events}",
            groupId = "${ledger.kafka.consumer.group-id}",
            containerFactory = "ledgerKafkaListenerContainerFactory"
    )
    public void onMessage(
            final ConsumerRecord<String, LedgerConsumerMessage> record,
            final Acknowledgment acknowledgment
    ) {
        int attempt = resolveAttemptNumber(record);
        if (attempt > 1) {
            consumerEventObserver.onRetryAttempt(record.topic(), attempt, "retry");
        }
        recordProcessor.processRecord(record, acknowledgment::acknowledge);
    }

    @DltHandler
    public void onDltMessage(
            final ConsumerRecord<String, LedgerConsumerMessage> record,
            final Acknowledgment acknowledgment
    ) {
        dlqRecordPersister.persist(record);
        acknowledgment.acknowledge();
    }

    private static int resolveAttemptNumber(final ConsumerRecord<String, LedgerConsumerMessage> record) {
        Header header = record.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
        if (header == null || header.value() == null) {
            return 1;
        }
        if (header.value().length == 4) {
            return ByteBuffer.wrap(header.value()).getInt();
        }
        return 1;
    }
}
