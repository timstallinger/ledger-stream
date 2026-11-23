package io.ledgerstream.adapter.in.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ledger.kafka.consumer")
public record KafkaConsumerProperties(
        boolean enabled,
        String groupId,
        int concurrency
) {

    public KafkaConsumerProperties {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("ledger.kafka.consumer.group-id must not be blank");
        }
        if (concurrency <= 0) {
            throw new IllegalArgumentException("ledger.kafka.consumer.concurrency must be positive");
        }
    }
}
