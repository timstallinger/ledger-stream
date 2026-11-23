package io.ledgerstream.adapter.out.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ledger.kafka.topics")
public record KafkaTopicProperties(
        String ledgerEvents,
        String ledgerEventsDlq
) {

    public KafkaTopicProperties {
        if (ledgerEvents == null || ledgerEvents.isBlank()) {
            throw new IllegalArgumentException("ledger.kafka.topics.ledger-events must not be blank");
        }
        if (ledgerEventsDlq == null || ledgerEventsDlq.isBlank()) {
            throw new IllegalArgumentException("ledger.kafka.topics.ledger-events-dlq must not be blank");
        }
    }
}
