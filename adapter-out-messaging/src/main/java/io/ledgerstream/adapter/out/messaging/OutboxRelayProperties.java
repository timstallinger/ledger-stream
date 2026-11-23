package io.ledgerstream.adapter.out.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ledger.outbox.relay")
public record OutboxRelayProperties(
        boolean enabled,
        int batchSize,
        long fixedDelayMs,
        long publishTimeoutMs
) {

    public OutboxRelayProperties {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("ledger.outbox.relay.batch-size must be positive");
        }
        if (fixedDelayMs <= 0) {
            throw new IllegalArgumentException("ledger.outbox.relay.fixed-delay-ms must be positive");
        }
        if (publishTimeoutMs <= 0) {
            throw new IllegalArgumentException("ledger.outbox.relay.publish-timeout-ms must be positive");
        }
    }
}
