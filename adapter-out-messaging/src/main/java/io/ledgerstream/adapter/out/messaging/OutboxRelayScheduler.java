package io.ledgerstream.adapter.out.messaging;

import io.ledgerstream.application.port.in.RelayOutboxUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Objects;

public final class OutboxRelayScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final RelayOutboxUseCase relayOutboxUseCase;

    public OutboxRelayScheduler(final RelayOutboxUseCase relayOutboxUseCase) {
        this.relayOutboxUseCase = Objects.requireNonNull(relayOutboxUseCase, "relayOutboxUseCase must not be null");
    }

    @Scheduled(fixedDelayString = "${ledger.outbox.relay.fixed-delay-ms:500}")
    public void pollAndRelay() {
        int relayed = relayOutboxUseCase.relayPendingBatch();
        if (relayed > 0) {
            LOG.debug("Relayed {} outbox event(s) to Kafka", relayed);
        }
    }
}
