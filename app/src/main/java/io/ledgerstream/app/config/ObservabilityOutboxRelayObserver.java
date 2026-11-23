package io.ledgerstream.app.config;

import io.ledgerstream.application.port.out.OutboxRelayObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public final class ObservabilityOutboxRelayObserver implements OutboxRelayObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ObservabilityOutboxRelayObserver.class);

    private final Counter failedCounter;

    public ObservabilityOutboxRelayObserver(final MeterRegistry meterRegistry) {
        this.failedCounter = Counter.builder("outbox_events_failed_total")
                .description("Total number of outbox events permanently marked FAILED after exhausting retries")
                .register(meterRegistry);
    }

    @Override
    public void onEventFailedPermanently(
            final UUID eventId,
            final UUID aggregateId,
            final int retryCount,
            final String errorMessage
    ) {
        failedCounter.increment();
        LOG.warn(
                "Outbox event permanently marked FAILED: eventId={}, aggregateId={}, retry_count={}, error={}",
                eventId,
                aggregateId,
                retryCount,
                errorMessage
        );
    }
}
