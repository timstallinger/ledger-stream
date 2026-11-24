package io.ledgerstream.app.config;

import io.ledgerstream.application.port.out.OutboxRelayObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Micrometer metrics observer for the Outbox Relay.
 * Cardinality policy (N4): ONLY closed-set labels (type, result) — never IDs or amounts.
 */
public final class ObservabilityOutboxRelayObserver implements OutboxRelayObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ObservabilityOutboxRelayObserver.class);

    private final Counter failedCounter;
    private final Counter relayedCreatedSuccess;
    private final Counter relayedCancelledSuccess;
    private final Counter relayedOtherSuccess;
    private final Counter relayedFailure;

    public ObservabilityOutboxRelayObserver(final MeterRegistry meterRegistry) {
        this.failedCounter = Counter.builder("outbox_events_failed_total")
                .description("Total number of outbox events permanently marked FAILED after exhausting retries")
                .register(meterRegistry);
        this.relayedCreatedSuccess = Counter.builder("outbox_events_relayed_total")
                .tag("type", "BookingCreated")
                .tag("result", "success")
                .description("Total number of outbox relay attempts by closed-set event type and result")
                .register(meterRegistry);
        this.relayedCancelledSuccess = Counter.builder("outbox_events_relayed_total")
                .tag("type", "BookingCancelled")
                .tag("result", "success")
                .description("Total number of outbox relay attempts by closed-set event type and result")
                .register(meterRegistry);
        this.relayedOtherSuccess = Counter.builder("outbox_events_relayed_total")
                .tag("type", "other")
                .tag("result", "success")
                .description("Total number of outbox relay attempts by closed-set event type and result")
                .register(meterRegistry);
        this.relayedFailure = Counter.builder("outbox_events_relayed_total")
                .tag("type", "other")
                .tag("result", "failure")
                .description("Total number of outbox relay attempts by closed-set event type and result")
                .register(meterRegistry);
    }

    @Override
    public void onEventRelayed(final String eventType, final boolean success) {
        if (!success) {
            relayedFailure.increment();
            return;
        }
        if ("BookingCreated".equals(eventType)) {
            relayedCreatedSuccess.increment();
        } else if ("BookingCancelled".equals(eventType)) {
            relayedCancelledSuccess.increment();
        } else {
            relayedOtherSuccess.increment();
        }
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
                "Outbox event permanently marked FAILED: eventId={} retryCount={} error={}",
                eventId,
                retryCount,
                errorMessage
        );
    }
}
