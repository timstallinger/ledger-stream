package io.ledgerstream.app.config;

import io.ledgerstream.application.port.out.ConsumerEventObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Micrometer metrics observer for the Kafka consumer.
 * Cardinality policy (N4): ONLY closed-set labels (outcome, reason) — never IDs or amounts.
 */
public final class ObservabilityConsumerEventObserver implements ConsumerEventObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ObservabilityConsumerEventObserver.class);

    private final Counter processedOutcomeCounter;
    private final Counter duplicateOutcomeCounter;
    private final Counter poisonOutcomeCounter;
    private final Counter duplicateRejectedCounter;
    private final Counter poisonTotalCounter;
    private final Counter retriesCounter;

    public ObservabilityConsumerEventObserver(final MeterRegistry meterRegistry) {
        this.processedOutcomeCounter = Counter.builder("consumer_processed_total")
                .tag("outcome", "processed")
                .description("Total number of consumer events by closed-set outcome (processed, duplicate, poison)")
                .register(meterRegistry);
        this.duplicateOutcomeCounter = Counter.builder("consumer_processed_total")
                .tag("outcome", "duplicate")
                .description("Total number of consumer events by closed-set outcome (processed, duplicate, poison)")
                .register(meterRegistry);
        this.poisonOutcomeCounter = Counter.builder("consumer_processed_total")
                .tag("outcome", "poison")
                .description("Total number of consumer events by closed-set outcome (processed, duplicate, poison)")
                .register(meterRegistry);
        this.duplicateRejectedCounter = Counter.builder("consumer_duplicates_rejected_total")
                .tag("reason", "duplicate")
                .description("Total number of genuine duplicate consumer events rejected via UNIQUE(event_id)")
                .register(meterRegistry);
        this.poisonTotalCounter = Counter.builder("consumer_poison_total")
                .description("Total number of non-retryable poison events routed directly to DLQ")
                .register(meterRegistry);
        this.retriesCounter = Counter.builder("consumer_retries_total")
                .description("Total number of non-blocking consumer retry attempts executed")
                .register(meterRegistry);
    }

    @Override
    public void onProcessed(final UUID eventId) {
        processedOutcomeCounter.increment();
    }

    @Override
    public void onDuplicateRejected(final UUID eventId) {
        duplicateOutcomeCounter.increment();
        duplicateRejectedCounter.increment();
        LOG.debug("Rejected duplicate event via UNIQUE(event_id): eventId={}", eventId);
    }

    @Override
    public void onPoisonRejected(final String eventIdOrKey, final String reason) {
        poisonOutcomeCounter.increment();
        poisonTotalCounter.increment();
        LOG.warn("Rejected poison event directly to DLQ: reason={}", reason);
    }

    @Override
    public void onRetryAttempt(final String topic, final int attemptNumber, final String errorClass) {
        retriesCounter.increment();
        LOG.warn("Executing consumer retry attempt: topic={} attempt={} errorClass={}", topic, attemptNumber, errorClass);
    }
}
