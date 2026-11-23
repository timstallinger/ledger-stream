package io.ledgerstream.app.config;

import io.ledgerstream.application.port.out.ConsumerEventObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public final class ObservabilityConsumerEventObserver implements ConsumerEventObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ObservabilityConsumerEventObserver.class);

    private final Counter processedCounter;
    private final Counter duplicateRejectedCounter;
    private final Counter poisonRejectedCounter;
    private final Counter retriesCounter;

    public ObservabilityConsumerEventObserver(final MeterRegistry meterRegistry) {
        this.processedCounter = Counter.builder("consumer_processed_total")
                .description("Total number of ledger events projected into account_summary")
                .register(meterRegistry);
        this.duplicateRejectedCounter = Counter.builder("consumer_duplicates_rejected_total")
                .tag("reason", "duplicate")
                .description("Total number of rejected consumer events by reason (duplicate or poison)")
                .register(meterRegistry);
        this.poisonRejectedCounter = Counter.builder("consumer_duplicates_rejected_total")
                .tag("reason", "poison")
                .description("Total number of rejected consumer events by reason (duplicate or poison)")
                .register(meterRegistry);
        this.retriesCounter = Counter.builder("consumer_retries_total")
                .description("Total number of non-blocking consumer retry attempts executed")
                .register(meterRegistry);
    }

    @Override
    public void onProcessed(final UUID eventId) {
        processedCounter.increment();
    }

    @Override
    public void onDuplicateRejected(final UUID eventId) {
        duplicateRejectedCounter.increment();
        LOG.debug("Rejected duplicate event via UNIQUE(event_id): eventId={}", eventId);
    }

    @Override
    public void onPoisonRejected(final String eventIdOrKey, final String reason) {
        poisonRejectedCounter.increment();
        LOG.warn("Rejected poison event directly to DLQ: ref={}, reason={}", eventIdOrKey, reason);
    }

    @Override
    public void onRetryAttempt(final String topic, final int attemptNumber, final String errorClass) {
        retriesCounter.increment();
        LOG.warn("Executing consumer retry attempt: topic={}, attempt={}, category={}", topic, attemptNumber, errorClass);
    }
}
