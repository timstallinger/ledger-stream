package io.ledgerstream.app.config;

import io.ledgerstream.application.port.out.DlqObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public final class ObservabilityDlqObserver implements DlqObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ObservabilityDlqObserver.class);

    private final Counter replayedCounter;

    public ObservabilityDlqObserver(final MeterRegistry meterRegistry) {
        this.replayedCounter = Counter.builder("dlq_replayed_total")
                .description("Total number of DLQ entries replayed back to their original topic")
                .register(meterRegistry);
    }

    @Override
    public void onDlqReplayed(final UUID dlqEntryId, final String topic, final String key) {
        replayedCounter.increment();
        LOG.info("Replayed DLQ entry: id={}, topic={}, key={}", dlqEntryId, topic, key);
    }
}
