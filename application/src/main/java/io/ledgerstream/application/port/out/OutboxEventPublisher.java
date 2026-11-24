package io.ledgerstream.application.port.out;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface OutboxEventPublisher {

    void publish(OutboxEvent event);

    /**
     * Publishes a wave of events (distinct aggregates) and returns a map of failed event IDs to their causes.
     * Implementations may pipeline broker sends in parallel while preserving per-event MDC logging.
     */
    default Map<UUID, RuntimeException> publishBatch(final List<OutboxEvent> events) {
        Map<UUID, RuntimeException> failures = new LinkedHashMap<>();
        for (OutboxEvent event : events) {
            try {
                publish(event);
            } catch (RuntimeException exception) {
                failures.put(event.id(), exception);
            }
        }
        return failures;
    }
}
