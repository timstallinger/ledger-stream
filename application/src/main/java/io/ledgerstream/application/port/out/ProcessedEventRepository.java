package io.ledgerstream.application.port.out;

import java.time.Instant;
import java.util.UUID;

public interface ProcessedEventRepository {

    boolean tryMarkProcessed(UUID eventId, String eventType, Instant processedAt);
}
