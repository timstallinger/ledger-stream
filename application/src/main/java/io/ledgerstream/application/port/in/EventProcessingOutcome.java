package io.ledgerstream.application.port.in;

public enum EventProcessingOutcome {
    PROCESSED,
    DUPLICATE_REJECTED,
    POISON_REJECTED
}
