package io.ledgerstream.application.port.in;

public interface ProjectLedgerEventUseCase {

    EventProcessingOutcome process(IncomingLedgerEvent event);
}
