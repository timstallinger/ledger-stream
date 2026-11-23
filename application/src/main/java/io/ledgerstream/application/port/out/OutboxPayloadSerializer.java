package io.ledgerstream.application.port.out;

import io.ledgerstream.domain.event.DomainEvent;

public interface OutboxPayloadSerializer {

    String serialize(DomainEvent event);
}
