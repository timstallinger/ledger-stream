package io.ledgerstream.application.port.out;

public interface OutboxEventPublisher {

    void publish(OutboxEvent event);
}
