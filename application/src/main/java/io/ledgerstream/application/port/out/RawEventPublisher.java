package io.ledgerstream.application.port.out;

import java.util.Map;

public interface RawEventPublisher {

    void publishRaw(String topic, String key, byte[] payload, Map<String, String> headers);
}
