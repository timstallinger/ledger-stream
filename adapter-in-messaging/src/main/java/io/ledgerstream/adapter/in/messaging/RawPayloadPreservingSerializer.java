package io.ledgerstream.adapter.in.messaging;

import org.apache.kafka.common.serialization.Serializer;

public final class RawPayloadPreservingSerializer implements Serializer<Object> {

    @Override
    public byte[] serialize(final String topic, final Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof LedgerConsumerMessage message) {
            return message.rawPayload();
        }
        if (data instanceof byte[] bytes) {
            return bytes;
        }
        throw new IllegalArgumentException(
                "RawPayloadPreservingSerializer only supports LedgerConsumerMessage or byte[], got: "
                        + data.getClass().getName()
        );
    }
}
