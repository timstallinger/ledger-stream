package io.ledgerstream.adapter.in.messaging;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

public final class RawPayloadPreservingAvroDeserializer implements Deserializer<LedgerConsumerMessage> {

    public static final String HEADER_ORIGINAL_PAYLOAD = "x-original-payload";
    public static final String HEADER_EXCEPTION_CLASS = "x-exception-class";
    public static final String HEADER_EXCEPTION_MESSAGE = "x-exception-message";

    private static final byte CONFLUENT_MAGIC_BYTE = 0x00;

    private final Deserializer<Object> delegate;

    public RawPayloadPreservingAvroDeserializer(final Deserializer<Object> delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate deserializer must not be null");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public LedgerConsumerMessage deserialize(final String topic, final byte[] data) {
        return deserialize(topic, null, data);
    }

    @Override
    public LedgerConsumerMessage deserialize(final String topic, final Headers headers, final byte[] data) {
        byte[] rawBytes = data != null ? data : new byte[0];
        Integer schemaId = extractConfluentSchemaId(rawBytes);

        try {
            Object deserialized = delegate.deserialize(topic, headers, data);
            if (deserialized instanceof GenericRecord genericRecord) {
                return LedgerConsumerMessage.valid(genericRecord, rawBytes, schemaId);
            }
            String message = "Expected Avro GenericRecord but got "
                    + (deserialized == null ? "null" : deserialized.getClass().getName());
            recordFailureHeaders(headers, rawBytes, IllegalArgumentException.class.getName(), message);
            return LedgerConsumerMessage.deserializationFailed(
                    rawBytes,
                    schemaId,
                    IllegalArgumentException.class.getName(),
                    message
            );
        } catch (RuntimeException exception) {
            String errorClass = exception.getClass().getName();
            String errorMessage = exception.getMessage() != null ? exception.getMessage() : errorClass;
            recordFailureHeaders(headers, rawBytes, errorClass, errorMessage);
            return LedgerConsumerMessage.deserializationFailed(rawBytes, schemaId, errorClass, errorMessage);
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

    public static Integer extractConfluentSchemaId(final byte[] data) {
        if (data == null || data.length < 5 || data[0] != CONFLUENT_MAGIC_BYTE) {
            return null;
        }
        return ByteBuffer.wrap(data, 1, 4).getInt();
    }

    private static void recordFailureHeaders(
            final Headers headers,
            final byte[] rawBytes,
            final String errorClass,
            final String errorMessage
    ) {
        if (headers == null) {
            return;
        }
        String base64Payload = Base64.getEncoder().encodeToString(rawBytes);
        headers.remove(HEADER_ORIGINAL_PAYLOAD);
        headers.add(HEADER_ORIGINAL_PAYLOAD, base64Payload.getBytes(StandardCharsets.UTF_8));
        headers.remove(HEADER_EXCEPTION_CLASS);
        headers.add(HEADER_EXCEPTION_CLASS, errorClass.getBytes(StandardCharsets.UTF_8));
        headers.remove(HEADER_EXCEPTION_MESSAGE);
        headers.add(HEADER_EXCEPTION_MESSAGE, errorMessage.getBytes(StandardCharsets.UTF_8));
    }
}
