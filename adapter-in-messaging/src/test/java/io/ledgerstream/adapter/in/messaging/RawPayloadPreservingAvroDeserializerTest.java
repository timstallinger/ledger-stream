package io.ledgerstream.adapter.in.messaging;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

final class RawPayloadPreservingAvroDeserializerTest {

    @Test
    void preservesExactRawBytesInBase64HeaderAndMessageOnDeserializationFailure() {
        byte[] corruptBytes = new byte[]{0x00, 0x00, 0x00, 0x00, 0x2a, (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
        Deserializer<Object> failingDelegate = new Deserializer<>() {
            @Override
            public Object deserialize(final String topic, final byte[] data) {
                throw new SerializationException("Schema 42 not found in registry");
            }
        };

        RawPayloadPreservingAvroDeserializer deserializer = new RawPayloadPreservingAvroDeserializer(failingDelegate);
        RecordHeaders headers = new RecordHeaders();

        LedgerConsumerMessage result = deserializer.deserialize("ledger.events", headers, corruptBytes);

        assertThat(result.isDeserializationFailure()).isTrue();
        assertThat(result.schemaId()).isEqualTo(42);
        assertThat(result.rawPayload()).isEqualTo(corruptBytes);
        assertThat(result.deserializationErrorClass()).isEqualTo(SerializationException.class.getName());
        assertThat(result.deserializationErrorMessage()).contains("Schema 42 not found");

        String base64Header = new String(
                headers.lastHeader(RawPayloadPreservingAvroDeserializer.HEADER_ORIGINAL_PAYLOAD).value(),
                StandardCharsets.UTF_8
        );
        byte[] reconstructed = Base64.getDecoder().decode(base64Header);
        assertThat(reconstructed).isEqualTo(corruptBytes);

        assertThat(new String(
                headers.lastHeader(RawPayloadPreservingAvroDeserializer.HEADER_EXCEPTION_CLASS).value(),
                StandardCharsets.UTF_8
        )).isEqualTo(SerializationException.class.getName());
        assertThat(new String(
                headers.lastHeader(RawPayloadPreservingAvroDeserializer.HEADER_EXCEPTION_MESSAGE).value(),
                StandardCharsets.UTF_8
        )).contains("Schema 42 not found");
    }
}
