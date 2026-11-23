package io.ledgerstream.adapter.in.messaging;

import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;

public final class LedgerConsumerMessage {

    private final GenericRecord record;
    private final byte[] rawPayload;
    private final Integer schemaId;
    private final String deserializationErrorClass;
    private final String deserializationErrorMessage;

    private LedgerConsumerMessage(
            final GenericRecord record,
            final byte[] rawPayload,
            final Integer schemaId,
            final String deserializationErrorClass,
            final String deserializationErrorMessage
    ) {
        this.record = record;
        this.rawPayload = rawPayload != null ? Arrays.copyOf(rawPayload, rawPayload.length) : new byte[0];
        this.schemaId = schemaId;
        this.deserializationErrorClass = deserializationErrorClass;
        this.deserializationErrorMessage = deserializationErrorMessage;
    }

    public static LedgerConsumerMessage valid(
            final GenericRecord record,
            final byte[] rawPayload,
            final Integer schemaId
    ) {
        return new LedgerConsumerMessage(record, rawPayload, schemaId, null, null);
    }

    public static LedgerConsumerMessage deserializationFailed(
            final byte[] rawPayload,
            final Integer schemaId,
            final String errorClass,
            final String errorMessage
    ) {
        return new LedgerConsumerMessage(null, rawPayload, schemaId, errorClass, errorMessage);
    }

    public boolean isDeserializationFailure() {
        return deserializationErrorClass != null || record == null;
    }

    public GenericRecord record() {
        return record;
    }

    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }

    public Integer schemaId() {
        return schemaId;
    }

    public String deserializationErrorClass() {
        return deserializationErrorClass;
    }

    public String deserializationErrorMessage() {
        return deserializationErrorMessage;
    }
}
