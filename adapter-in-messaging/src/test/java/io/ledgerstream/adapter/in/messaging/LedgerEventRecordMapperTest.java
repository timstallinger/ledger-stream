package io.ledgerstream.adapter.in.messaging;

import io.ledgerstream.application.port.in.IncomingLedgerEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

final class LedgerEventRecordMapperTest {

    private final LedgerEventRecordMapper mapper = new LedgerEventRecordMapper();

    @Test
    void resolvesNonNullDefaultWhenOptionalInitiationChannelFieldIsAbsentOnGenericRecord() {
        String v1SchemaJson = """
                {
                  "type": "record",
                  "name": "LedgerBookingEvent",
                  "namespace": "io.ledgerstream.adapter.out.messaging.avro",
                  "fields": [
                    { "name": "eventId", "type": "string" },
                    { "name": "eventType", "type": "string" },
                    { "name": "aggregateId", "type": "string" },
                    { "name": "bookingId", "type": "string" },
                    { "name": "originalBookingId", "type": ["null", "string"], "default": null },
                    { "name": "bookingType", "type": "string" },
                    { "name": "amount", "type": "string" },
                    { "name": "currency", "type": "string" },
                    { "name": "reference", "type": "string" },
                    { "name": "version", "type": "long" },
                    { "name": "occurredAt", "type": "string" },
                    { "name": "schemaVersion", "type": "int", "default": 1 }
                  ]
                }
                """;
        Schema v1Schema = new Schema.Parser().parse(v1SchemaJson);
        GenericRecord record = new GenericData.Record(v1Schema);
        record.put("eventId", UUID.randomUUID().toString());
        record.put("eventType", "BookingCreated");
        record.put("aggregateId", UUID.randomUUID().toString());
        record.put("bookingId", UUID.randomUUID().toString());
        record.put("originalBookingId", null);
        record.put("bookingType", "CREDIT");
        record.put("amount", "42.00");
        record.put("currency", "EUR");
        record.put("reference", "v1 record without optional field");
        record.put("version", 1L);
        record.put("occurredAt", Instant.now().toString());
        record.put("schemaVersion", 1);

        IncomingLedgerEvent mapped = mapper.map(record).orElseThrow();

        assertThat(mapped.initiationChannel()).isNotNull();
        assertThat(mapped.initiationChannel()).isEqualTo("API");
    }
}
