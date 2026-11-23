package io.ledgerstream.adapter.out.messaging;

import io.ledgerstream.adapter.out.messaging.avro.LedgerBookingEvent;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

final class AvroSchemaCompatibilityTest {

    @Test
    void v1AndV2AreFullCompatibleInBothDirections() throws Exception {
        Schema v1 = loadSchema("/avro/LedgerBookingEventV1.avsc");
        Schema v2 = LedgerBookingEvent.getClassSchema();

        SchemaCompatibility.SchemaPairCompatibility backward =
                SchemaCompatibility.checkReaderWriterCompatibility(v2, v1);
        SchemaCompatibility.SchemaPairCompatibility forward =
                SchemaCompatibility.checkReaderWriterCompatibility(v1, v2);

        assertThat(backward.getType()).isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
        assertThat(forward.getType()).isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    void schemaWithRequiredFieldWithoutDefaultIsIncompatible() throws Exception {
        Schema v2 = LedgerBookingEvent.getClassSchema();
        Schema incompatible = loadSchema("/avro/LedgerBookingEventIncompatible.avsc");

        SchemaCompatibility.SchemaPairCompatibility check =
                SchemaCompatibility.checkReaderWriterCompatibility(incompatible, v2);

        assertThat(check.getType()).isEqualTo(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE);
    }

    private static Schema loadSchema(final String resourcePath) throws Exception {
        try (InputStream stream = AvroSchemaCompatibilityTest.class.getResourceAsStream(resourcePath)) {
            assertThat(stream).as("Missing schema resource " + resourcePath).isNotNull();
            return new Schema.Parser().parse(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        }
    }
}
