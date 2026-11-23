package io.ledgerstream.adapter.out.persistence;

import io.ledgerstream.application.port.out.ProcessedEventRepository;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;

public final class JdbcProcessedEventRepository implements ProcessedEventRepository {

    private final JdbcClient jdbcClient;

    public JdbcProcessedEventRepository(final JdbcClient jdbcClient) {
        this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient must not be null");
    }

    @Override
    public boolean tryMarkProcessed(final UUID eventId, final String eventType, final Instant processedAt) {
        int insertedRows = jdbcClient.sql("""
                        INSERT INTO processed_events (event_id, event_type, processed_at)
                        VALUES (:eventId, :eventType, :processedAt)
                        ON CONFLICT (event_id) DO NOTHING
                        """)
                .param("eventId", eventId)
                .param("eventType", eventType)
                .param("processedAt", OffsetDateTime.ofInstant(processedAt, ZoneOffset.UTC))
                .update();
        return insertedRows == 1;
    }
}
