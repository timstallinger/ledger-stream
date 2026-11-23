package io.ledgerstream.adapter.out.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.application.port.out.DlqEntry;
import io.ledgerstream.application.port.out.DlqRepository;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class JdbcDlqRepository implements DlqRepository {

    private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {
    };

    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;

    public JdbcDlqRepository(final JdbcClient jdbcClient, final ObjectMapper objectMapper) {
        this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    @Override
    public void save(final DlqEntry entry) {
        jdbcClient.sql("""
                        INSERT INTO dlq_entries (
                            id, topic, partition, "offset", record_key, event_id,
                            error_class, error_message, attempt_count, payload, headers,
                            created_at, replayed_at, replayed_by, note
                        )
                        VALUES (
                            :id, :topic, :partition, :offset, :recordKey, :eventId,
                            :errorClass, :errorMessage, :attemptCount, :payload, CAST(:headers AS jsonb),
                            :createdAt, :replayedAt, :replayedBy, :note
                        )
                        """)
                .param("id", entry.id())
                .param("topic", entry.topic())
                .param("partition", entry.partition())
                .param("offset", entry.offset())
                .param("recordKey", entry.recordKey())
                .param("eventId", entry.eventId())
                .param("errorClass", entry.errorClass())
                .param("errorMessage", entry.errorMessage())
                .param("attemptCount", entry.attemptCount())
                .param("payload", entry.payload())
                .param("headers", serializeHeaders(entry.headers()))
                .param("createdAt", OffsetDateTime.ofInstant(entry.createdAt(), ZoneOffset.UTC))
                .param("replayedAt", entry.replayedAt() != null
                        ? OffsetDateTime.ofInstant(entry.replayedAt(), ZoneOffset.UTC)
                        : null)
                .param("replayedBy", entry.replayedBy())
                .param("note", entry.note())
                .update();
    }

    @Override
    public Optional<DlqEntry> findById(final UUID id) {
        return jdbcClient.sql("""
                        SELECT id, topic, partition, "offset", record_key, event_id,
                               error_class, error_message, attempt_count, payload, headers::text AS headers_json,
                               created_at, replayed_at, replayed_by, note
                        FROM dlq_entries
                        WHERE id = :id
                        """)
                .param("id", id)
                .query(this::mapEntry)
                .optional();
    }

    @Override
    public List<DlqEntry> findUnhandled() {
        return jdbcClient.sql("""
                        SELECT id, topic, partition, "offset", record_key, event_id,
                               error_class, error_message, attempt_count, payload, headers::text AS headers_json,
                               created_at, replayed_at, replayed_by, note
                        FROM dlq_entries
                        WHERE replayed_at IS NULL
                        ORDER BY created_at ASC
                        """)
                .query(this::mapEntry)
                .list();
    }

    @Override
    public boolean markReplayed(
            final UUID id,
            final Instant replayedAt,
            final String replayedBy,
            final String note
    ) {
        int updated = jdbcClient.sql("""
                        UPDATE dlq_entries
                        SET replayed_at = :replayedAt,
                            replayed_by = :replayedBy,
                            note = :note
                        WHERE id = :id
                          AND replayed_at IS NULL
                        """)
                .param("replayedAt", OffsetDateTime.ofInstant(replayedAt, ZoneOffset.UTC))
                .param("replayedBy", replayedBy)
                .param("note", note)
                .param("id", id)
                .update();
        return updated == 1;
    }

    @Override
    public long countUnhandled() {
        return jdbcClient.sql("SELECT COUNT(*) FROM dlq_entries WHERE replayed_at IS NULL")
                .query(Long.class)
                .single();
    }

    private DlqEntry mapEntry(final ResultSet rs, final int rowNum) throws SQLException {
        OffsetDateTime replayedAt = rs.getObject("replayed_at", OffsetDateTime.class);
        return new DlqEntry(
                rs.getObject("id", UUID.class),
                rs.getString("topic"),
                rs.getInt("partition"),
                rs.getLong("offset"),
                rs.getString("record_key"),
                rs.getObject("event_id", UUID.class),
                rs.getString("error_class"),
                rs.getString("error_message"),
                rs.getInt("attempt_count"),
                rs.getBytes("payload"),
                deserializeHeaders(rs.getString("headers_json")),
                rs.getObject("created_at", OffsetDateTime.class).toInstant(),
                replayedAt != null ? replayedAt.toInstant() : null,
                rs.getString("replayed_by"),
                rs.getString("note")
        );
    }

    private String serializeHeaders(final Map<String, String> headers) {
        try {
            return objectMapper.writeValueAsString(headers != null ? headers : Map.of());
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize DLQ headers to JSON", exception);
        }
    }

    private Map<String, String> deserializeHeaders(final String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return new LinkedHashMap<>(objectMapper.readValue(json, HEADERS_TYPE));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to deserialize DLQ headers from JSON", exception);
        }
    }
}
