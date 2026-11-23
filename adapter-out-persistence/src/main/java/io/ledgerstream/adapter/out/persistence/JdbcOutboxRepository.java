package io.ledgerstream.adapter.out.persistence;

import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxRepository;
import io.ledgerstream.application.port.out.OutboxStatus;
import io.ledgerstream.application.port.out.PendingOutboxCandidate;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class JdbcOutboxRepository implements OutboxRepository {

    private final JdbcClient jdbcClient;

    public JdbcOutboxRepository(final JdbcClient jdbcClient) {
        this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient must not be null");
    }

    @Override
    public void save(final OutboxEvent event) {
        jdbcClient.sql("""
                        INSERT INTO outbox_events (
                            id, aggregate_type, aggregate_id, event_type, schema_version,
                            payload, correlation_id, status, occurred_at, created_at,
                            sent_at, retry_count, last_error, next_attempt_at
                        )
                        VALUES (
                            :id, :aggregateType, :aggregateId, :eventType, :schemaVersion,
                            :payload, :correlationId, :status, :occurredAt, :createdAt,
                            :sentAt, :retryCount, :lastError, :nextAttemptAt
                        )
                        """)
                .param("id", event.id())
                .param("aggregateType", event.aggregateType())
                .param("aggregateId", event.aggregateId())
                .param("eventType", event.eventType())
                .param("schemaVersion", event.schemaVersion())
                .param("payload", event.payload())
                .param("correlationId", event.correlationId())
                .param("status", event.status().name())
                .param("occurredAt", OffsetDateTime.ofInstant(event.occurredAt(), ZoneOffset.UTC))
                .param("createdAt", OffsetDateTime.ofInstant(event.createdAt(), ZoneOffset.UTC))
                .param("sentAt", event.sentAt() != null ? OffsetDateTime.ofInstant(event.sentAt(), ZoneOffset.UTC) : null)
                .param("retryCount", event.retryCount())
                .param("lastError", event.lastError())
                .param("nextAttemptAt", OffsetDateTime.ofInstant(event.nextAttemptAt(), ZoneOffset.UTC))
                .update();
    }

    @Override
    public List<PendingOutboxCandidate> findPendingCandidates(final int limit, final Instant now) {
        OffsetDateTime nowUtc = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);
        return jdbcClient.sql("""
                        SELECT e.id, e.aggregate_id
                        FROM outbox_events e
                        WHERE e.status = 'PENDING'
                          AND e.next_attempt_at <= :now
                          AND NOT EXISTS (
                              SELECT 1
                              FROM outbox_events earlier
                              WHERE earlier.aggregate_id = e.aggregate_id
                                AND earlier.status = 'PENDING'
                                AND earlier.created_at < e.created_at
                                AND earlier.next_attempt_at > :now
                          )
                        ORDER BY e.created_at ASC
                        LIMIT :limit
                        """)
                .param("now", nowUtc)
                .param("limit", limit)
                .query((rs, rowNum) -> new PendingOutboxCandidate(
                        rs.getObject("id", UUID.class),
                        rs.getObject("aggregate_id", UUID.class)
                ))
                .list();
    }

    @Override
    public Optional<OutboxEvent> lockPendingById(final UUID eventId, final Instant now) {
        return jdbcClient.sql("""
                        SELECT id, aggregate_type, aggregate_id, event_type, schema_version,
                               payload, correlation_id, status, occurred_at, created_at,
                               sent_at, retry_count, last_error, next_attempt_at
                        FROM outbox_events
                        WHERE id = :id
                          AND status = 'PENDING'
                          AND next_attempt_at <= :now
                        FOR UPDATE SKIP LOCKED
                        """)
                .param("id", eventId)
                .param("now", OffsetDateTime.ofInstant(now, ZoneOffset.UTC))
                .query(this::mapOutboxEvent)
                .optional();
    }

    @Override
    public void markSent(final UUID eventId, final Instant sentAt) {
        jdbcClient.sql("""
                        UPDATE outbox_events
                        SET status = 'SENT',
                            sent_at = :sentAt,
                            last_error = NULL
                        WHERE id = :id
                        """)
                .param("sentAt", OffsetDateTime.ofInstant(sentAt, ZoneOffset.UTC))
                .param("id", eventId)
                .update();
    }

    @Override
    public int recordRetryFailure(
            final UUID eventId,
            final String errorMessage,
            final Instant nextAttemptAt,
            final int maxAttempts
    ) {
        return jdbcClient.sql("""
                        UPDATE outbox_events
                        SET retry_count = retry_count + 1,
                            last_error = :lastError,
                            next_attempt_at = :nextAttemptAt,
                            status = CASE WHEN retry_count + 1 >= :maxAttempts THEN 'FAILED' ELSE 'PENDING' END
                        WHERE id = :id
                        RETURNING retry_count
                        """)
                .param("lastError", errorMessage)
                .param("nextAttemptAt", OffsetDateTime.ofInstant(nextAttemptAt, ZoneOffset.UTC))
                .param("maxAttempts", maxAttempts)
                .param("id", eventId)
                .query(Integer.class)
                .single();
    }

    @Override
    public long countPending() {
        return jdbcClient.sql("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'")
                .query(Long.class)
                .single();
    }

    @Override
    public Optional<Instant> findOldestPendingCreatedAt() {
        OffsetDateTime oldest = jdbcClient.sql("SELECT MIN(created_at) FROM outbox_events WHERE status = 'PENDING'")
                .query(OffsetDateTime.class)
                .optional()
                .orElse(null);
        return Optional.ofNullable(oldest).map(OffsetDateTime::toInstant);
    }

    private OutboxEvent mapOutboxEvent(final ResultSet rs, final int rowNum) throws SQLException {
        OffsetDateTime sentAt = rs.getObject("sent_at", OffsetDateTime.class);
        return new OutboxEvent(
                rs.getObject("id", UUID.class),
                rs.getString("aggregate_type"),
                rs.getObject("aggregate_id", UUID.class),
                rs.getString("event_type"),
                rs.getInt("schema_version"),
                rs.getString("payload"),
                rs.getString("correlation_id"),
                OutboxStatus.valueOf(rs.getString("status")),
                rs.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                rs.getObject("created_at", OffsetDateTime.class).toInstant(),
                sentAt != null ? sentAt.toInstant() : null,
                rs.getInt("retry_count"),
                rs.getString("last_error"),
                rs.getObject("next_attempt_at", OffsetDateTime.class).toInstant()
        );
    }
}
