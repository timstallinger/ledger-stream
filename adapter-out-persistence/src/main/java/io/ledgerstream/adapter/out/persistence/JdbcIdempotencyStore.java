package io.ledgerstream.adapter.out.persistence;

import io.ledgerstream.application.port.in.StoredIdempotentResponse;
import io.ledgerstream.application.port.out.DuplicateIdempotencyKeyException;
import io.ledgerstream.application.port.out.IdempotencyRecord;
import io.ledgerstream.application.port.out.IdempotencyStore;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.BookingId;
import io.ledgerstream.domain.model.IdempotencyKey;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.Objects;
import java.util.Optional;

public final class JdbcIdempotencyStore implements IdempotencyStore {

    private static final String CONSTRAINT_IDEMPOTENCY_PK = "pk_idempotency_keys";

    private final JdbcClient jdbcClient;

    public JdbcIdempotencyStore(final JdbcClient jdbcClient) {
        this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient must not be null");
    }

    @Override
    public Optional<IdempotencyRecord> find(
            final AccountId accountId,
            final IdempotencyKey idempotencyKey
    ) {
        return jdbcClient.sql("""
                        SELECT request_fingerprint, response_status, response_body
                        FROM idempotency_keys
                        WHERE account_id = :accountId
                          AND idempotency_key = :idempotencyKey
                        """)
                .param("accountId", accountId.value())
                .param("idempotencyKey", idempotencyKey.value())
                .query((rs, rowNum) -> new IdempotencyRecord(
                        rs.getString("request_fingerprint"),
                        new StoredIdempotentResponse(
                                rs.getInt("response_status"),
                                rs.getString("response_body")
                        )
                ))
                .optional();
    }

    @Override
    public void save(
            final AccountId accountId,
            final IdempotencyKey idempotencyKey,
            final BookingId bookingId,
            final String requestFingerprint,
            final StoredIdempotentResponse response
    ) {
        try {
            jdbcClient.sql("""
                            INSERT INTO idempotency_keys (
                                account_id, idempotency_key, booking_id, request_fingerprint, response_status, response_body
                            )
                            VALUES (
                                :accountId, :idempotencyKey, :bookingId, :requestFingerprint, :responseStatus, :responseBody
                            )
                            """)
                    .param("accountId", accountId.value())
                    .param("idempotencyKey", idempotencyKey.value())
                    .param("bookingId", bookingId.value())
                    .param("requestFingerprint", requestFingerprint)
                    .param("responseStatus", response.statusCode())
                    .param("responseBody", response.responseBody())
                    .update();
        } catch (DataIntegrityViolationException exception) {
            if (PostgresErrorTranslator.isConstraintViolation(
                    exception,
                    PostgresErrorTranslator.SQL_STATE_UNIQUE_VIOLATION,
                    CONSTRAINT_IDEMPOTENCY_PK
            )) {
                throw new DuplicateIdempotencyKeyException(accountId, idempotencyKey);
            }
            throw exception;
        } catch (DataAccessException exception) {
            PostgresErrorTranslator.rethrowIfLockTimeout(exception, "idempotency key insert");
            throw exception;
        }
    }
}
