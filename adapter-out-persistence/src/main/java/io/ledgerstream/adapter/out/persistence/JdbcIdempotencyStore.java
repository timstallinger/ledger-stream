package io.ledgerstream.adapter.out.persistence;

import io.ledgerstream.application.port.in.StoredIdempotentResponse;
import io.ledgerstream.application.port.out.DuplicateIdempotencyKeyException;
import io.ledgerstream.application.port.out.IdempotencyStore;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.BookingId;
import io.ledgerstream.domain.model.IdempotencyKey;
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
    public Optional<StoredIdempotentResponse> find(
            final AccountId accountId,
            final IdempotencyKey idempotencyKey
    ) {
        return jdbcClient.sql("""
                        SELECT response_status, response_body
                        FROM idempotency_keys
                        WHERE account_id = :accountId
                          AND idempotency_key = :idempotencyKey
                        """)
                .param("accountId", accountId.value())
                .param("idempotencyKey", idempotencyKey.value())
                .query((rs, rowNum) -> new StoredIdempotentResponse(
                        rs.getInt("response_status"),
                        rs.getString("response_body")
                ))
                .optional();
    }

    @Override
    public void save(
            final AccountId accountId,
            final IdempotencyKey idempotencyKey,
            final BookingId bookingId,
            final StoredIdempotentResponse response
    ) {
        try {
            jdbcClient.sql("""
                            INSERT INTO idempotency_keys (
                                account_id, idempotency_key, booking_id, response_status, response_body
                            )
                            VALUES (
                                :accountId, :idempotencyKey, :bookingId, :responseStatus, :responseBody
                            )
                            """)
                    .param("accountId", accountId.value())
                    .param("idempotencyKey", idempotencyKey.value())
                    .param("bookingId", bookingId.value())
                    .param("responseStatus", response.statusCode())
                    .param("responseBody", response.responseBody())
                    .update();
        } catch (DataIntegrityViolationException exception) {
            if (PostgresErrorTranslator.isSqlState(exception, PostgresErrorTranslator.SQL_STATE_UNIQUE_VIOLATION)
                    && PostgresErrorTranslator.mentionsConstraint(exception, CONSTRAINT_IDEMPOTENCY_PK)) {
                throw new DuplicateIdempotencyKeyException(accountId, idempotencyKey);
            }
            throw exception;
        }
    }
}
