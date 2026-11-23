package io.ledgerstream.adapter.out.persistence;

import io.ledgerstream.application.port.out.BookingRepository;
import io.ledgerstream.application.port.out.DuplicateIdempotencyKeyException;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.Booking;
import io.ledgerstream.domain.model.BookingId;
import io.ledgerstream.domain.model.BookingStatus;
import io.ledgerstream.domain.model.BookingType;
import io.ledgerstream.domain.model.CurrencyCode;
import io.ledgerstream.domain.model.IdempotencyKey;
import io.ledgerstream.domain.model.Money;
import io.ledgerstream.domain.model.Reference;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class JdbcBookingRepository implements BookingRepository {

    private static final String CONSTRAINT_BOOKING_IDEMPOTENCY = "uq_bookings_account_idempotency_key";

    private final JdbcClient jdbcClient;

    public JdbcBookingRepository(final JdbcClient jdbcClient) {
        this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient must not be null");
    }

    @Override
    public Booking saveNew(final Booking booking) {
        try {
            jdbcClient.sql("""
                            INSERT INTO bookings (
                                id, account_id, booking_type, amount, currency,
                                reference, status, idempotency_key, original_booking_id, created_at
                            )
                            VALUES (
                                :id, :accountId, :bookingType, :amount, :currency,
                                :reference, :status, :idempotencyKey, :originalBookingId, :createdAt
                            )
                            """)
                    .param("id", booking.id().value())
                    .param("accountId", booking.accountId().value())
                    .param("bookingType", booking.type().name())
                    .param("amount", booking.amount().amount())
                    .param("currency", booking.amount().currency().value())
                    .param("reference", booking.reference().value())
                    .param("status", booking.status().name())
                    .param("idempotencyKey", booking.idempotencyKey().value())
                    .param("originalBookingId",
                            booking.originalBookingId() != null ? booking.originalBookingId().value() : null)
                    .param("createdAt", OffsetDateTime.ofInstant(booking.createdAt(), ZoneOffset.UTC))
                    .update();
            return booking;
        } catch (DataIntegrityViolationException exception) {
            if (PostgresErrorTranslator.isSqlState(exception, PostgresErrorTranslator.SQL_STATE_UNIQUE_VIOLATION)
                    && PostgresErrorTranslator.mentionsConstraint(exception, CONSTRAINT_BOOKING_IDEMPOTENCY)) {
                throw new DuplicateIdempotencyKeyException(booking.accountId(), booking.idempotencyKey());
            }
            throw exception;
        }
    }

    @Override
    public void markCancelled(final BookingId bookingId) {
        jdbcClient.sql("UPDATE bookings SET status = 'CANCELLED' WHERE id = :id")
                .param("id", bookingId.value())
                .update();
    }

    @Override
    public Optional<Booking> findByIdAndAccountId(final BookingId bookingId, final AccountId accountId) {
        return jdbcClient.sql("""
                        SELECT id, account_id, booking_type, amount, currency,
                               reference, status, idempotency_key, original_booking_id, created_at
                        FROM bookings
                        WHERE id = :id AND account_id = :accountId
                        """)
                .param("id", bookingId.value())
                .param("accountId", accountId.value())
                .query(this::mapBooking)
                .optional();
    }

    @Override
    public List<Booking> findByAccountId(final AccountId accountId) {
        return jdbcClient.sql("""
                        SELECT id, account_id, booking_type, amount, currency,
                               reference, status, idempotency_key, original_booking_id, created_at
                        FROM bookings
                        WHERE account_id = :accountId
                        ORDER BY created_at DESC
                        """)
                .param("accountId", accountId.value())
                .query(this::mapBooking)
                .list();
    }

    private Booking mapBooking(final ResultSet rs, final int rowNum) throws SQLException {
        CurrencyCode currency = CurrencyCode.of(rs.getString("currency"));
        UUID originalId = rs.getObject("original_booking_id", UUID.class);
        return new Booking(
                BookingId.of(rs.getObject("id", UUID.class)),
                AccountId.of(rs.getObject("account_id", UUID.class)),
                BookingType.valueOf(rs.getString("booking_type")),
                Money.of(rs.getBigDecimal("amount"), currency),
                Reference.of(rs.getString("reference")),
                BookingStatus.valueOf(rs.getString("status")),
                IdempotencyKey.of(rs.getString("idempotency_key")),
                originalId != null ? BookingId.of(originalId) : null,
                rs.getObject("created_at", OffsetDateTime.class).toInstant()
        );
    }
}
