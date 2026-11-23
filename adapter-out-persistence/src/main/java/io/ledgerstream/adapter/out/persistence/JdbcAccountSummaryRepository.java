package io.ledgerstream.adapter.out.persistence;

import io.ledgerstream.application.port.out.AccountSummary;
import io.ledgerstream.application.port.out.AccountSummaryRepository;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.AccountNumber;
import io.ledgerstream.domain.model.BookingType;
import io.ledgerstream.domain.model.CurrencyCode;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class JdbcAccountSummaryRepository implements AccountSummaryRepository {

    private final JdbcClient jdbcClient;

    public JdbcAccountSummaryRepository(final JdbcClient jdbcClient) {
        this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient must not be null");
    }

    @Override
    public void applyBookingDelta(
            final AccountId accountId,
            final AccountNumber accountNumber,
            final CurrencyCode currency,
            final BookingType bookingType,
            final BigDecimal amount,
            final UUID eventId,
            final Instant occurredAt,
            final Instant updatedAt
    ) {
        BigDecimal balanceDelta = bookingType == BookingType.CREDIT ? amount : amount.negate();
        BigDecimal creditDelta = bookingType == BookingType.CREDIT ? amount : BigDecimal.ZERO;
        BigDecimal debitDelta = bookingType == BookingType.DEBIT ? amount : BigDecimal.ZERO;

        jdbcClient.sql("""
                        INSERT INTO account_summary (
                            account_id, account_number, balance, currency,
                            booking_count, credit_total, debit_total,
                            last_event_id, last_event_at, updated_at
                        )
                        VALUES (
                            :accountId, :accountNumber, :balanceDelta, :currency,
                            1, :creditDelta, :debitDelta,
                            :eventId, :occurredAt, :updatedAt
                        )
                        ON CONFLICT (account_id) DO UPDATE SET
                            balance = account_summary.balance + EXCLUDED.balance,
                            booking_count = account_summary.booking_count + 1,
                            credit_total = account_summary.credit_total + EXCLUDED.credit_total,
                            debit_total = account_summary.debit_total + EXCLUDED.debit_total,
                            last_event_id = EXCLUDED.last_event_id,
                            last_event_at = EXCLUDED.last_event_at,
                            updated_at = EXCLUDED.updated_at
                        """)
                .param("accountId", accountId.value())
                .param("accountNumber", accountNumber.value())
                .param("balanceDelta", balanceDelta)
                .param("currency", currency.value())
                .param("creditDelta", creditDelta)
                .param("debitDelta", debitDelta)
                .param("eventId", eventId)
                .param("occurredAt", OffsetDateTime.ofInstant(occurredAt, ZoneOffset.UTC))
                .param("updatedAt", OffsetDateTime.ofInstant(updatedAt, ZoneOffset.UTC))
                .update();
    }

    @Override
    public Optional<AccountSummary> findByAccountId(final AccountId accountId) {
        return jdbcClient.sql("""
                        SELECT account_id, account_number, balance, currency,
                               booking_count, credit_total, debit_total,
                               last_event_id, last_event_at, updated_at
                        FROM account_summary
                        WHERE account_id = :accountId
                        """)
                .param("accountId", accountId.value())
                .query(this::mapSummary)
                .optional();
    }

    @Override
    public List<AccountSummary> findAll() {
        return jdbcClient.sql("""
                        SELECT account_id, account_number, balance, currency,
                               booking_count, credit_total, debit_total,
                               last_event_id, last_event_at, updated_at
                        FROM account_summary
                        ORDER BY account_number ASC
                        """)
                .query(this::mapSummary)
                .list();
    }

    private AccountSummary mapSummary(final ResultSet rs, final int rowNum) throws SQLException {
        OffsetDateTime lastEventAt = rs.getObject("last_event_at", OffsetDateTime.class);
        return new AccountSummary(
                AccountId.of(rs.getObject("account_id", UUID.class)),
                AccountNumber.of(rs.getString("account_number")),
                rs.getBigDecimal("balance"),
                CurrencyCode.of(rs.getString("currency")),
                rs.getLong("booking_count"),
                rs.getBigDecimal("credit_total"),
                rs.getBigDecimal("debit_total"),
                rs.getObject("last_event_id", UUID.class),
                lastEventAt != null ? lastEventAt.toInstant() : null,
                rs.getObject("updated_at", OffsetDateTime.class).toInstant()
        );
    }
}
