package io.ledgerstream.adapter.out.persistence;

import io.ledgerstream.application.port.out.AccountRepository;
import io.ledgerstream.domain.exception.DuplicateAccountNumberException;
import io.ledgerstream.domain.exception.InsufficientBalanceException;
import io.ledgerstream.domain.exception.OptimisticLockConflictException;
import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.AccountNumber;
import io.ledgerstream.domain.model.CurrencyCode;
import io.ledgerstream.domain.model.Money;
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

public final class JdbcAccountRepository implements AccountRepository {

    private static final String CONSTRAINT_ACCOUNT_NUMBER = "uq_accounts_account_number";
    private static final String CONSTRAINT_BALANCE_NON_NEGATIVE = "chk_accounts_balance_non_negative";

    private final JdbcClient jdbcClient;

    public JdbcAccountRepository(final JdbcClient jdbcClient) {
        this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient must not be null");
    }

    @Override
    public Account saveNew(final Account account) {
        try {
            jdbcClient.sql("""
                            INSERT INTO accounts (id, account_number, balance, currency, version, created_at, updated_at)
                            VALUES (:id, :accountNumber, :balance, :currency, :version, :createdAt, :updatedAt)
                            """)
                    .param("id", account.id().value())
                    .param("accountNumber", account.accountNumber().value())
                    .param("balance", account.balance().amount())
                    .param("currency", account.balance().currency().value())
                    .param("version", account.version())
                    .param("createdAt", OffsetDateTime.ofInstant(account.createdAt(), ZoneOffset.UTC))
                    .param("updatedAt", OffsetDateTime.ofInstant(account.updatedAt(), ZoneOffset.UTC))
                    .update();
            return account;
        } catch (DataIntegrityViolationException exception) {
            if (PostgresErrorTranslator.isSqlState(exception, PostgresErrorTranslator.SQL_STATE_UNIQUE_VIOLATION)
                    && PostgresErrorTranslator.mentionsConstraint(exception, CONSTRAINT_ACCOUNT_NUMBER)) {
                throw new DuplicateAccountNumberException(account.accountNumber());
            }
            if (PostgresErrorTranslator.isSqlState(exception, PostgresErrorTranslator.SQL_STATE_CHECK_VIOLATION)
                    && PostgresErrorTranslator.mentionsConstraint(exception, CONSTRAINT_BALANCE_NON_NEGATIVE)) {
                throw new InsufficientBalanceException(account.id());
            }
            throw exception;
        }
    }

    @Override
    public Account updateWithOptimisticLock(final Account account, final long expectedVersion) {
        try {
            int updatedRows = jdbcClient.sql("""
                            UPDATE accounts
                            SET balance = :balance,
                                version = version + 1,
                                updated_at = :updatedAt
                            WHERE id = :id
                              AND version = :expectedVersion
                            """)
                    .param("balance", account.balance().amount())
                    .param("updatedAt", OffsetDateTime.ofInstant(account.updatedAt(), ZoneOffset.UTC))
                    .param("id", account.id().value())
                    .param("expectedVersion", expectedVersion)
                    .update();
            if (updatedRows == 0) {
                throw new OptimisticLockConflictException(account.id(), expectedVersion);
            }
            return account;
        } catch (DataIntegrityViolationException exception) {
            if (PostgresErrorTranslator.isSqlState(exception, PostgresErrorTranslator.SQL_STATE_CHECK_VIOLATION)
                    && PostgresErrorTranslator.mentionsConstraint(exception, CONSTRAINT_BALANCE_NON_NEGATIVE)) {
                throw new InsufficientBalanceException(account.id());
            }
            throw exception;
        }
    }

    @Override
    public Optional<Account> findById(final AccountId accountId) {
        return jdbcClient.sql("""
                        SELECT id, account_number, balance, currency, version, created_at, updated_at
                        FROM accounts
                        WHERE id = :id
                        """)
                .param("id", accountId.value())
                .query(this::mapAccount)
                .optional();
    }

    @Override
    public List<Account> findAll() {
        return jdbcClient.sql("""
                        SELECT id, account_number, balance, currency, version, created_at, updated_at
                        FROM accounts
                        ORDER BY created_at DESC
                        """)
                .query(this::mapAccount)
                .list();
    }

    private Account mapAccount(final ResultSet rs, final int rowNum) throws SQLException {
        CurrencyCode currency = CurrencyCode.of(rs.getString("currency"));
        return new Account(
                AccountId.of(rs.getObject("id", UUID.class)),
                AccountNumber.of(rs.getString("account_number")),
                Money.of(rs.getBigDecimal("balance"), currency),
                rs.getLong("version"),
                rs.getObject("created_at", OffsetDateTime.class).toInstant(),
                rs.getObject("updated_at", OffsetDateTime.class).toInstant()
        );
    }
}
