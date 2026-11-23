package io.ledgerstream.adapter.out.persistence;

import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;

final class PostgresErrorTranslator {

    static final String SQL_STATE_UNIQUE_VIOLATION = "23505";
    static final String SQL_STATE_CHECK_VIOLATION = "23514";

    private PostgresErrorTranslator() {
    }

    static boolean isSqlState(final DataIntegrityViolationException exception, final String expectedSqlState) {
        Throwable current = exception;
        while (current != null) {
            if (current instanceof SQLException sqlException
                    && expectedSqlState.equals(sqlException.getSQLState())) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    static boolean mentionsConstraint(final DataIntegrityViolationException exception, final String constraintName) {
        Throwable current = exception;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains(constraintName)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
