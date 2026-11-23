package io.ledgerstream.adapter.out.persistence;

import io.ledgerstream.domain.exception.LockTimeoutException;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;

final class PostgresErrorTranslator {

    static final String SQL_STATE_UNIQUE_VIOLATION = "23505";
    static final String SQL_STATE_CHECK_VIOLATION = "23514";
    static final String SQL_STATE_LOCK_NOT_AVAILABLE = "55P03";

    private PostgresErrorTranslator() {
    }

    static boolean isConstraintViolation(
            final DataIntegrityViolationException exception,
            final String expectedSqlState,
            final String expectedConstraintName
    ) {
        Throwable current = exception;
        while (current != null) {
            if (current instanceof PSQLException psqlException
                    && expectedSqlState.equals(psqlException.getSQLState())) {
                ServerErrorMessage serverError = psqlException.getServerErrorMessage();
                return serverError != null && expectedConstraintName.equals(serverError.getConstraint());
            }
            if (current instanceof SQLException sqlException
                    && expectedSqlState.equals(sqlException.getSQLState())
                    && !(current instanceof PSQLException)) {
                String message = sqlException.getMessage();
                return message != null && message.contains(expectedConstraintName);
            }
            current = current.getCause();
        }
        return false;
    }

    static void rethrowIfLockTimeout(final DataAccessException exception, final String context) {
        Throwable current = exception;
        while (current != null) {
            if (current instanceof SQLException sqlException
                    && SQL_STATE_LOCK_NOT_AVAILABLE.equals(sqlException.getSQLState())) {
                throw new LockTimeoutException("Database lock timeout exceeded during " + context);
            }
            current = current.getCause();
        }
    }
}
