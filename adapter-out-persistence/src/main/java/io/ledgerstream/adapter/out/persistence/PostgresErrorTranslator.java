package io.ledgerstream.adapter.out.persistence;

import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;

final class PostgresErrorTranslator {

    static final String SQL_STATE_UNIQUE_VIOLATION = "23505";
    static final String SQL_STATE_CHECK_VIOLATION = "23514";

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
}
