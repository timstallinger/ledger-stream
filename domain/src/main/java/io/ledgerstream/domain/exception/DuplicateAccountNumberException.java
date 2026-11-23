package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;
import io.ledgerstream.domain.model.AccountNumber;

public final class DuplicateAccountNumberException extends DomainException {

    public DuplicateAccountNumberException(final AccountNumber accountNumber) {
        super("DUPLICATE_ACCOUNT_NUMBER", "Account number already exists: " + accountNumber.value());
    }
}
