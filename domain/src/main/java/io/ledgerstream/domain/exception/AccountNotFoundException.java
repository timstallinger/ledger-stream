package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;
import io.ledgerstream.domain.model.AccountId;

public final class AccountNotFoundException extends DomainException {

    public AccountNotFoundException(final AccountId accountId) {
        super("ACCOUNT_NOT_FOUND", "Account not found: " + accountId.value());
    }
}
