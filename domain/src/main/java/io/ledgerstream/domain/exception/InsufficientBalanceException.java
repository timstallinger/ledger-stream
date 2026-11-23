package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.Money;

public final class InsufficientBalanceException extends DomainException {

    public InsufficientBalanceException(final AccountId accountId, final Money currentBalance, final Money debitAmount) {
        super(
                "INSUFFICIENT_BALANCE",
                "Insufficient balance on account " + accountId.value()
                        + ": balance " + currentBalance.amount() + " " + currentBalance.currency().value()
                        + " cannot cover debit of " + debitAmount.amount() + " " + debitAmount.currency().value()
        );
    }

    public InsufficientBalanceException(final AccountId accountId) {
        super("INSUFFICIENT_BALANCE", "Insufficient balance on account " + accountId.value() + ": balance must not become negative");
    }
}
