package io.ledgerstream.application.port.in;

import io.ledgerstream.domain.model.AccountNumber;
import io.ledgerstream.domain.model.CurrencyCode;

import java.util.Objects;

public record CreateAccountCommand(
        AccountNumber accountNumber,
        CurrencyCode currency
) {

    public CreateAccountCommand {
        Objects.requireNonNull(accountNumber, "accountNumber must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
    }
}
