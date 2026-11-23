package io.ledgerstream.application.port.in;

import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;

public interface GetAccountUseCase {

    Account getById(AccountId accountId);
}
