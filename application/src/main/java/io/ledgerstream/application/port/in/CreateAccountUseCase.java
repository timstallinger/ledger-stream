package io.ledgerstream.application.port.in;

import io.ledgerstream.domain.model.Account;

public interface CreateAccountUseCase {

    Account execute(CreateAccountCommand command);
}
