package io.ledgerstream.application.port.in;

import io.ledgerstream.domain.model.Account;

import java.util.List;

public interface ListAccountsUseCase {

    List<Account> listAll();
}
