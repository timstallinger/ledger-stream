package io.ledgerstream.application.port.out;

import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;

import java.util.List;
import java.util.Optional;

public interface AccountRepository {

    Account saveNew(Account account);

    Account updateWithOptimisticLock(Account account, long expectedVersion);

    Optional<Account> findById(AccountId accountId);

    List<Account> findAll();
}
