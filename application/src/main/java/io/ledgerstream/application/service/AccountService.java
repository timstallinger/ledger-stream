package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.CreateAccountCommand;
import io.ledgerstream.application.port.in.CreateAccountUseCase;
import io.ledgerstream.application.port.in.GetAccountUseCase;
import io.ledgerstream.application.port.in.ListAccountsUseCase;
import io.ledgerstream.application.port.out.AccountRepository;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.domain.exception.AccountNotFoundException;
import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

public final class AccountService implements CreateAccountUseCase, GetAccountUseCase, ListAccountsUseCase {

    private final AccountRepository accountRepository;
    private final TransactionRunner transactionRunner;
    private final Clock clock;

    public AccountService(
            final AccountRepository accountRepository,
            final TransactionRunner transactionRunner,
            final Clock clock
    ) {
        this.accountRepository = Objects.requireNonNull(accountRepository, "accountRepository must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public Account execute(final CreateAccountCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        return transactionRunner.executeInTransaction(() -> {
            Account account = Account.open(
                    AccountId.generate(),
                    command.accountNumber(),
                    command.currency(),
                    clock.instant()
            );
            return accountRepository.saveNew(account);
        });
    }

    @Override
    public Account getById(final AccountId accountId) {
        Objects.requireNonNull(accountId, "accountId must not be null");
        return accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException(accountId));
    }

    @Override
    public List<Account> listAll() {
        return accountRepository.findAll();
    }
}
