package io.ledgerstream.adapter.in.rest;

import io.ledgerstream.application.port.in.CreateAccountCommand;
import io.ledgerstream.application.port.in.CreateAccountUseCase;
import io.ledgerstream.application.port.in.GetAccountUseCase;
import io.ledgerstream.application.port.in.ListAccountsUseCase;
import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.AccountNumber;
import io.ledgerstream.domain.model.CurrencyCode;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/accounts")
public final class AccountController {

    private final CreateAccountUseCase createAccountUseCase;
    private final GetAccountUseCase getAccountUseCase;
    private final ListAccountsUseCase listAccountsUseCase;

    public AccountController(
            final CreateAccountUseCase createAccountUseCase,
            final GetAccountUseCase getAccountUseCase,
            final ListAccountsUseCase listAccountsUseCase
    ) {
        this.createAccountUseCase = Objects.requireNonNull(createAccountUseCase, "createAccountUseCase must not be null");
        this.getAccountUseCase = Objects.requireNonNull(getAccountUseCase, "getAccountUseCase must not be null");
        this.listAccountsUseCase = Objects.requireNonNull(listAccountsUseCase, "listAccountsUseCase must not be null");
    }

    @PostMapping
    public ResponseEntity<AccountResponse> createAccount(@Valid @RequestBody final CreateAccountRequest request) {
        Account created = createAccountUseCase.execute(new CreateAccountCommand(
                AccountNumber.of(request.accountNumber()),
                CurrencyCode.of(request.currency())
        ));
        AccountResponse response = AccountResponse.from(created);
        return ResponseEntity
                .created(URI.create("/api/v1/accounts/" + response.id()))
                .body(response);
    }

    @GetMapping
    public ResponseEntity<List<AccountResponse>> listAccounts() {
        List<AccountResponse> accounts = listAccountsUseCase.listAll().stream()
                .map(AccountResponse::from)
                .toList();
        return ResponseEntity.ok(accounts);
    }

    @GetMapping("/{accountId}")
    public ResponseEntity<AccountResponse> getAccount(@PathVariable final UUID accountId) {
        Account account = getAccountUseCase.getById(AccountId.of(accountId));
        return ResponseEntity.ok(AccountResponse.from(account));
    }
}
