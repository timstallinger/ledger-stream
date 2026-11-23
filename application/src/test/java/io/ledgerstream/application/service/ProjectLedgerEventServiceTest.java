package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.EventProcessingOutcome;
import io.ledgerstream.application.port.in.IncomingLedgerEvent;
import io.ledgerstream.application.port.out.AccountRepository;
import io.ledgerstream.application.port.out.AccountSummary;
import io.ledgerstream.application.port.out.AccountSummaryRepository;
import io.ledgerstream.application.port.out.ConsumerEventObserver;
import io.ledgerstream.application.port.out.ProcessedEventRepository;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.AccountNumber;
import io.ledgerstream.domain.model.BookingType;
import io.ledgerstream.domain.model.CurrencyCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

final class ProjectLedgerEventServiceTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");
    private static final CurrencyCode EUR = CurrencyCode.of("EUR");

    private Map<AccountId, Account> accounts;
    private Set<UUID> processedEventIds;
    private Map<AccountId, AccountSummary> summaries;
    private List<UUID> duplicateEvents;
    private List<String> poisonReasons;
    private ProjectLedgerEventService service;

    @BeforeEach
    void setUp() {
        accounts = new HashMap<>();
        processedEventIds = new HashSet<>();
        summaries = new HashMap<>();
        duplicateEvents = new ArrayList<>();
        poisonReasons = new ArrayList<>();

        AccountRepository accountRepository = new AccountRepository() {
            @Override
            public Account saveNew(final Account account) {
                accounts.put(account.id(), account);
                return account;
            }

            @Override
            public Account updateWithOptimisticLock(final Account account, final long expectedVersion) {
                accounts.put(account.id(), account);
                return account;
            }

            @Override
            public Optional<Account> findById(final AccountId accountId) {
                return Optional.ofNullable(accounts.get(accountId));
            }

            @Override
            public List<Account> findAll() {
                return List.copyOf(accounts.values());
            }
        };

        ProcessedEventRepository processedRepo = (eventId, eventType, processedAt) -> processedEventIds.add(eventId);

        AccountSummaryRepository summaryRepo = new AccountSummaryRepository() {
            @Override
            public void applyBookingDelta(
                    final AccountId accountId,
                    final AccountNumber accountNumber,
                    final CurrencyCode currency,
                    final BookingType bookingType,
                    final BigDecimal amount,
                    final UUID eventId,
                    final Instant occurredAt,
                    final Instant updatedAt
            ) {
                AccountSummary current = summaries.getOrDefault(
                        accountId,
                        new AccountSummary(
                                accountId, accountNumber, BigDecimal.ZERO, currency,
                                0L, BigDecimal.ZERO, BigDecimal.ZERO, null, null, updatedAt
                        )
                );
                BigDecimal delta = bookingType == BookingType.CREDIT ? amount : amount.negate();
                BigDecimal creditTotal = bookingType == BookingType.CREDIT
                        ? current.creditTotal().add(amount)
                        : current.creditTotal();
                BigDecimal debitTotal = bookingType == BookingType.DEBIT
                        ? current.debitTotal().add(amount)
                        : current.debitTotal();
                summaries.put(accountId, new AccountSummary(
                        accountId,
                        accountNumber,
                        current.balance().add(delta),
                        currency,
                        current.bookingCount() + 1,
                        creditTotal,
                        debitTotal,
                        eventId,
                        occurredAt,
                        updatedAt
                ));
            }

            @Override
            public Optional<AccountSummary> findByAccountId(final AccountId accountId) {
                return Optional.ofNullable(summaries.get(accountId));
            }

            @Override
            public List<AccountSummary> findAll() {
                return List.copyOf(summaries.values());
            }
        };

        ConsumerEventObserver observer = new ConsumerEventObserver() {
            @Override
            public void onProcessed(final UUID eventId) {
            }

            @Override
            public void onDuplicateRejected(final UUID eventId) {
                duplicateEvents.add(eventId);
            }

            @Override
            public void onPoisonRejected(final String eventIdOrKey, final String reason) {
                poisonReasons.add(reason);
            }
        };

        TransactionRunner tx = new TransactionRunner() {
            @Override
            public <T> T executeInTransaction(final Supplier<T> action) {
                return action.get();
            }
        };

        service = new ProjectLedgerEventService(
                accountRepository,
                processedRepo,
                summaryRepo,
                observer,
                tx,
                Clock.fixed(T0, ZoneOffset.UTC)
        );
    }

    @Test
    void processesEventOnceAndRejectsDuplicates() {
        Account account = Account.open(AccountId.generate(), AccountNumber.of("DE-P4-1"), EUR, T0);
        accounts.put(account.id(), account);

        UUID eventId = UUID.randomUUID();
        IncomingLedgerEvent event = new IncomingLedgerEvent(
                eventId, "BookingCreated", account.id().value(), UUID.randomUUID(), null,
                "CREDIT", new BigDecimal("75.00"), "EUR", "Deposit", 1L, T0, 1
        );

        assertThat(service.process(event)).isEqualTo(EventProcessingOutcome.PROCESSED);
        assertThat(service.process(event)).isEqualTo(EventProcessingOutcome.DUPLICATE_REJECTED);
        assertThat(service.process(event)).isEqualTo(EventProcessingOutcome.DUPLICATE_REJECTED);

        AccountSummary summary = summaries.get(account.id());
        assertThat(summary.balance()).isEqualByComparingTo("75.00");
        assertThat(summary.bookingCount()).isEqualTo(1L);
        assertThat(duplicateEvents).containsExactly(eventId, eventId);
    }

    @Test
    void rejectsUnknownAccountAndNonPositiveAmountAsPoison() {
        Account knownAccount = Account.open(AccountId.generate(), AccountNumber.of("DE-P4-2"), EUR, T0);
        accounts.put(knownAccount.id(), knownAccount);

        IncomingLedgerEvent unknownAccountEvent = new IncomingLedgerEvent(
                UUID.randomUUID(), "BookingCreated", UUID.randomUUID(), UUID.randomUUID(), null,
                "CREDIT", new BigDecimal("10.00"), "EUR", "Ghost", 1L, T0, 1
        );
        IncomingLedgerEvent zeroAmountEvent = new IncomingLedgerEvent(
                UUID.randomUUID(), "BookingCreated", knownAccount.id().value(), UUID.randomUUID(), null,
                "CREDIT", BigDecimal.ZERO, "EUR", "Zero", 1L, T0, 1
        );
        IncomingLedgerEvent negativeAmountEvent = new IncomingLedgerEvent(
                UUID.randomUUID(), "BookingCreated", knownAccount.id().value(), UUID.randomUUID(), null,
                "DEBIT", new BigDecimal("-5.00"), "EUR", "Neg", 1L, T0, 1
        );

        assertThat(service.process(unknownAccountEvent)).isEqualTo(EventProcessingOutcome.POISON_REJECTED);
        assertThat(service.process(zeroAmountEvent)).isEqualTo(EventProcessingOutcome.POISON_REJECTED);
        assertThat(service.process(negativeAmountEvent)).isEqualTo(EventProcessingOutcome.POISON_REJECTED);
        assertThat(poisonReasons).containsExactly("unknown_account", "non_positive_amount", "non_positive_amount");
        assertThat(summaries).isEmpty();
        assertThat(processedEventIds).isEmpty();
    }
}
