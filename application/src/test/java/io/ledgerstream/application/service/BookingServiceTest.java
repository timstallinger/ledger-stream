package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.CancelBookingCommand;
import io.ledgerstream.application.port.in.CreateAccountCommand;
import io.ledgerstream.application.port.in.CreateBookingCommand;
import io.ledgerstream.application.port.in.IdempotentExecutionResult;
import io.ledgerstream.application.port.in.StoredIdempotentResponse;
import io.ledgerstream.application.port.out.AccountRepository;
import io.ledgerstream.application.port.out.BookingRepository;
import io.ledgerstream.application.port.out.DuplicateIdempotencyKeyException;
import io.ledgerstream.application.port.out.IdempotencyStore;
import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxPayloadSerializer;
import io.ledgerstream.application.port.out.OutboxRepository;
import io.ledgerstream.application.port.out.OutboxStatus;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.domain.exception.InsufficientBalanceException;
import io.ledgerstream.domain.exception.OptimisticLockConflictException;
import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.AccountNumber;
import io.ledgerstream.domain.model.Booking;
import io.ledgerstream.domain.model.BookingId;
import io.ledgerstream.domain.model.BookingStatus;
import io.ledgerstream.domain.model.BookingType;
import io.ledgerstream.domain.model.CurrencyCode;
import io.ledgerstream.domain.model.IdempotencyKey;
import io.ledgerstream.domain.model.Money;
import io.ledgerstream.domain.model.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class BookingServiceTest {

    private static final CurrencyCode EUR = CurrencyCode.of("EUR");
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-01-01T12:00:00Z"), ZoneOffset.UTC);

    private InMemoryAccountRepository accountRepository;
    private InMemoryBookingRepository bookingRepository;
    private InMemoryIdempotencyStore idempotencyStore;
    private InMemoryOutboxRepository outboxRepository;
    private OutboxPayloadSerializer payloadSerializer;
    private AccountService accountService;
    private BookingService bookingService;

    @BeforeEach
    void setUp() {
        accountRepository = new InMemoryAccountRepository();
        bookingRepository = new InMemoryBookingRepository();
        idempotencyStore = new InMemoryIdempotencyStore();
        outboxRepository = new InMemoryOutboxRepository();
        payloadSerializer = event -> "{\"eventId\":\"" + event.eventId() + "\",\"eventType\":\"" + event.eventType() + "\"}";
        TransactionRunner directTransactionRunner = new TransactionRunner() {
            @Override
            public <T> T executeInTransaction(final Supplier<T> action) {
                return action.get();
            }
        };
        accountService = new AccountService(accountRepository, directTransactionRunner, FIXED_CLOCK);
        bookingService = new BookingService(
                accountRepository,
                bookingRepository,
                idempotencyStore,
                outboxRepository,
                payloadSerializer,
                directTransactionRunner,
                FIXED_CLOCK
        );
    }

    @Test
    void returnsByteIdenticalCachedResponseOnDuplicateIdempotencyKeyWithoutSecondEffectOrSecondOutboxEvent() {
        Account account = accountService.execute(new CreateAccountCommand(AccountNumber.of("DE-2001"), EUR));
        CreateBookingCommand command = new CreateBookingCommand(
                account.id(),
                BookingType.CREDIT,
                Money.of(new BigDecimal("100.00"), EUR),
                Reference.of("Salary"),
                IdempotencyKey.of("idem-dup-1")
        );

        IdempotentExecutionResult first = bookingService.execute(
                command,
                exec -> new StoredIdempotentResponse(201, "{\"id\":\"" + exec.booking().id().value() + "\"}")
        );
        IdempotentExecutionResult second = bookingService.execute(
                command,
                exec -> new StoredIdempotentResponse(201, "{\"should\":\"never-be-called\"}")
        );

        assertThat(first.replayed()).isFalse();
        assertThat(second.replayed()).isTrue();
        assertThat(second.response()).isEqualTo(first.response());
        assertThat(accountRepository.findById(account.id()).orElseThrow().balance())
                .isEqualTo(Money.of(new BigDecimal("100.00"), EUR));
        assertThat(bookingRepository.findByAccountId(account.id())).hasSize(1);
        assertThat(outboxRepository.savedEvents).hasSize(1);
        assertThat(outboxRepository.savedEvents.getFirst().status()).isEqualTo(OutboxStatus.PENDING);
    }

    @Test
    void doesNotCacheRejectedRequestsOrWriteOutboxEventSoRetryingAfterFundingSucceeds() {
        Account account = accountService.execute(new CreateAccountCommand(AccountNumber.of("DE-2002"), EUR));
        IdempotencyKey key = IdempotencyKey.of("idem-retry-after-fail");
        CreateBookingCommand debit = new CreateBookingCommand(
                account.id(),
                BookingType.DEBIT,
                Money.of(new BigDecimal("50.00"), EUR),
                Reference.of("Purchase"),
                key
        );

        assertThatThrownBy(() -> bookingService.execute(
                debit,
                exec -> new StoredIdempotentResponse(201, "{\"ok\":true}")
        )).isInstanceOf(InsufficientBalanceException.class);

        assertThat(idempotencyStore.find(account.id(), key)).isEmpty();
        assertThat(outboxRepository.savedEvents).isEmpty();

        bookingService.execute(
                new CreateBookingCommand(
                        account.id(),
                        BookingType.CREDIT,
                        Money.of(new BigDecimal("80.00"), EUR),
                        Reference.of("Top-up"),
                        IdempotencyKey.of("idem-fund")
                ),
                exec -> new StoredIdempotentResponse(201, "{\"funded\":true}")
        );

        IdempotentExecutionResult retry = bookingService.execute(
                debit,
                exec -> new StoredIdempotentResponse(201, "{\"debited\":true}")
        );

        assertThat(retry.replayed()).isFalse();
        assertThat(retry.response().responseBody()).isEqualTo("{\"debited\":true}");
        assertThat(accountRepository.findById(account.id()).orElseThrow().balance())
                .isEqualTo(Money.of(new BigDecimal("30.00"), EUR));
        assertThat(outboxRepository.savedEvents).hasSize(2);
    }

    @Test
    void concurrentLoserOnUniqueViolationReturnsWinnersStoredResponseWithoutConflict() {
        Account account = accountService.execute(new CreateAccountCommand(AccountNumber.of("DE-2003"), EUR));
        IdempotencyKey key = IdempotencyKey.of("idem-race-1");
        StoredIdempotentResponse winnerResponse = new StoredIdempotentResponse(201, "{\"winner\":true}");
        AtomicBoolean simulateRace = new AtomicBoolean(true);

        IdempotencyStore racingStore = new IdempotencyStore() {
            @Override
            public Optional<StoredIdempotentResponse> find(final AccountId accountId, final IdempotencyKey idempotencyKey) {
                if (simulateRace.get()) {
                    return Optional.empty();
                }
                return Optional.of(winnerResponse);
            }

            @Override
            public void save(
                    final AccountId accountId,
                    final IdempotencyKey idempotencyKey,
                    final BookingId bookingId,
                    final StoredIdempotentResponse response
            ) {
                simulateRace.set(false);
                throw new DuplicateIdempotencyKeyException(accountId, idempotencyKey);
            }
        };

        BookingService racingService = new BookingService(
                accountRepository,
                bookingRepository,
                racingStore,
                outboxRepository,
                payloadSerializer,
                new TransactionRunner() {
                    @Override
                    public <T> T executeInTransaction(final Supplier<T> action) {
                        return action.get();
                    }
                },
                FIXED_CLOCK
        );

        IdempotentExecutionResult result = racingService.execute(
                new CreateBookingCommand(
                        account.id(),
                        BookingType.CREDIT,
                        Money.of(new BigDecimal("25.00"), EUR),
                        Reference.of("Race"),
                        key
                ),
                exec -> new StoredIdempotentResponse(201, "{\"loser\":true}")
        );

        assertThat(result.replayed()).isTrue();
        assertThat(result.response()).isEqualTo(winnerResponse);
    }

    @Test
    void cancellationCreatesCounterBookingAndWritesBookingCancelledOutboxEvent() {
        Account account = accountService.execute(new CreateAccountCommand(AccountNumber.of("DE-2004"), EUR));
        final BookingId[] createdId = new BookingId[1];

        bookingService.execute(
                new CreateBookingCommand(
                        account.id(),
                        BookingType.CREDIT,
                        Money.of(new BigDecimal("100.00"), EUR),
                        Reference.of("Deposit"),
                        IdempotencyKey.of("idem-c1")
                ),
                exec -> {
                    createdId[0] = exec.booking().id();
                    return new StoredIdempotentResponse(201, "{\"id\":\"" + createdId[0].value() + "\"}");
                }
        );

        IdempotentExecutionResult cancelResult = bookingService.execute(
                new CancelBookingCommand(
                        account.id(),
                        createdId[0],
                        Reference.of("Customer reversal"),
                        IdempotencyKey.of("idem-c1-cancel")
                ),
                exec -> new StoredIdempotentResponse(201, "{\"counter\":\"" + exec.booking().id().value() + "\"}")
        );

        assertThat(cancelResult.replayed()).isFalse();
        assertThat(accountRepository.findById(account.id()).orElseThrow().balance()).isEqualTo(Money.zero(EUR));
        List<Booking> allBookings = bookingRepository.findByAccountId(account.id());
        assertThat(allBookings).hasSize(2);
        assertThat(allBookings)
                .anyMatch(b -> b.id().equals(createdId[0]) && b.status() == BookingStatus.CANCELLED)
                .anyMatch(b -> createdId[0].equals(b.originalBookingId())
                        && b.type() == BookingType.DEBIT
                        && b.status() == BookingStatus.POSTED);
        assertThat(outboxRepository.savedEvents)
                .extracting(OutboxEvent::eventType)
                .containsExactly("BookingCreated", "BookingCancelled");
    }

    private static final class InMemoryAccountRepository implements AccountRepository {
        private final Map<AccountId, Account> store = new HashMap<>();

        @Override
        public Account saveNew(final Account account) {
            store.put(account.id(), account);
            return account;
        }

        @Override
        public Account updateWithOptimisticLock(final Account account, final long expectedVersion) {
            Account existing = store.get(account.id());
            if (existing == null || existing.version() != expectedVersion) {
                throw new OptimisticLockConflictException(account.id(), expectedVersion);
            }
            store.put(account.id(), account);
            return account;
        }

        @Override
        public Optional<Account> findById(final AccountId id) {
            return Optional.ofNullable(store.get(id));
        }

        @Override
        public List<Account> findAll() {
            return new ArrayList<>(store.values());
        }
    }

    private static final class InMemoryBookingRepository implements BookingRepository {
        private final List<Booking> bookings = new ArrayList<>();

        @Override
        public Booking saveNew(final Booking booking) {
            bookings.add(booking);
            return booking;
        }

        @Override
        public void markCancelled(final BookingId bookingId) {
            for (int i = 0; i < bookings.size(); i++) {
                Booking current = bookings.get(i);
                if (current.id().equals(bookingId)) {
                    bookings.set(i, new Booking(
                            current.id(),
                            current.accountId(),
                            current.type(),
                            current.amount(),
                            current.reference(),
                            BookingStatus.CANCELLED,
                            current.idempotencyKey(),
                            current.originalBookingId(),
                            current.createdAt()
                    ));
                }
            }
        }

        @Override
        public Optional<Booking> findByIdAndAccountId(final BookingId bookingId, final AccountId accountId) {
            return bookings.stream()
                    .filter(b -> b.id().equals(bookingId) && b.accountId().equals(accountId))
                    .findFirst();
        }

        @Override
        public List<Booking> findByAccountId(final AccountId accountId) {
            return bookings.stream().filter(b -> b.accountId().equals(accountId)).toList();
        }
    }

    private static final class InMemoryIdempotencyStore implements IdempotencyStore {
        private final Map<String, StoredIdempotentResponse> store = new HashMap<>();

        @Override
        public Optional<StoredIdempotentResponse> find(final AccountId accountId, final IdempotencyKey key) {
            return Optional.ofNullable(store.get(accountId.value() + "::" + key.value()));
        }

        @Override
        public void save(
                final AccountId accountId,
                final IdempotencyKey key,
                final BookingId bookingId,
                final StoredIdempotentResponse response
        ) {
            String composite = accountId.value() + "::" + key.value();
            if (store.containsKey(composite)) {
                throw new DuplicateIdempotencyKeyException(accountId, key);
            }
            store.put(composite, response);
        }
    }

    private static final class InMemoryOutboxRepository implements OutboxRepository {
        private final List<OutboxEvent> savedEvents = new ArrayList<>();

        @Override
        public void save(final OutboxEvent event) {
            savedEvents.add(event);
        }

        @Override
        public List<UUID> findPendingIds(final int limit) {
            return savedEvents.stream().map(OutboxEvent::id).limit(limit).toList();
        }

        @Override
        public Optional<OutboxEvent> lockPendingById(final UUID eventId) {
            return savedEvents.stream().filter(e -> e.id().equals(eventId)).findFirst();
        }

        @Override
        public void markSent(final UUID eventId, final Instant sentAt) {
        }

        @Override
        public void recordFailure(final UUID eventId, final String errorMessage) {
        }

        @Override
        public long countPending() {
            return savedEvents.size();
        }
    }
}
