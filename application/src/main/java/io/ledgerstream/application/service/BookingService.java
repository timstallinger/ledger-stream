package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.BookingExecution;
import io.ledgerstream.application.port.in.CancelBookingCommand;
import io.ledgerstream.application.port.in.CancelBookingUseCase;
import io.ledgerstream.application.port.in.CreateBookingCommand;
import io.ledgerstream.application.port.in.CreateBookingUseCase;
import io.ledgerstream.application.port.in.IdempotentExecutionResult;
import io.ledgerstream.application.port.in.ListBookingsUseCase;
import io.ledgerstream.application.port.in.StoredIdempotentResponse;
import io.ledgerstream.application.port.out.AccountRepository;
import io.ledgerstream.application.port.out.BookingRepository;
import io.ledgerstream.application.port.out.DuplicateIdempotencyKeyException;
import io.ledgerstream.application.port.out.IdempotencyRecord;
import io.ledgerstream.application.port.out.IdempotencyStore;
import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxPayloadSerializer;
import io.ledgerstream.application.port.out.OutboxRepository;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.domain.event.BookingCancelledEvent;
import io.ledgerstream.domain.event.BookingCreatedEvent;
import io.ledgerstream.domain.exception.AccountNotFoundException;
import io.ledgerstream.domain.exception.BookingNotFoundException;
import io.ledgerstream.domain.exception.IdempotencyKeyReuseException;
import io.ledgerstream.domain.exception.OptimisticLockConflictException;
import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.Booking;
import io.ledgerstream.domain.model.BookingId;
import io.ledgerstream.domain.model.IdempotencyKey;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

public final class BookingService implements CreateBookingUseCase, CancelBookingUseCase, ListBookingsUseCase {

    private final AccountRepository accountRepository;
    private final BookingRepository bookingRepository;
    private final IdempotencyStore idempotencyStore;
    private final OutboxRepository outboxRepository;
    private final OutboxPayloadSerializer outboxPayloadSerializer;
    private final TransactionRunner transactionRunner;
    private final Clock clock;

    public BookingService(
            final AccountRepository accountRepository,
            final BookingRepository bookingRepository,
            final IdempotencyStore idempotencyStore,
            final OutboxRepository outboxRepository,
            final OutboxPayloadSerializer outboxPayloadSerializer,
            final TransactionRunner transactionRunner,
            final Clock clock
    ) {
        this.accountRepository = Objects.requireNonNull(accountRepository, "accountRepository must not be null");
        this.bookingRepository = Objects.requireNonNull(bookingRepository, "bookingRepository must not be null");
        this.idempotencyStore = Objects.requireNonNull(idempotencyStore, "idempotencyStore must not be null");
        this.outboxRepository = Objects.requireNonNull(outboxRepository, "outboxRepository must not be null");
        this.outboxPayloadSerializer = Objects.requireNonNull(
                outboxPayloadSerializer,
                "outboxPayloadSerializer must not be null"
        );
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public IdempotentExecutionResult execute(
            final CreateBookingCommand command,
            final Function<BookingExecution, StoredIdempotentResponse> responseSerializer
    ) {
        Objects.requireNonNull(command, "command must not be null");
        Objects.requireNonNull(responseSerializer, "responseSerializer must not be null");

        String fingerprint = RequestFingerprint.forCreateBooking(command);
        return executeIdempotently(
                command.accountId(),
                command.idempotencyKey(),
                fingerprint,
                () -> executeNewBooking(command, fingerprint, responseSerializer)
        );
    }

    @Override
    public IdempotentExecutionResult execute(
            final CancelBookingCommand command,
            final Function<BookingExecution, StoredIdempotentResponse> responseSerializer
    ) {
        Objects.requireNonNull(command, "command must not be null");
        Objects.requireNonNull(responseSerializer, "responseSerializer must not be null");

        String fingerprint = RequestFingerprint.forCancelBooking(command);
        return executeIdempotently(
                command.accountId(),
                command.idempotencyKey(),
                fingerprint,
                () -> executeCancellation(command, fingerprint, responseSerializer)
        );
    }

    @Override
    public List<Booking> listByAccountId(final AccountId accountId) {
        Objects.requireNonNull(accountId, "accountId must not be null");
        accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException(accountId));
        return bookingRepository.findByAccountId(accountId);
    }

    private IdempotentExecutionResult executeIdempotently(
            final AccountId accountId,
            final IdempotencyKey idempotencyKey,
            final String expectedFingerprint,
            final Supplier<IdempotentExecutionResult> transactionalWork
    ) {
        try {
            return transactionRunner.executeInTransaction(transactionalWork);
        } catch (DuplicateIdempotencyKeyException | OptimisticLockConflictException raceException) {
            Optional<IdempotencyRecord> winner = idempotencyStore.find(accountId, idempotencyKey);
            if (winner.isPresent()) {
                return verifyFingerprintOrThrow(accountId, idempotencyKey, expectedFingerprint, winner.get());
            }
            throw raceException;
        }
    }

    private IdempotentExecutionResult executeNewBooking(
            final CreateBookingCommand command,
            final String fingerprint,
            final Function<BookingExecution, StoredIdempotentResponse> responseSerializer
    ) {
        Optional<IdempotencyRecord> existing = idempotencyStore.find(
                command.accountId(),
                command.idempotencyKey()
        );
        if (existing.isPresent()) {
            return verifyFingerprintOrThrow(command.accountId(), command.idempotencyKey(), fingerprint, existing.get());
        }

        Account account = accountRepository.findById(command.accountId())
                .orElseThrow(() -> new AccountNotFoundException(command.accountId()));

        Instant now = clock.instant();
        Booking booking = Booking.post(
                BookingId.generate(),
                account.id(),
                command.type(),
                command.amount(),
                command.reference(),
                command.idempotencyKey(),
                now
        );

        Account updatedAccount = account.apply(booking, now);
        accountRepository.updateWithOptimisticLock(updatedAccount, account.version());
        bookingRepository.saveNew(booking);

        BookingCreatedEvent event = BookingCreatedEvent.of(
                UUID.randomUUID(),
                booking,
                updatedAccount.version(),
                now
        );
        String serializedPayload = outboxPayloadSerializer.serialize(event);
        outboxRepository.save(OutboxEvent.pending(event, serializedPayload, command.correlationId(), now));

        StoredIdempotentResponse storedResponse = responseSerializer.apply(
                new BookingExecution(booking, updatedAccount, event)
        );
        idempotencyStore.save(account.id(), command.idempotencyKey(), booking.id(), fingerprint, storedResponse);
        return new IdempotentExecutionResult(storedResponse, false);
    }

    private IdempotentExecutionResult executeCancellation(
            final CancelBookingCommand command,
            final String fingerprint,
            final Function<BookingExecution, StoredIdempotentResponse> responseSerializer
    ) {
        Optional<IdempotencyRecord> existing = idempotencyStore.find(
                command.accountId(),
                command.idempotencyKey()
        );
        if (existing.isPresent()) {
            return verifyFingerprintOrThrow(command.accountId(), command.idempotencyKey(), fingerprint, existing.get());
        }

        Account account = accountRepository.findById(command.accountId())
                .orElseThrow(() -> new AccountNotFoundException(command.accountId()));
        Booking original = bookingRepository.findByIdAndAccountId(command.bookingId(), command.accountId())
                .orElseThrow(() -> new BookingNotFoundException(command.bookingId()));

        Instant now = clock.instant();
        Booking.CancellationPair pair = original.cancel(
                BookingId.generate(),
                command.idempotencyKey(),
                command.reference(),
                now
        );

        Account updatedAccount = account.apply(pair.counterBooking(), now);
        accountRepository.updateWithOptimisticLock(updatedAccount, account.version());
        bookingRepository.markCancelled(pair.cancelledOriginal().id());
        bookingRepository.saveNew(pair.counterBooking());

        BookingCancelledEvent event = BookingCancelledEvent.of(
                UUID.randomUUID(),
                pair.cancelledOriginal(),
                pair.counterBooking(),
                updatedAccount.version(),
                now
        );
        String serializedPayload = outboxPayloadSerializer.serialize(event);
        outboxRepository.save(OutboxEvent.pending(event, serializedPayload, command.correlationId(), now));

        StoredIdempotentResponse storedResponse = responseSerializer.apply(
                new BookingExecution(pair.counterBooking(), updatedAccount, event)
        );
        idempotencyStore.save(
                account.id(),
                command.idempotencyKey(),
                pair.counterBooking().id(),
                fingerprint,
                storedResponse
        );
        return new IdempotentExecutionResult(storedResponse, false);
    }

    private static IdempotentExecutionResult verifyFingerprintOrThrow(
            final AccountId accountId,
            final IdempotencyKey idempotencyKey,
            final String expectedFingerprint,
            final IdempotencyRecord existing
    ) {
        if (!existing.requestFingerprint().equals(expectedFingerprint)) {
            throw new IdempotencyKeyReuseException(accountId, idempotencyKey);
        }
        return new IdempotentExecutionResult(existing.response(), true);
    }
}
