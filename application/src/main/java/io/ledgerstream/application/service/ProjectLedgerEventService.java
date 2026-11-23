package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.EventProcessingOutcome;
import io.ledgerstream.application.port.in.IncomingLedgerEvent;
import io.ledgerstream.application.port.in.ProjectLedgerEventUseCase;
import io.ledgerstream.application.port.out.AccountRepository;
import io.ledgerstream.application.port.out.AccountSummaryRepository;
import io.ledgerstream.application.port.out.ConsumerEventObserver;
import io.ledgerstream.application.port.out.ProcessedEventRepository;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.BookingType;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public final class ProjectLedgerEventService implements ProjectLedgerEventUseCase {

    private static final String EVENT_TYPE_CANCELLED = "BookingCancelled";

    private final AccountRepository accountRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final AccountSummaryRepository accountSummaryRepository;
    private final ConsumerEventObserver observer;
    private final TransactionRunner transactionRunner;
    private final Clock clock;

    public ProjectLedgerEventService(
            final AccountRepository accountRepository,
            final ProcessedEventRepository processedEventRepository,
            final AccountSummaryRepository accountSummaryRepository,
            final ConsumerEventObserver observer,
            final TransactionRunner transactionRunner,
            final Clock clock
    ) {
        this.accountRepository = Objects.requireNonNull(accountRepository, "accountRepository must not be null");
        this.processedEventRepository = Objects.requireNonNull(
                processedEventRepository,
                "processedEventRepository must not be null"
        );
        this.accountSummaryRepository = Objects.requireNonNull(
                accountSummaryRepository,
                "accountSummaryRepository must not be null"
        );
        this.observer = Objects.requireNonNull(observer, "observer must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public EventProcessingOutcome process(final IncomingLedgerEvent event) {
        if (event == null || event.eventId() == null || event.aggregateId() == null || event.eventType() == null) {
            String key = event != null && event.eventId() != null ? event.eventId().toString() : "unknown";
            observer.onPoisonRejected(key, "missing_required_identifiers");
            return EventProcessingOutcome.POISON_REJECTED;
        }

        if (event.amount() == null || event.amount().compareTo(BigDecimal.ZERO) <= 0) {
            observer.onPoisonRejected(event.eventId().toString(), "non_positive_amount");
            return EventProcessingOutcome.POISON_REJECTED;
        }

        BookingType bookingType = parseBookingType(event.bookingType());
        if (bookingType == null) {
            observer.onPoisonRejected(event.eventId().toString(), "invalid_booking_type");
            return EventProcessingOutcome.POISON_REJECTED;
        }

        if (EVENT_TYPE_CANCELLED.equals(event.eventType()) && event.originalBookingId() == null) {
            observer.onPoisonRejected(event.eventId().toString(), "missing_original_booking_id");
            return EventProcessingOutcome.POISON_REJECTED;
        }

        AccountId accountId = AccountId.of(event.aggregateId());
        Optional<Account> accountOptional = accountRepository.findById(accountId);
        if (accountOptional.isEmpty()) {
            observer.onPoisonRejected(event.eventId().toString(), "unknown_account");
            return EventProcessingOutcome.POISON_REJECTED;
        }

        Account account = accountOptional.get();
        if (event.currency() == null || !account.balance().currency().value().equalsIgnoreCase(event.currency())) {
            observer.onPoisonRejected(event.eventId().toString(), "currency_mismatch");
            return EventProcessingOutcome.POISON_REJECTED;
        }

        Instant now = clock.instant();
        Instant occurredAt = event.occurredAt() != null ? event.occurredAt() : now;

        EventProcessingOutcome outcome = transactionRunner.executeInTransaction(() -> {
            boolean inserted = processedEventRepository.tryMarkProcessed(event.eventId(), event.eventType(), now);
            if (!inserted) {
                return EventProcessingOutcome.DUPLICATE_REJECTED;
            }
            accountSummaryRepository.applyBookingDelta(
                    account.id(),
                    account.accountNumber(),
                    account.balance().currency(),
                    bookingType,
                    event.amount(),
                    event.eventId(),
                    occurredAt,
                    now
            );
            return EventProcessingOutcome.PROCESSED;
        });

        if (outcome == EventProcessingOutcome.PROCESSED) {
            observer.onProcessed(event.eventId());
        } else if (outcome == EventProcessingOutcome.DUPLICATE_REJECTED) {
            observer.onDuplicateRejected(event.eventId());
        }
        return outcome;
    }

    private static BookingType parseBookingType(final String rawType) {
        if (rawType == null || rawType.isBlank()) {
            return null;
        }
        try {
            return BookingType.valueOf(rawType.trim().toUpperCase());
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }
}
