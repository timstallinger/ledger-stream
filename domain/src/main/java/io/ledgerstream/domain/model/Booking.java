package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.BookingAlreadyCancelledException;
import io.ledgerstream.domain.exception.InvalidDomainValueException;

import java.time.Instant;
import java.util.Objects;

public record Booking(
        BookingId id,
        AccountId accountId,
        BookingType type,
        Money amount,
        Reference reference,
        BookingStatus status,
        IdempotencyKey idempotencyKey,
        BookingId originalBookingId,
        Instant createdAt
) {

    public Booking {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(accountId, "accountId must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(reference, "reference must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        if (!amount.isPositive()) {
            throw new InvalidDomainValueException("Booking amount must be strictly positive: " + amount.amount());
        }
    }

    public static Booking post(
            final BookingId id,
            final AccountId accountId,
            final BookingType type,
            final Money amount,
            final Reference reference,
            final IdempotencyKey idempotencyKey,
            final Instant now
    ) {
        return new Booking(id, accountId, type, amount, reference, BookingStatus.POSTED, idempotencyKey, null, now);
    }

    public CancellationPair cancel(
            final BookingId counterBookingId,
            final IdempotencyKey cancellationKey,
            final Reference reason,
            final Instant now
    ) {
        if (this.status == BookingStatus.CANCELLED) {
            throw new BookingAlreadyCancelledException(this.id);
        }
        if (this.originalBookingId != null) {
            throw new InvalidDomainValueException("Counter-booking cannot be cancelled: " + this.id.value());
        }

        Booking cancelledOriginal = new Booking(
                this.id,
                this.accountId,
                this.type,
                this.amount,
                this.reference,
                BookingStatus.CANCELLED,
                this.idempotencyKey,
                null,
                this.createdAt
        );
        Booking counterBooking = new Booking(
                counterBookingId,
                this.accountId,
                this.type.opposite(),
                this.amount,
                reason,
                BookingStatus.POSTED,
                cancellationKey,
                this.id,
                now
        );
        return new CancellationPair(cancelledOriginal, counterBooking);
    }

    public record CancellationPair(Booking cancelledOriginal, Booking counterBooking) {
    }
}
