package io.ledgerstream.domain.model;

import io.ledgerstream.domain.exception.InsufficientBalanceException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class AccountTest {

    private static final CurrencyCode EUR = CurrencyCode.of("EUR");
    private static final Instant T0 = Instant.parse("2026-01-01T10:00:00Z");
    private static final Instant T1 = Instant.parse("2026-01-01T10:01:00Z");
    private static final Instant T2 = Instant.parse("2026-01-01T10:02:00Z");

    @Test
    void opensAccountWithZeroBalanceAndVersionZero() {
        Account account = Account.open(AccountId.generate(), AccountNumber.of("DE-1001"), EUR, T0);

        assertThat(account.balance()).isEqualTo(Money.zero(EUR));
        assertThat(account.version()).isZero();
        assertThat(account.createdAt()).isEqualTo(T0);
        assertThat(account.updatedAt()).isEqualTo(T0);
    }

    @Test
    void creditIncreasesBalanceAndIncrementsVersion() {
        Account account = Account.open(AccountId.generate(), AccountNumber.of("DE-1002"), EUR, T0);
        Booking credit = Booking.post(
                BookingId.generate(),
                account.id(),
                BookingType.CREDIT,
                Money.of(new BigDecimal("250.00"), EUR),
                Reference.of("Initial deposit"),
                IdempotencyKey.of("idem-credit-1"),
                T1
        );

        Account updated = account.apply(credit, T1);

        assertThat(updated.balance()).isEqualTo(Money.of(new BigDecimal("250.00"), EUR));
        assertThat(updated.version()).isEqualTo(1L);
        assertThat(updated.updatedAt()).isEqualTo(T1);
    }

    @Test
    void debitDownToExactZeroIsAllowed() {
        Account account = Account.open(AccountId.generate(), AccountNumber.of("DE-1003"), EUR, T0);
        Booking credit = Booking.post(
                BookingId.generate(),
                account.id(),
                BookingType.CREDIT,
                Money.of(new BigDecimal("100.00"), EUR),
                Reference.of("Deposit"),
                IdempotencyKey.of("idem-1"),
                T1
        );
        Account funded = account.apply(credit, T1);

        Booking debit = Booking.post(
                BookingId.generate(),
                account.id(),
                BookingType.DEBIT,
                Money.of(new BigDecimal("100.00"), EUR),
                Reference.of("Full withdrawal"),
                IdempotencyKey.of("idem-2"),
                T2
        );
        Account emptied = funded.apply(debit, T2);

        assertThat(emptied.balance()).isEqualTo(Money.zero(EUR));
        assertThat(emptied.version()).isEqualTo(2L);
    }

    @Test
    void debitExceedingBalanceIsRejectedWithInsufficientBalanceException() {
        Account account = Account.open(AccountId.generate(), AccountNumber.of("DE-1004"), EUR, T0);
        Booking credit = Booking.post(
                BookingId.generate(),
                account.id(),
                BookingType.CREDIT,
                Money.of(new BigDecimal("50.00"), EUR),
                Reference.of("Deposit"),
                IdempotencyKey.of("idem-1"),
                T1
        );
        Account funded = account.apply(credit, T1);

        Booking overdraft = Booking.post(
                BookingId.generate(),
                account.id(),
                BookingType.DEBIT,
                Money.of(new BigDecimal("50.01"), EUR),
                Reference.of("Too large"),
                IdempotencyKey.of("idem-2"),
                T2
        );

        assertThatThrownBy(() -> funded.apply(overdraft, T2))
                .isInstanceOf(InsufficientBalanceException.class)
                .hasMessageContaining("Insufficient balance");
    }
}
