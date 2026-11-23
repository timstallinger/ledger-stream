package io.ledgerstream.application.port.out;

import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.AccountNumber;
import io.ledgerstream.domain.model.BookingType;
import io.ledgerstream.domain.model.CurrencyCode;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface AccountSummaryRepository {

    void applyBookingDelta(
            AccountId accountId,
            AccountNumber accountNumber,
            CurrencyCode currency,
            BookingType bookingType,
            BigDecimal amount,
            UUID eventId,
            Instant occurredAt,
            Instant updatedAt
    );

    Optional<AccountSummary> findByAccountId(AccountId accountId);

    List<AccountSummary> findAll();
}
