package io.ledgerstream.application.port.out;

import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.Booking;
import io.ledgerstream.domain.model.BookingId;

import java.util.List;
import java.util.Optional;

public interface BookingRepository {

    Booking saveNew(Booking booking);

    void markCancelled(BookingId bookingId);

    Optional<Booking> findByIdAndAccountId(BookingId bookingId, AccountId accountId);

    List<Booking> findByAccountId(AccountId accountId);
}
