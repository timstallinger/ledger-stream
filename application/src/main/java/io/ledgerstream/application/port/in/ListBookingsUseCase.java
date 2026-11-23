package io.ledgerstream.application.port.in;

import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.Booking;

import java.util.List;

public interface ListBookingsUseCase {

    List<Booking> listByAccountId(AccountId accountId);
}
