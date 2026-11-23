package io.ledgerstream.application.port.out;

import io.ledgerstream.application.port.in.StoredIdempotentResponse;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.BookingId;
import io.ledgerstream.domain.model.IdempotencyKey;

import java.util.Optional;

public interface IdempotencyStore {

    Optional<StoredIdempotentResponse> find(AccountId accountId, IdempotencyKey idempotencyKey);

    void save(
            AccountId accountId,
            IdempotencyKey idempotencyKey,
            BookingId bookingId,
            StoredIdempotentResponse response
    );
}
