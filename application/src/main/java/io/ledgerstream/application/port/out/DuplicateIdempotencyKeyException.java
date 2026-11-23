package io.ledgerstream.application.port.out;

import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.IdempotencyKey;

public final class DuplicateIdempotencyKeyException extends RuntimeException {

    public DuplicateIdempotencyKeyException(final AccountId accountId, final IdempotencyKey key) {
        super("Duplicate idempotency key '" + key.value() + "' for account " + accountId.value());
    }
}
