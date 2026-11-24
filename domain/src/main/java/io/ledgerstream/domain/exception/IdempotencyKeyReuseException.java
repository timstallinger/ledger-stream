package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.IdempotencyKey;

public final class IdempotencyKeyReuseException extends DomainException {

    public IdempotencyKeyReuseException(final AccountId accountId, final IdempotencyKey idempotencyKey) {
        super(
                "IDEMPOTENCY_KEY_REUSE",
                "Idempotency key '" + idempotencyKey.value()
                        + "' was already used on account " + accountId.value()
                        + " with a different request payload"
        );
    }
}
