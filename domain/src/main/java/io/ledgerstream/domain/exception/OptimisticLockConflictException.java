package io.ledgerstream.domain.exception;

import io.ledgerstream.domain.DomainException;
import io.ledgerstream.domain.model.AccountId;

public final class OptimisticLockConflictException extends DomainException {

    public OptimisticLockConflictException(final AccountId accountId, final long expectedVersion) {
        super(
                "OPTIMISTIC_LOCK_CONFLICT",
                "Concurrent modification detected on account " + accountId.value() + " at version " + expectedVersion
        );
    }
}
