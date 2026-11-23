package io.ledgerstream.adapter.out.persistence;

import io.ledgerstream.application.port.out.TransactionRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Objects;
import java.util.function.Supplier;

public final class SpringTransactionRunner implements TransactionRunner {

    private final TransactionTemplate transactionTemplate;

    public SpringTransactionRunner(final PlatformTransactionManager transactionManager) {
        Objects.requireNonNull(transactionManager, "transactionManager must not be null");
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    }

    @Override
    public <T> T executeInTransaction(final Supplier<T> action) {
        Objects.requireNonNull(action, "action must not be null");
        return transactionTemplate.execute(status -> action.get());
    }
}
