package io.ledgerstream.application.port.out;

import java.util.function.Supplier;

public interface TransactionRunner {

    <T> T executeInTransaction(Supplier<T> action);

    default void runInTransaction(final Runnable action) {
        executeInTransaction(() -> {
            action.run();
            return null;
        });
    }
}
