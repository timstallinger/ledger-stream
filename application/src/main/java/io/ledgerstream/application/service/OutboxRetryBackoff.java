package io.ledgerstream.application.service;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

public final class OutboxRetryBackoff {

    public static final Duration INITIAL_DELAY = Duration.ofSeconds(1);
    public static final Duration MAX_DELAY = Duration.ofSeconds(60);
    public static final double JITTER_RATIO = 0.20;

    private final DoubleSupplier randomUnit;

    public OutboxRetryBackoff() {
        this(() -> ThreadLocalRandom.current().nextDouble());
    }

    public OutboxRetryBackoff(final DoubleSupplier randomUnit) {
        this.randomUnit = Objects.requireNonNull(randomUnit, "randomUnit must not be null");
    }

    public Duration delayForAttempt(final int attemptNumber) {
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber must be >= 1: " + attemptNumber);
        }
        int exponent = Math.min(attemptNumber - 1, 10);
        long baseMillis = INITIAL_DELAY.toMillis() * (1L << exponent);
        long cappedMillis = Math.min(baseMillis, MAX_DELAY.toMillis());
        double unit = Math.clamp(randomUnit.getAsDouble(), 0.0, 1.0);
        double factor = (1.0 - JITTER_RATIO) + (2.0 * JITTER_RATIO * unit);
        long jitteredMillis = Math.max(1L, Math.round(cappedMillis * factor));
        return Duration.ofMillis(jitteredMillis);
    }
}
