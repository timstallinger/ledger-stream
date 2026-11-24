package io.ledgerstream.loadgen;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lock-free exact latency recorder (in nanoseconds) backed by a pre-allocated array.
 */
public final class LatencyRecorder {

    public record Percentiles(int count, double p50Ms, double p95Ms, double p99Ms, double maxMs) {
    }

    private final long[] samplesNanos;
    private final AtomicInteger index = new AtomicInteger(0);

    public LatencyRecorder(final int maxSamples) {
        if (maxSamples <= 0) {
            throw new IllegalArgumentException("maxSamples must be positive");
        }
        this.samplesNanos = new long[maxSamples];
    }

    public void recordNanos(final long durationNanos) {
        int slot = index.getAndIncrement();
        if (slot < samplesNanos.length) {
            samplesNanos[slot] = Math.max(0L, durationNanos);
        }
    }

    public Percentiles snapshot() {
        int count = Math.min(index.get(), samplesNanos.length);
        if (count == 0) {
            return new Percentiles(0, 0.0, 0.0, 0.0, 0.0);
        }
        long[] copy = Arrays.copyOf(samplesNanos, count);
        Arrays.sort(copy);
        return new Percentiles(
                count,
                percentileMs(copy, 0.50),
                percentileMs(copy, 0.95),
                percentileMs(copy, 0.99),
                copy[copy.length - 1] / 1_000_000.0
        );
    }

    private static double percentileMs(final long[] sorted, final double quantile) {
        int rank = (int) Math.ceil(quantile * sorted.length) - 1;
        int clamped = Math.max(0, Math.min(sorted.length - 1, rank));
        return sorted[clamped] / 1_000_000.0;
    }
}
