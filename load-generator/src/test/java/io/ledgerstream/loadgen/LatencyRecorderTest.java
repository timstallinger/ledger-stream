package io.ledgerstream.loadgen;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

final class LatencyRecorderTest {

    @Test
    void calculatesExactP50P95P99InMilliseconds() {
        LatencyRecorder recorder = new LatencyRecorder(100);
        for (int ms = 1; ms <= 100; ms++) {
            recorder.recordNanos(ms * 1_000_000L);
        }

        LatencyRecorder.Percentiles p = recorder.snapshot();
        assertThat(p.count()).isEqualTo(100);
        assertThat(p.p50Ms()).isCloseTo(50.0, within(0.001));
        assertThat(p.p95Ms()).isCloseTo(95.0, within(0.001));
        assertThat(p.p99Ms()).isCloseTo(99.0, within(0.001));
        assertThat(p.maxMs()).isCloseTo(100.0, within(0.001));
    }
}
