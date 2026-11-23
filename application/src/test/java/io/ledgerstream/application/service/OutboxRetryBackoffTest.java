package io.ledgerstream.application.service;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

final class OutboxRetryBackoffTest {

    @Test
    void computesExponentialBackoffStartingAt1sCappedAt60sWithMidpointJitter() {
        OutboxRetryBackoff midpoint = new OutboxRetryBackoff(() -> 0.5);

        assertThat(midpoint.delayForAttempt(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(midpoint.delayForAttempt(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(midpoint.delayForAttempt(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(midpoint.delayForAttempt(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(midpoint.delayForAttempt(5)).isEqualTo(Duration.ofSeconds(16));
        assertThat(midpoint.delayForAttempt(6)).isEqualTo(Duration.ofSeconds(32));
        assertThat(midpoint.delayForAttempt(7)).isEqualTo(Duration.ofSeconds(60));
        assertThat(midpoint.delayForAttempt(10)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void appliesPlusMinusTwentyPercentJitterBounds() {
        OutboxRetryBackoff minJitter = new OutboxRetryBackoff(() -> 0.0);
        OutboxRetryBackoff maxJitter = new OutboxRetryBackoff(() -> 1.0);

        assertThat(minJitter.delayForAttempt(1)).isEqualTo(Duration.ofMillis(800));
        assertThat(maxJitter.delayForAttempt(1)).isEqualTo(Duration.ofMillis(1200));

        assertThat(minJitter.delayForAttempt(7)).isEqualTo(Duration.ofSeconds(48));
        assertThat(maxJitter.delayForAttempt(7)).isEqualTo(Duration.ofSeconds(72));
    }
}
