package io.ledgerstream.loadgen;

import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

final class AcceptedRequestRingBufferTest {

    @Test
    void overwritesOldestEntriesAndSamplesAcceptedPayloadsDeterministically() {
        AcceptedRequestRingBuffer buffer = new AcceptedRequestRingBuffer(3);
        assertThat(buffer.sample(new SplittableRandom(1))).isNull();

        for (int i = 0; i < 5; i++) {
            buffer.add(new AcceptedRequestRingBuffer.AcceptedBooking(
                    "acc-" + i,
                    "key-" + i,
                    "{\"amount\":\"" + (i + 1) + ".00\"}",
                    "{\"id\":\"b-" + i + "\"}"
            ));
        }

        assertThat(buffer.size()).isEqualTo(3);
        AcceptedRequestRingBuffer.AcceptedBooking sample1 = buffer.sample(new SplittableRandom(99));
        AcceptedRequestRingBuffer.AcceptedBooking sample2 = buffer.sample(new SplittableRandom(99));
        assertThat(sample1).isEqualTo(sample2);
        assertThat(sample1.idempotencyKey()).isIn("key-2", "key-3", "key-4");
    }
}
