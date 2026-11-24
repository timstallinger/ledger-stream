package io.ledgerstream.loadgen;

import java.util.Objects;
import java.util.SplittableRandom;

/**
 * Fixed-capacity ring buffer storing already accepted HTTP booking requests (accountId, Idempotency-Key,
 * exact JSON payload, and expected response body) for L2 genuine duplicate replay testing.
 */
public final class AcceptedRequestRingBuffer {

    public record AcceptedBooking(
            String accountId,
            String idempotencyKey,
            String payloadJson,
            String expectedResponseBody
    ) {
        public AcceptedBooking {
            Objects.requireNonNull(accountId, "accountId must not be null");
            Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
            Objects.requireNonNull(payloadJson, "payloadJson must not be null");
            Objects.requireNonNull(expectedResponseBody, "expectedResponseBody must not be null");
        }
    }

    private final AcceptedBooking[] buffer;
    private int writeIndex;
    private int count;

    public AcceptedRequestRingBuffer(final int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.buffer = new AcceptedBooking[capacity];
    }

    public synchronized void add(final AcceptedBooking booking) {
        Objects.requireNonNull(booking, "booking must not be null");
        buffer[writeIndex] = booking;
        writeIndex = (writeIndex + 1) % buffer.length;
        if (count < buffer.length) {
            count++;
        }
    }

    public synchronized AcceptedBooking sample(final SplittableRandom random) {
        if (count == 0) {
            return null;
        }
        int idx = random.nextInt(count);
        return buffer[idx];
    }

    public synchronized int size() {
        return count;
    }

    public int capacity() {
        return buffer.length;
    }
}
