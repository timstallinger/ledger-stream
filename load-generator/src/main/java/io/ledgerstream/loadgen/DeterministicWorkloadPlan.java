package io.ledgerstream.loadgen;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * Deterministic workload partitioning and payload generation driven strictly by --seed.
 * Two runs with the same (seed, bookings, duplicates, poison, threads, accounts) generate
 * identical cohort counts, account numbers, booking types, amounts, idempotency keys,
 * and poison event IDs.
 */
public final class DeterministicWorkloadPlan {

    public record WorkerAllocation(
            int workerIndex,
            long workerSeed,
            int regularCount,
            int duplicateCount,
            int poisonCount
    ) {
    }

    public record BookingSpec(
            String type,
            BigDecimal amount,
            String idempotencyKey,
            String reference,
            String correlationId
    ) {
    }

    private DeterministicWorkloadPlan() {
    }

    public static List<WorkerAllocation> allocateWorkers(
            final long seed,
            final int totalRegular,
            final int totalDuplicates,
            final int totalPoison,
            final int threads
    ) {
        List<WorkerAllocation> result = new ArrayList<>(threads);
        for (int w = 0; w < threads; w++) {
            int reg = share(totalRegular, threads, w);
            int dup = share(totalDuplicates, threads, w);
            int poi = share(totalPoison, threads, w);
            long workerSeed = seed ^ ((w + 1L) * 0x9E3779B97F4A7C15L);
            result.add(new WorkerAllocation(w, workerSeed, reg, dup, poi));
        }
        return result;
    }

    public static String accountNumberForIndex(final long seed, final int index) {
        String seedHex = Long.toUnsignedString(seed, 16);
        return "DE-BENCH-" + seedHex + "-" + String.format("%04d", index);
    }

    public static String warmupAccountNumberForIndex(final long seed, final int index) {
        String seedHex = Long.toUnsignedString(seed, 16);
        return "DE-WARM-" + seedHex + "-" + String.format("%03d", index);
    }

    public static BookingSpec nextRegularBooking(
            final SplittableRandom random,
            final long seed,
            final int workerIndex,
            final int sequenceNumber,
            final BigDecimal currentAccountBalance
    ) {
        int cents = 100 + random.nextInt(9900); // 1.00 .. 99.99 EUR
        BigDecimal amount = BigDecimal.valueOf(cents, 2).setScale(2, RoundingMode.UNNECESSARY);
        boolean chooseDebit = random.nextInt(100) < 35 && currentAccountBalance.compareTo(amount) >= 0;
        String type = chooseDebit ? "DEBIT" : "CREDIT";
        String idempotencyKey = "idem-" + seed + "-w" + workerIndex + "-seq" + sequenceNumber;
        String reference = "LoadTest s=" + seed + " w=" + workerIndex + " #" + sequenceNumber;
        String correlationId = "lg-" + Long.toUnsignedString(seed, 36) + "-w" + workerIndex + "-" + sequenceNumber;
        return new BookingSpec(type, amount, idempotencyKey, reference, correlationId);
    }

    public static UUID deterministicUuid(final SplittableRandom random) {
        long msb = (random.nextLong() & 0xFFFFFFFFFFFF0FFFL) | 0x0000000000004000L;
        long lsb = (random.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb);
    }

    private static int share(final int total, final int workers, final int index) {
        int base = total / workers;
        int remainder = total % workers;
        return base + (index < remainder ? 1 : 0);
    }
}
