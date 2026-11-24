package io.ledgerstream.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multi-threaded HTTP/1.1 Keep-Alive + Kafka Poison load runner implementing:
 *  - Isolated warmup phase (default 5s, excluded from throughput/latency stats)
 *  - Cohort 1: Regular bookings via HTTP POST -> 201 Created
 *  - Cohort 2 (L2): Genuine replays of previously accepted requests from ring buffer
 *                   (same Idempotency-Key + identical payload -> 201 + Idempotency-Replayed: true)
 *  - Cohort 3: Schema-valid Avro v2 poison events directly to Kafka ledger.events
 */
public final class LoadRunner {

    public record AccountHandle(UUID id, String accountNumber) {
    }

    public record RunResult(
            long seed,
            int threads,
            int warmupSeconds,
            long warmupAccepted,
            long regularSent,
            long regularAccepted,
            long duplicateSent,
            long duplicateReplayed,
            long duplicateMismatches,
            long validationRejected,
            long unexpectedHttpErrors,
            long poisonSent,
            long poisonUnknownAccount,
            long poisonNonPositiveAmount,
            double durationSeconds,
            LatencyRecorder.Percentiles regularLatency,
            LatencyRecorder.Percentiles allHttpLatency,
            List<AccountHandle> benchmarkAccounts,
            List<AccountHandle> warmupAccounts
    ) {
        public double httpSentPerSecond() {
            return durationSeconds > 0.0 ? (regularSent + duplicateSent) / durationSeconds : 0.0;
        }

        public double regularAcceptedPerSecond() {
            return durationSeconds > 0.0 ? regularAccepted / durationSeconds : 0.0;
        }
    }

    private final LoadGeneratorArgs args;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final OidcTokenProvider tokenProvider;
    private final PoisonEventProducer poisonEventProducer;

    public LoadRunner(
            final LoadGeneratorArgs args,
            final HttpClient httpClient,
            final ObjectMapper objectMapper,
            final OidcTokenProvider tokenProvider,
            final PoisonEventProducer poisonEventProducer
    ) {
        this.args = args;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.tokenProvider = tokenProvider;
        this.poisonEventProducer = poisonEventProducer;
    }

    public RunResult execute() throws Exception {
        // 1. Pre-create deterministic benchmark & warmup accounts
        int warmupAccountCount = args.warmupSeconds() > 0 ? Math.min(16, args.threads()) : 0;
        System.out.printf(
                Locale.ROOT,
                "[setup] Creating %d benchmark accounts and %d warmup accounts (seed=%d)...%n",
                args.accounts(),
                warmupAccountCount,
                args.seed()
        );
        List<AccountHandle> benchmarkAccounts = new ArrayList<>(args.accounts());
        for (int i = 0; i < args.accounts(); i++) {
            String accNum = DeterministicWorkloadPlan.accountNumberForIndex(args.seed(), i);
            benchmarkAccounts.add(createAccount(accNum));
        }
        List<AccountHandle> warmupAccounts = new ArrayList<>(warmupAccountCount);
        for (int i = 0; i < warmupAccountCount; i++) {
            String accNum = DeterministicWorkloadPlan.warmupAccountNumberForIndex(args.seed(), i);
            warmupAccounts.add(createAccount(accNum));
        }

        // 2. Warmup phase (strictly excluded from benchmark throughput and latency!)
        long warmupAccepted = 0L;
        if (args.warmupSeconds() > 0) {
            warmupAccepted = runWarmupPhase(warmupAccounts, args.warmupSeconds());
            System.out.printf(
                    Locale.ROOT,
                    "[warmup] Completed %ds warmup: %d warmup bookings accepted (excluded from stats).%n",
                    args.warmupSeconds(),
                    warmupAccepted
            );
        }

        // 3. Measured main run across 3 cohorts
        int totalRegular = args.bookings();
        int totalDuplicates = args.duplicateCount();
        int totalPoison = args.poisonCount();
        List<DeterministicWorkloadPlan.WorkerAllocation> allocations = DeterministicWorkloadPlan.allocateWorkers(
                args.seed(),
                totalRegular,
                totalDuplicates,
                totalPoison,
                args.threads()
        );

        LatencyRecorder regularLatency = new LatencyRecorder(totalRegular);
        LatencyRecorder allHttpLatency = new LatencyRecorder(totalRegular + totalDuplicates);

        AtomicLong regularSent = new AtomicLong();
        AtomicLong regularAccepted = new AtomicLong();
        AtomicLong duplicateSent = new AtomicLong();
        AtomicLong duplicateReplayed = new AtomicLong();
        AtomicLong duplicateMismatches = new AtomicLong();
        AtomicLong validationRejected = new AtomicLong();
        AtomicLong unexpectedHttpErrors = new AtomicLong();
        AtomicLong poisonSent = new AtomicLong();
        AtomicLong poisonUnknownAccount = new AtomicLong();
        AtomicLong poisonNonPositiveAmount = new AtomicLong();

        RateLimiter rateLimiter = args.ratePerSecond() > 0.0 ? new RateLimiter(args.ratePerSecond()) : null;

        System.out.printf(
                Locale.ROOT,
                "[run] Starting measured load: regular=%d duplicates=%d (%.1f%%) poison=%d (%.1f%%) threads=%d seed=%d%n",
                totalRegular,
                totalDuplicates,
                args.duplicateRatio() * 100.0,
                totalPoison,
                args.poisonRatio() * 100.0,
                args.threads(),
                args.seed()
        );

        long startNanos = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(args.threads());
        try {
            List<Callable<Void>> tasks = new ArrayList<>(args.threads());
            for (DeterministicWorkloadPlan.WorkerAllocation alloc : allocations) {
                List<AccountHandle> workerAccounts = sliceAccountsForWorker(benchmarkAccounts, alloc.workerIndex(), args.threads());
                tasks.add(() -> {
                    runWorker(
                            alloc,
                            workerAccounts,
                            rateLimiter,
                            regularLatency,
                            allHttpLatency,
                            regularSent,
                            regularAccepted,
                            duplicateSent,
                            duplicateReplayed,
                            duplicateMismatches,
                            validationRejected,
                            unexpectedHttpErrors,
                            poisonSent,
                            poisonUnknownAccount,
                            poisonNonPositiveAmount
                    );
                    return null;
                });
            }

            List<Future<Void>> futures = pool.invokeAll(tasks);
            for (Future<Void> future : futures) {
                future.get();
            }
            if (poisonEventProducer != null) {
                poisonEventProducer.flush();
            }
        } finally {
            pool.shutdownNow();
        }
        double durationSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;

        System.out.printf(
                Locale.ROOT,
                "[run] Finished in %.2fs: regular=%d/%d dupReplayed=%d/%d (mismatch=%d) rejected=%d httpErr=%d poison=%d%n",
                durationSeconds,
                regularAccepted.get(),
                regularSent.get(),
                duplicateReplayed.get(),
                duplicateSent.get(),
                duplicateMismatches.get(),
                validationRejected.get(),
                unexpectedHttpErrors.get(),
                poisonSent.get()
        );

        return new RunResult(
                args.seed(),
                args.threads(),
                args.warmupSeconds(),
                warmupAccepted,
                regularSent.get(),
                regularAccepted.get(),
                duplicateSent.get(),
                duplicateReplayed.get(),
                duplicateMismatches.get(),
                validationRejected.get(),
                unexpectedHttpErrors.get(),
                poisonSent.get(),
                poisonUnknownAccount.get(),
                poisonNonPositiveAmount.get(),
                durationSeconds,
                regularLatency.snapshot(),
                allHttpLatency.snapshot(),
                Collections.unmodifiableList(benchmarkAccounts),
                Collections.unmodifiableList(warmupAccounts)
        );
    }

    private long runWarmupPhase(final List<AccountHandle> warmupAccounts, final int seconds) throws Exception {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
        AtomicLong accepted = new AtomicLong();
        AtomicInteger seq = new AtomicInteger();
        int workers = Math.min(warmupAccounts.size(), args.threads());
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            List<Callable<Void>> tasks = new ArrayList<>(workers);
            for (int w = 0; w < workers; w++) {
                final AccountHandle account = warmupAccounts.get(w);
                final int workerId = w;
                tasks.add(() -> {
                    while (System.nanoTime() < deadlineNanos) {
                        int n = seq.incrementAndGet();
                        String payload = "{\"type\":\"CREDIT\",\"amount\":\"10.00\",\"currency\":\"EUR\",\"reference\":\"Warmup #" + n + "\"}";
                        String idemKey = "warmup-" + args.seed() + "-w" + workerId + "-" + n;
                        String cid = "lg-warm-" + workerId + "-" + n;
                        HttpResponse<String> response = sendBookingPost(account.id().toString(), idemKey, cid, payload);
                        if (response.statusCode() == 201) {
                            accepted.incrementAndGet();
                        }
                    }
                    return null;
                });
            }
            for (Future<Void> f : pool.invokeAll(tasks)) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }
        return accepted.get();
    }

    private void runWorker(
            final DeterministicWorkloadPlan.WorkerAllocation alloc,
            final List<AccountHandle> workerAccounts,
            final RateLimiter rateLimiter,
            final LatencyRecorder regularLatency,
            final LatencyRecorder allHttpLatency,
            final AtomicLong regularSent,
            final AtomicLong regularAccepted,
            final AtomicLong duplicateSent,
            final AtomicLong duplicateReplayed,
            final AtomicLong duplicateMismatches,
            final AtomicLong validationRejected,
            final AtomicLong unexpectedHttpErrors,
            final AtomicLong poisonSent,
            final AtomicLong poisonUnknownAccount,
            final AtomicLong poisonNonPositiveAmount
    ) throws Exception {
        SplittableRandom random = new SplittableRandom(alloc.workerSeed());
        BigDecimal[] localBalances = new BigDecimal[workerAccounts.size()];
        for (int i = 0; i < localBalances.length; i++) {
            localBalances[i] = BigDecimal.ZERO;
        }
        AcceptedRequestRingBuffer ringBuffer = new AcceptedRequestRingBuffer(2048);

        int regDone = 0;
        int dupDone = 0;
        int poiDone = 0;

        // Interleave regular, duplicate replays (L2), and poison injections deterministically throughout the run
        while (regDone < alloc.regularCount()) {
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            int accIdx = regDone % workerAccounts.size();
            AccountHandle account = workerAccounts.get(accIdx);
            DeterministicWorkloadPlan.BookingSpec spec = DeterministicWorkloadPlan.nextRegularBooking(
                    random,
                    args.seed(),
                    alloc.workerIndex(),
                    regDone,
                    localBalances[accIdx]
            );
            String payloadJson = buildBookingJson(spec.type(), spec.amount().toPlainString(), spec.reference());

            long reqStart = System.nanoTime();
            HttpResponse<String> response = sendBookingPost(
                    account.id().toString(),
                    spec.idempotencyKey(),
                    spec.correlationId(),
                    payloadJson
            );
            long elapsed = System.nanoTime() - reqStart;
            regularLatency.recordNanos(elapsed);
            allHttpLatency.recordNanos(elapsed);
            regularSent.incrementAndGet();

            int status = response.statusCode();
            if (status == 201) {
                regularAccepted.incrementAndGet();
                if ("CREDIT".equals(spec.type())) {
                    localBalances[accIdx] = localBalances[accIdx].add(spec.amount());
                } else {
                    localBalances[accIdx] = localBalances[accIdx].subtract(spec.amount());
                }
                ringBuffer.add(new AcceptedRequestRingBuffer.AcceptedBooking(
                        account.id().toString(),
                        spec.idempotencyKey(),
                        payloadJson,
                        response.body()
                ));
            } else if (status == 400 || status == 409 || status == 422) {
                validationRejected.incrementAndGet();
            } else {
                unexpectedHttpErrors.incrementAndGet();
            }
            regDone++;

            // Progress logging every 10,000 regular requests globally
            long globalReg = regularSent.get();
            if (globalReg % 10_000L == 0L) {
                System.out.printf(
                        Locale.ROOT,
                        "[progress] regular=%d/%d duplicates=%d poison=%d rejected=%d%n",
                        globalReg,
                        args.bookings(),
                        duplicateSent.get(),
                        poisonSent.get(),
                        validationRejected.get()
                );
            }

            // Send scheduled Cohort 2 (L2) duplicate replays drawn from ringBuffer of already-accepted requests
            long expectedDupByNow = ((long) regDone * alloc.duplicateCount()) / Math.max(1, alloc.regularCount());
            while (dupDone < expectedDupByNow && ringBuffer.size() > 0) {
                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }
                AcceptedRequestRingBuffer.AcceptedBooking target = ringBuffer.sample(random);
                String dupCid = "lg-dup-" + Long.toUnsignedString(args.seed(), 36) + "-w" + alloc.workerIndex() + "-" + dupDone;
                long dupStart = System.nanoTime();
                HttpResponse<String> dupResp = sendBookingPost(
                        target.accountId(),
                        target.idempotencyKey(),
                        dupCid,
                        target.payloadJson()
                );
                long dupElapsed = System.nanoTime() - dupStart;
                allHttpLatency.recordNanos(dupElapsed);
                duplicateSent.incrementAndGet();

                String replayedHeader = dupResp.headers().firstValue("Idempotency-Replayed").orElse("");
                if (dupResp.statusCode() == 201
                        && "true".equalsIgnoreCase(replayedHeader)
                        && target.expectedResponseBody().equals(dupResp.body())) {
                    duplicateReplayed.incrementAndGet();
                } else {
                    duplicateMismatches.incrementAndGet();
                }
                dupDone++;
            }

            // Send scheduled Cohort 3 poison events directly into Kafka ledger.events
            long expectedPoiByNow = ((long) regDone * alloc.poisonCount()) / Math.max(1, alloc.regularCount());
            while (poiDone < expectedPoiByNow && poisonEventProducer != null) {
                UUID eventId = DeterministicWorkloadPlan.deterministicUuid(random);
                UUID bookingId = DeterministicWorkloadPlan.deterministicUuid(random);
                boolean unknownAccountCase = ((alloc.workerIndex() + poiDone) % 2 == 0);
                UUID aggregateId;
                String amount;
                String ref;
                if (unknownAccountCase) {
                    aggregateId = DeterministicWorkloadPlan.deterministicUuid(random);
                    amount = "10.00";
                    ref = "Poison unknown account #" + poiDone;
                    poisonUnknownAccount.incrementAndGet();
                } else {
                    aggregateId = workerAccounts.get(poiDone % workerAccounts.size()).id();
                    amount = "-5.00";
                    ref = "Poison non-positive amount #" + poiDone;
                    poisonNonPositiveAmount.incrementAndGet();
                }
                String poiCid = "lg-poi-" + Long.toUnsignedString(args.seed(), 36) + "-w" + alloc.workerIndex() + "-" + poiDone;
                poisonEventProducer.sendPoisonEvent(eventId, aggregateId, bookingId, amount, poiCid, ref);
                poisonSent.incrementAndGet();
                poiDone++;
            }
        }
    }

    private AccountHandle createAccount(final String accountNumber) throws Exception {
        String body = "{\"accountNumber\":\"" + accountNumber + "\",\"currency\":\"EUR\"}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(args.baseUrl() + "/api/v1/accounts"))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + tokenProvider.getAccessToken())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201) {
            throw new IllegalStateException(
                    "Failed to create account " + accountNumber + ": HTTP " + response.statusCode() + " " + response.body()
            );
        }
        JsonNode root = objectMapper.readTree(response.body());
        return new AccountHandle(UUID.fromString(root.get("id").asText()), accountNumber);
    }

    private HttpResponse<String> sendBookingPost(
            final String accountId,
            final String idempotencyKey,
            final String correlationId,
            final String payloadJson
    ) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(args.baseUrl() + "/api/v1/accounts/" + accountId + "/bookings"))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + tokenProvider.getAccessToken())
                .header("Idempotency-Key", idempotencyKey)
                .header("X-Correlation-Id", correlationId)
                .POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String buildBookingJson(final String type, final String amount, final String reference) {
        return "{\"type\":\"" + type + "\",\"amount\":\"" + amount + "\",\"currency\":\"EUR\",\"reference\":\"" + reference + "\"}";
    }

    private static List<AccountHandle> sliceAccountsForWorker(
            final List<AccountHandle> allAccounts,
            final int workerIndex,
            final int threads
    ) {
        List<AccountHandle> slice = new ArrayList<>();
        for (int i = workerIndex; i < allAccounts.size(); i += threads) {
            slice.add(allAccounts.get(i));
        }
        return slice;
    }

    /**
     * Simple token-pacing rate limiter for optional --rate=<req/s>.
     */
    private static final class RateLimiter {
        private final long intervalNanos;
        private final AtomicLong nextSlotNanos;

        RateLimiter(final double permitsPerSecond) {
            this.intervalNanos = Math.max(1L, Math.round(1_000_000_000.0 / permitsPerSecond));
            this.nextSlotNanos = new AtomicLong(System.nanoTime());
        }

        void acquire() throws InterruptedException {
            while (true) {
                long now = System.nanoTime();
                long prev = nextSlotNanos.get();
                long slot = Math.max(now, prev + intervalNanos);
                if (nextSlotNanos.compareAndSet(prev, slot)) {
                    long sleepNanos = slot - now;
                    if (sleepNanos > 50_000L) {
                        Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
                    }
                    return;
                }
            }
        }
    }
}
