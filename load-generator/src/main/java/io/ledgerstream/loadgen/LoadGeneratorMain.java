package io.ledgerstream.loadgen;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Framework-free CLI entry point for Phase 10 load generation and automated invariant verification.
 */
public final class LoadGeneratorMain {

    private LoadGeneratorMain() {
    }

    public static void main(final String[] rawArgs) {
        try {
            LoadGeneratorArgs args = LoadGeneratorArgs.parse(rawArgs);
            int exitCode = run(args);
            System.exit(exitCode);
        } catch (Exception exception) {
            System.err.println("LOAD-GENERATOR ERROR: " + exception.getMessage());
            exception.printStackTrace(System.err);
            System.exit(1);
        }
    }

    public static int run(final LoadGeneratorArgs args) throws Exception {
        // Explicit HTTP/1.1 Keep-Alive pooled HttpClient sized to worker concurrency (L3c)
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .executor(Executors.newFixedThreadPool(Math.max(args.threads(), 8)))
                .build();
        ObjectMapper objectMapper = new ObjectMapper();
        OidcTokenProvider tokenProvider = new OidcTokenProvider(
                httpClient,
                objectMapper,
                args.oidcTokenUrl(),
                args.oidcClientId(),
                args.oidcClientSecret(),
                args.oidcUsername(),
                args.oidcPassword()
        );
        PrometheusClient prometheusClient = new PrometheusClient(
                httpClient,
                objectMapper,
                args.baseUrl(),
                args.prometheusUrl()
        );

        AtomicLong maxOutboxUnsent = new AtomicLong(0L);
        ScheduledExecutorService outboxSampler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbox-unsent-sampler");
            t.setDaemon(true);
            return t;
        });
        outboxSampler.scheduleAtFixedRate(() -> {
            try {
                double unsent = prometheusClient.sumMetricMatching(
                        prometheusClient.scrapeActuatorMetrics(),
                        "outbox_unsent",
                        null
                );
                maxOutboxUnsent.accumulateAndGet(Math.round(unsent), Math::max);
            } catch (Exception ignored) {
                // best-effort live peak sampling
            }
        }, 1, 1, TimeUnit.SECONDS);

        LoadRunner.RunResult runResult;
        try (PoisonEventProducer poisonProducer = args.poisonCount() > 0
                ? new PoisonEventProducer(args.kafkaBootstrap(), args.schemaRegistryUrl(), args.topic())
                : null) {
            LoadRunner runner = new LoadRunner(args, httpClient, objectMapper, tokenProvider, poisonProducer);
            runResult = runner.execute();
        } finally {
            outboxSampler.shutdownNow();
        }

        // Capture server-side metrics immediately after measured HTTP run (and again after drain)
        PrometheusClient.ServerMetricsSnapshot postLoadServer = prometheusClient.captureServerSnapshot("5m");
        maxOutboxUnsent.accumulateAndGet(Math.round(postLoadServer.outboxUnsentGauge()), Math::max);

        if (args.skipInvariants()) {
            printSummary(args, runResult, postLoadServer, null, maxOutboxUnsent.get());
            return 0;
        }

        InvariantChecker checker = new InvariantChecker(args, prometheusClient);
        InvariantChecker.VerificationReport report = checker.waitAndVerify(runResult);
        PrometheusClient.ServerMetricsSnapshot postDrainServer = prometheusClient.captureServerSnapshot("5m");

        printSummary(args, runResult, postLoadServer, report, maxOutboxUnsent.get());
        printPrometheusSnapshot("POST_DRAIN_SERVER_METRICS", postDrainServer);

        return report.allPassed() ? 0 : 1;
    }

    private static void printSummary(
            final LoadGeneratorArgs args,
            final LoadRunner.RunResult run,
            final PrometheusClient.ServerMetricsSnapshot server,
            final InvariantChecker.VerificationReport report,
            final long maxOutboxUnsent
    ) {
        System.out.println();
        System.out.println("============================================================");
        System.out.println("LEDGER-STREAM PHASE 10 LOAD REPORT");
        System.out.println("============================================================");
        System.out.printf(
                Locale.ROOT,
                "Seed: %d | Threads: %d | Accounts: %d | Warmup: %ds (%d bookings excluded)%n",
                run.seed(),
                run.threads(),
                args.accounts(),
                run.warmupSeconds(),
                run.warmupAccepted()
        );
        System.out.printf(
                Locale.ROOT,
                "Cohort 1 (Regular HTTP):    sent=%d accepted=%d rejected=%d httpErrors=%d%n",
                run.regularSent(),
                run.regularAccepted(),
                run.validationRejected(),
                run.unexpectedHttpErrors()
        );
        System.out.printf(
                Locale.ROOT,
                "Cohort 2 (Duplicates L2):   sent=%d replayed_201_header_match=%d mismatches=%d%n",
                run.duplicateSent(),
                run.duplicateReplayed(),
                run.duplicateMismatches()
        );
        System.out.printf(
                Locale.ROOT,
                "Cohort 3 (Poison Avro):     sent=%d (unknown_account=%d, non_positive_amount=%d)%n",
                run.poisonSent(),
                run.poisonUnknownAccount(),
                run.poisonNonPositiveAmount()
        );
        System.out.printf(
                Locale.ROOT,
                "Duration (excl. warmup):    %.2f s%n",
                run.durationSeconds()
        );
        System.out.printf(
                Locale.ROOT,
                "Client throughput:          sent=%.1f HTTP req/s | accepted=%.1f bookings/s%n",
                run.httpSentPerSecond(),
                run.regularAcceptedPerSecond()
        );
        System.out.printf(
                Locale.ROOT,
                "Client latency (regular):   p50=%.2f ms | p95=%.2f ms | p99=%.2f ms | max=%.2f ms%n",
                run.regularLatency().p50Ms(),
                run.regularLatency().p95Ms(),
                run.regularLatency().p99Ms(),
                run.regularLatency().maxMs()
        );
        System.out.printf(
                Locale.ROOT,
                "Client latency (all HTTP):  p50=%.2f ms | p95=%.2f ms | p99=%.2f ms | max=%.2f ms%n",
                run.allHttpLatency().p50Ms(),
                run.allHttpLatency().p95Ms(),
                run.allHttpLatency().p99Ms(),
                run.allHttpLatency().maxMs()
        );
        printPrometheusSnapshot("POST_LOAD_SERVER_METRICS", server);

        if (report != null) {
            System.out.println("------------------------------------------------------------");
            System.out.println("AUTOMATED INVARIANT CHECK (Exit code 0 iff all PASS)");
            System.out.println("------------------------------------------------------------");
            System.out.printf(
                    Locale.ROOT,
                    "Max outbox_unsent (live):   %d | Drain time: %.2f s (outbox_unsent remaining=%d)%n",
                    maxOutboxUnsent,
                    report.drainSeconds(),
                    report.outboxUnsentRemaining()
            );
            System.out.printf(
                    Locale.ROOT,
                    "[%s] Benchmark DB bookings: %d == expected %d (diff=%d)%n",
                    report.dbBenchmarkBookings() == report.expectedBenchmarkBookings() ? "PASS" : "FAIL",
                    report.dbBenchmarkBookings(),
                    report.expectedBenchmarkBookings(),
                    report.dbBenchmarkBookings() - report.expectedBenchmarkBookings()
            );
            System.out.printf(
                    Locale.ROOT,
                    "[%s] Total DB bookings:     %d == expected %d (incl. warmup=%d, diff=%d)%n",
                    report.dbTotalBookings() == report.expectedTotalBookings() ? "PASS" : "FAIL",
                    report.dbTotalBookings(),
                    report.expectedTotalBookings(),
                    run.warmupAccepted(),
                    report.dbTotalBookings() - report.expectedTotalBookings()
            );
            System.out.printf(
                    Locale.ROOT,
                    "[%s] Rejected HTTP reqs:    validation=%d httpErrors=%d dupMismatch=%d (expected 0)%n",
                    (report.validationRejectedHttp() == 0 && report.unexpectedHttpErrors() == 0 && report.duplicateMismatches() == 0)
                            ? "PASS" : "FAIL",
                    report.validationRejectedHttp(),
                    report.unexpectedHttpErrors(),
                    report.duplicateMismatches()
            );
            System.out.printf(
                    Locale.ROOT,
                    "[%s] DLQ unhandled entries: %d == poison sent %d (diff=%d)%n",
                    report.dbDlqUnhandled() == report.expectedDlq() ? "PASS" : "FAIL",
                    report.dbDlqUnhandled(),
                    report.expectedDlq(),
                    report.dbDlqUnhandled() - report.expectedDlq()
            );
            System.out.printf(
                    Locale.ROOT,
                    "[%s] Processed events (DB): %d == expected %d (diff=%d)%n",
                    report.dbProcessedEvents() == report.expectedTotalBookings() ? "PASS" : "FAIL",
                    report.dbProcessedEvents(),
                    report.expectedTotalBookings(),
                    report.dbProcessedEvents() - report.expectedTotalBookings()
            );
            System.out.printf(
                    Locale.ROOT,
                    "[%s] L2 consumer_duplicates_rejected_total{reason=\"duplicate\"}: %.0f == 0%n",
                    report.consumerDuplicatesRejectedMetric() == 0.0 ? "PASS" : "FAIL",
                    report.consumerDuplicatesRejectedMetric()
            );
            System.out.println("Account balance exact sample (10 benchmark accounts):");
            for (InvariantChecker.AccountBalanceCheck check : report.balanceSample()) {
                System.out.printf(
                        Locale.ROOT,
                        "  [%s] %s | account.balance=%s | sum(bookings)=%s | summary.balance=%s | count=%d/%d%n",
                        check.exactMatch() ? "PASS" : "FAIL",
                        check.accountNumber(),
                        check.accountBalance().toPlainString(),
                        check.bookingsSum().toPlainString(),
                        check.summaryBalance().toPlainString(),
                        check.bookingsCount(),
                        check.summaryBookingCount()
                );
            }
            System.out.println("------------------------------------------------------------");
            if (report.allPassed()) {
                System.out.println("INVARIANT_CHECK_RESULT=PASS (EXIT_CODE=0)");
            } else {
                System.out.println("INVARIANT_CHECK_RESULT=FAIL (EXIT_CODE=1)");
                for (String failure : report.failures()) {
                    System.out.println("  FAILURE: " + failure);
                }
            }
            System.out.println("============================================================");
        }
    }

    private static void printPrometheusSnapshot(
            final String label,
            final PrometheusClient.ServerMetricsSnapshot server
    ) {
        System.out.printf(
                Locale.ROOT,
                "%s: http_p50=%.2fms http_p95=%.2fms http_p99=%.2fms http_rate=%.1f/s consumer_rate=%.1f/s "
                        + "consumer_processed=%.0f consumer_dup_rejected=%.0f consumer_poison=%.0f dlq_unhandled=%.0f outbox_unsent=%.0f%n",
                label,
                server.httpP50Ms(),
                server.httpP95Ms(),
                server.httpP99Ms(),
                server.httpRequestRatePerSec(),
                server.consumerEventsPerSec(),
                server.consumerProcessedTotal(),
                server.consumerDuplicatesRejectedTotal(),
                server.consumerPoisonRejectedTotal(),
                server.dlqUnhandledGauge(),
                server.outboxUnsentGauge()
        );
    }
}
