package io.ledgerstream.loadgen;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Automated post-run drain waiter and invariant verifier.
 * Any single violation causes verification to return false (exit code != 0).
 */
public final class InvariantChecker {

    public record AccountBalanceCheck(
            UUID accountId,
            String accountNumber,
            BigDecimal accountBalance,
            BigDecimal bookingsSum,
            BigDecimal summaryBalance,
            long bookingsCount,
            long summaryBookingCount,
            boolean exactMatch
    ) {
    }

    public record VerificationReport(
            boolean allPassed,
            double drainSeconds,
            long outboxUnsentRemaining,
            long dbTotalBookings,
            long expectedTotalBookings,
            long dbBenchmarkBookings,
            long expectedBenchmarkBookings,
            long dbProcessedEvents,
            long dbDlqUnhandled,
            long expectedDlq,
            double consumerDuplicatesRejectedMetric,
            double consumerProcessedMetric,
            double consumerPoisonMetric,
            long validationRejectedHttp,
            long unexpectedHttpErrors,
            long duplicateMismatches,
            List<AccountBalanceCheck> balanceSample,
            List<String> failures
    ) {
    }

    private final LoadGeneratorArgs args;
    private final PrometheusClient prometheusClient;

    public InvariantChecker(final LoadGeneratorArgs args, final PrometheusClient prometheusClient) {
        this.args = args;
        this.prometheusClient = prometheusClient;
    }

    public VerificationReport waitAndVerify(final LoadRunner.RunResult run) throws Exception {
        long expectedTotalBookings = run.regularAccepted() + run.warmupAccepted();
        long expectedDlq = run.poisonSent();

        Instant drainStart = Instant.now();
        Instant deadline = drainStart.plusSeconds(args.drainTimeoutSeconds());

        long outboxRemaining = -1L;
        long processedEvents = -1L;
        long dlqUnhandled = -1L;

        System.out.printf(
                Locale.ROOT,
                "[drain] Waiting for outbox_unsent=0, processed_events=%d, dlq_entries=%d (timeout=%ds)...%n",
                expectedTotalBookings,
                expectedDlq,
                args.drainTimeoutSeconds()
        );

        try (Connection conn = DriverManager.getConnection(args.dbUrl(), args.dbUser(), args.dbPassword())) {
            while (Instant.now().isBefore(deadline)) {
                outboxRemaining = querySingleLong(conn, "SELECT COUNT(*) FROM outbox_events WHERE status <> 'SENT'");
                processedEvents = querySingleLong(conn, "SELECT COUNT(*) FROM processed_events");
                dlqUnhandled = querySingleLong(conn, "SELECT COUNT(*) FROM dlq_entries WHERE replayed_at IS NULL");

                if (outboxRemaining == 0L && processedEvents >= expectedTotalBookings && dlqUnhandled >= expectedDlq) {
                    break;
                }
                Thread.sleep(500L);
            }

            // Short settle window to detect any stray late duplicates or unexpected events
            Thread.sleep(2000L);

            double drainSeconds = Duration.between(drainStart, Instant.now()).toMillis() / 1000.0;

            outboxRemaining = querySingleLong(conn, "SELECT COUNT(*) FROM outbox_events WHERE status <> 'SENT'");
            long dbTotalBookings = querySingleLong(conn, "SELECT COUNT(*) FROM bookings");
            long dbBenchmarkBookings = countBenchmarkBookings(conn, run.benchmarkAccounts());
            processedEvents = querySingleLong(conn, "SELECT COUNT(*) FROM processed_events");
            dlqUnhandled = querySingleLong(conn, "SELECT COUNT(*) FROM dlq_entries WHERE replayed_at IS NULL");

            // Sample 10 deterministic benchmark accounts for exact balance == SUM(bookings) AND summary match
            int sampleSize = Math.min(10, run.benchmarkAccounts().size());
            List<AccountBalanceCheck> sampleChecks = new ArrayList<>(sampleSize);
            for (int i = 0; i < sampleSize; i++) {
                sampleChecks.add(checkAccountBalance(conn, run.benchmarkAccounts().get(i)));
            }

            PrometheusClient.ServerMetricsSnapshot metrics = prometheusClient.captureServerSnapshot("5m");

            List<String> failures = new ArrayList<>();
            if (outboxRemaining != 0L) {
                failures.add("outbox_events not fully drained: remaining=" + outboxRemaining);
            }
            if (dbBenchmarkBookings != run.regularAccepted()) {
                failures.add("benchmark DB bookings (" + dbBenchmarkBookings
                        + ") != regular HTTP accepted (" + run.regularAccepted() + ")");
            }
            if (dbTotalBookings != expectedTotalBookings) {
                failures.add("total DB bookings (" + dbTotalBookings
                        + ") != regular+warmup HTTP accepted (" + expectedTotalBookings + ")");
            }
            if (run.validationRejected() != 0L) {
                failures.add("HTTP validation/conflict rejections expected 0, got " + run.validationRejected());
            }
            if (run.unexpectedHttpErrors() != 0L) {
                failures.add("unexpected HTTP errors expected 0, got " + run.unexpectedHttpErrors());
            }
            if (run.duplicateMismatches() != 0L) {
                failures.add("L2 duplicate replay mismatches expected 0, got " + run.duplicateMismatches());
            }
            if (run.duplicateReplayed() != run.duplicateSent()) {
                failures.add("L2 duplicate replayed (" + run.duplicateReplayed()
                        + ") != duplicate sent (" + run.duplicateSent() + ")");
            }
            if (dlqUnhandled != expectedDlq) {
                failures.add("dlq_entries unhandled (" + dlqUnhandled + ") != poison sent (" + expectedDlq + ")");
            }
            if (processedEvents != expectedTotalBookings) {
                failures.add("processed_events (" + processedEvents
                        + ") != expected total bookings (" + expectedTotalBookings + ")");
            }
            if (metrics.consumerDuplicatesRejectedTotal() != 0.0) {
                failures.add("L2 consumer_duplicates_rejected_total{reason=\"duplicate\"} expected 0, got "
                        + metrics.consumerDuplicatesRejectedTotal());
            }
            for (AccountBalanceCheck check : sampleChecks) {
                if (!check.exactMatch()) {
                    failures.add("Balance mismatch for account " + check.accountNumber()
                            + ": account.balance=" + check.accountBalance()
                            + ", sum(bookings)=" + check.bookingsSum()
                            + ", summary.balance=" + check.summaryBalance()
                            + ", count=" + check.bookingsCount() + "/" + check.summaryBookingCount());
                }
            }

            return new VerificationReport(
                    failures.isEmpty(),
                    drainSeconds,
                    outboxRemaining,
                    dbTotalBookings,
                    expectedTotalBookings,
                    dbBenchmarkBookings,
                    run.regularAccepted(),
                    processedEvents,
                    dlqUnhandled,
                    expectedDlq,
                    metrics.consumerDuplicatesRejectedTotal(),
                    metrics.consumerProcessedTotal(),
                    metrics.consumerPoisonRejectedTotal(),
                    run.validationRejected(),
                    run.unexpectedHttpErrors(),
                    run.duplicateMismatches(),
                    sampleChecks,
                    failures
            );
        }
    }

    private static long querySingleLong(final Connection conn, final String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long countBenchmarkBookings(
            final Connection conn,
            final List<LoadRunner.AccountHandle> accounts
    ) throws SQLException {
        if (accounts.isEmpty()) {
            return 0L;
        }
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM bookings WHERE account_id IN (");
        for (int i = 0; i < accounts.size(); i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append('?');
        }
        sql.append(')');
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < accounts.size(); i++) {
                ps.setObject(i + 1, accounts.get(i).id());
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static AccountBalanceCheck checkAccountBalance(
            final Connection conn,
            final LoadRunner.AccountHandle account
    ) throws SQLException {
        BigDecimal accountBalance;
        try (PreparedStatement ps = conn.prepareStatement("SELECT balance FROM accounts WHERE id = ?")) {
            ps.setObject(1, account.id());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Account not found in DB: " + account.id());
                }
                accountBalance = rs.getBigDecimal(1);
            }
        }

        BigDecimal bookingsSum;
        long bookingsCount;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COALESCE(SUM(CASE WHEN booking_type = 'CREDIT' THEN amount ELSE -amount END), 0), COUNT(*) "
                        + "FROM bookings WHERE account_id = ?"
        )) {
            ps.setObject(1, account.id());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                bookingsSum = rs.getBigDecimal(1);
                bookingsCount = rs.getLong(2);
            }
        }

        BigDecimal summaryBalance = BigDecimal.ZERO;
        long summaryBookingCount = 0L;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT balance, booking_count FROM account_summary WHERE account_id = ?"
        )) {
            ps.setObject(1, account.id());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    summaryBalance = rs.getBigDecimal(1);
                    summaryBookingCount = rs.getLong(2);
                }
            }
        }

        boolean exactMatch = accountBalance.compareTo(bookingsSum) == 0
                && summaryBalance.compareTo(bookingsSum) == 0
                && bookingsCount == summaryBookingCount;

        return new AccountBalanceCheck(
                account.id(),
                account.accountNumber(),
                accountBalance,
                bookingsSum,
                summaryBalance,
                bookingsCount,
                summaryBookingCount,
                exactMatch
        );
    }
}
