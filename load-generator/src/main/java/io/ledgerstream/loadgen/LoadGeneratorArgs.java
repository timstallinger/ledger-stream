package io.ledgerstream.loadgen;

import java.util.HashMap;
import java.util.Map;

/**
 * Immutable CLI configuration for the ledger-stream load generator.
 */
public record LoadGeneratorArgs(
        int bookings,
        double duplicateRatio,
        double poisonRatio,
        long seed,
        int threads,
        String baseUrl,
        double ratePerSecond,
        int warmupSeconds,
        int accounts,
        String kafkaBootstrap,
        String schemaRegistryUrl,
        String topic,
        String dbUrl,
        String dbUser,
        String dbPassword,
        String prometheusUrl,
        String oidcTokenUrl,
        String oidcClientId,
        String oidcClientSecret,
        String oidcUsername,
        String oidcPassword,
        int drainTimeoutSeconds,
        boolean skipInvariants
) {

    public int duplicateCount() {
        return (int) Math.round(bookings * duplicateRatio);
    }

    public int poisonCount() {
        return (int) Math.round(bookings * poisonRatio);
    }

    public int totalPlannedRequests() {
        return bookings + duplicateCount() + poisonCount();
    }

    public static LoadGeneratorArgs parse(final String[] rawArgs) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < rawArgs.length; i++) {
            String token = rawArgs[i];
            if (!token.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected positional argument: " + token);
            }
            int eq = token.indexOf('=');
            if (eq > 2) {
                values.put(token.substring(2, eq), token.substring(eq + 1));
            } else if (i + 1 < rawArgs.length && !rawArgs[i + 1].startsWith("--")) {
                values.put(token.substring(2), rawArgs[++i]);
            } else {
                values.put(token.substring(2), "true");
            }
        }

        int bookings = parseInt(values, "bookings", 100_000);
        double duplicates = parseDouble(values, "duplicates", 0.05);
        double poison = parseDouble(values, "poison", 0.01);
        long seed = parseLong(values, "seed", 20261124L);
        int threads = parseInt(values, "threads", 32);
        String baseUrl = stripTrailingSlash(get(values, "base-url", env("LOADGEN_BASE_URL", "http://127.0.0.1:8080")));
        double rate = parseDouble(values, "rate", 0.0);
        int warmupSeconds = parseInt(values, "warmup-seconds", 5);
        int accounts = parseInt(values, "accounts", Math.max(threads * 4, 128));
        String kafkaBootstrap = get(values, "kafka-bootstrap", env("LOADGEN_KAFKA_BOOTSTRAP", "127.0.0.1:9092"));
        String schemaRegistryUrl = stripTrailingSlash(
                get(values, "schema-registry-url", env("LOADGEN_SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081"))
        );
        String topic = get(values, "topic", env("LOADGEN_TOPIC", "ledger.events"));
        String dbUrl = get(values, "db-url", env("LOADGEN_DB_URL", "jdbc:postgresql://127.0.0.1:5433/ledger"));
        String dbUser = get(values, "db-user", env("LOADGEN_DB_USER", "ledger"));
        String dbPassword = get(values, "db-password", env("LOADGEN_DB_PASSWORD", "ledger_local_dev"));
        String prometheusUrl = stripTrailingSlash(
                get(values, "prometheus-url", env("LOADGEN_PROMETHEUS_URL", "http://127.0.0.1:9090"))
        );
        String oidcTokenUrl = get(
                values,
                "oidc-token-url",
                env("LOADGEN_OIDC_TOKEN_URL", "http://localhost:8180/realms/ledger-stream/protocol/openid-connect/token")
        );
        String oidcClientId = get(values, "oidc-client-id", env("LOADGEN_OIDC_CLIENT_ID", "ledger-stream-cli"));
        String oidcClientSecret = get(
                values,
                "oidc-client-secret",
                env("LOADGEN_OIDC_CLIENT_SECRET", env("LEDGER_CLI_CLIENT_SECRET", "change_me_local_cli_secret"))
        );
        String oidcUsername = get(values, "oidc-username", env("LOADGEN_OIDC_USERNAME", ""));
        String oidcPassword = get(values, "oidc-password", env("LOADGEN_OIDC_PASSWORD", ""));
        int drainTimeoutSeconds = parseInt(values, "drain-timeout-seconds", 300);
        boolean skipInvariants = Boolean.parseBoolean(values.getOrDefault("skip-invariants", "false"));

        if (bookings <= 0) {
            throw new IllegalArgumentException("--bookings must be > 0");
        }
        if (duplicates < 0.0 || duplicates > 1.0) {
            throw new IllegalArgumentException("--duplicates must be in [0.0, 1.0]");
        }
        if (poison < 0.0 || poison > 1.0) {
            throw new IllegalArgumentException("--poison must be in [0.0, 1.0]");
        }
        if (threads <= 0) {
            throw new IllegalArgumentException("--threads must be > 0");
        }
        if (accounts < threads) {
            throw new IllegalArgumentException("--accounts (" + accounts + ") must be >= --threads (" + threads + ")");
        }
        if (rate < 0.0) {
            throw new IllegalArgumentException("--rate must be >= 0");
        }
        if (warmupSeconds < 0) {
            throw new IllegalArgumentException("--warmup-seconds must be >= 0");
        }
        if (drainTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("--drain-timeout-seconds must be > 0");
        }

        return new LoadGeneratorArgs(
                bookings,
                duplicates,
                poison,
                seed,
                threads,
                baseUrl,
                rate,
                warmupSeconds,
                accounts,
                kafkaBootstrap,
                schemaRegistryUrl,
                topic,
                dbUrl,
                dbUser,
                dbPassword,
                prometheusUrl,
                oidcTokenUrl,
                oidcClientId,
                oidcClientSecret,
                oidcUsername,
                oidcPassword,
                drainTimeoutSeconds,
                skipInvariants
        );
    }

    private static String get(final Map<String, String> values, final String key, final String fallback) {
        return values.getOrDefault(key, fallback);
    }

    private static int parseInt(final Map<String, String> values, final String key, final int fallback) {
        String raw = values.get(key);
        return raw == null ? fallback : Integer.parseInt(raw.trim());
    }

    private static long parseLong(final Map<String, String> values, final String key, final long fallback) {
        String raw = values.get(key);
        return raw == null ? fallback : Long.parseLong(raw.trim());
    }

    private static double parseDouble(final Map<String, String> values, final String key, final double fallback) {
        String raw = values.get(key);
        return raw == null ? fallback : Double.parseDouble(raw.trim());
    }

    private static String env(final String key, final String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private static String stripTrailingSlash(final String url) {
        if (url == null || url.isEmpty()) {
            return "";
        }
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
