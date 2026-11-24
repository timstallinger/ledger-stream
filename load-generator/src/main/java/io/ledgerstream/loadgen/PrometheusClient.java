package io.ledgerstream.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;

/**
 * Queries both Spring Boot /actuator/prometheus (for exact instant counter verification)
 * and Prometheus HTTP API /api/v1/query (for server-side p50/p95/p99 and throughput rates).
 */
public final class PrometheusClient {

    public record ServerMetricsSnapshot(
            double httpP50Ms,
            double httpP95Ms,
            double httpP99Ms,
            double httpRequestRatePerSec,
            double consumerEventsPerSec,
            double consumerProcessedTotal,
            double consumerDuplicatesRejectedTotal,
            double consumerPoisonRejectedTotal,
            double dlqUnhandledGauge,
            double outboxUnsentGauge
    ) {
    }

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String appBaseUrl;
    private final String prometheusBaseUrl;

    public PrometheusClient(
            final HttpClient httpClient,
            final ObjectMapper objectMapper,
            final String appBaseUrl,
            final String prometheusBaseUrl
    ) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.appBaseUrl = Objects.requireNonNull(appBaseUrl, "appBaseUrl must not be null");
        this.prometheusBaseUrl = Objects.requireNonNull(prometheusBaseUrl, "prometheusBaseUrl must not be null");
    }

    /**
     * Scrapes /actuator/prometheus directly so counter invariants are never stale due to Prometheus scrape interval.
     */
    public Map<String, Double> scrapeActuatorMetrics() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(appBaseUrl + "/actuator/prometheus"))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Actuator prometheus scrape failed: HTTP " + response.statusCode());
            }
            Map<String, Double> metrics = new LinkedHashMap<>();
            for (String line : response.body().split("\\R")) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                int lastSpace = trimmed.lastIndexOf(' ');
                if (lastSpace <= 0) {
                    continue;
                }
                String key = trimmed.substring(0, lastSpace).trim();
                String rawVal = trimmed.substring(lastSpace + 1).trim();
                try {
                    metrics.put(key, Double.parseDouble(rawVal));
                } catch (NumberFormatException ignored) {
                    // ignore NaN/Inf or malformed lines
                }
            }
            return metrics;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to scrape /actuator/prometheus", exception);
        }
    }

    public double sumMetricMatching(final Map<String, Double> metrics, final String namePrefix, final String requiredSubstring) {
        double sum = 0.0;
        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(namePrefix) && (requiredSubstring == null || key.contains(requiredSubstring))) {
                sum += entry.getValue();
            }
        }
        return sum;
    }

    public OptionalDouble queryScalar(final String promQl) {
        if (prometheusBaseUrl == null || prometheusBaseUrl.isBlank()) {
            return OptionalDouble.empty();
        }
        try {
            String url = prometheusBaseUrl + "/api/v1/query?query=" + URLEncoder.encode(promQl, StandardCharsets.UTF_8);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return OptionalDouble.empty();
            }
            JsonNode root = objectMapper.readTree(response.body());
            if (!"success".equals(root.path("status").asText())) {
                return OptionalDouble.empty();
            }
            JsonNode results = root.path("data").path("result");
            if (!results.isArray() || results.isEmpty()) {
                return OptionalDouble.empty();
            }
            JsonNode valNode = results.get(0).path("value");
            if (!valNode.isArray() || valNode.size() < 2) {
                return OptionalDouble.empty();
            }
            String valStr = valNode.get(1).asText();
            if ("NaN".equalsIgnoreCase(valStr) || "+Inf".equalsIgnoreCase(valStr) || "-Inf".equalsIgnoreCase(valStr)) {
                return OptionalDouble.empty();
            }
            return OptionalDouble.of(Double.parseDouble(valStr));
        } catch (Exception ignored) {
            return OptionalDouble.empty();
        }
    }

    public ServerMetricsSnapshot captureServerSnapshot(final String windowSpec) {
        Map<String, Double> actuator = scrapeActuatorMetrics();
        String w = (windowSpec == null || windowSpec.isBlank()) ? "5m" : windowSpec;

        double p50Sec = queryScalar(
                "histogram_quantile(0.50, sum by (le) (rate(http_server_requests_seconds_bucket{uri=\"/api/v1/accounts/{accountId}/bookings\",method=\"POST\"}[" + w + "])))"
        ).orElse(Double.NaN);
        double p95Sec = queryScalar(
                "histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{uri=\"/api/v1/accounts/{accountId}/bookings\",method=\"POST\"}[" + w + "])))"
        ).orElse(Double.NaN);
        double p99Sec = queryScalar(
                "histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{uri=\"/api/v1/accounts/{accountId}/bookings\",method=\"POST\"}[" + w + "])))"
        ).orElse(Double.NaN);
        double httpRate = queryScalar(
                "sum(rate(http_server_requests_seconds_count{uri=\"/api/v1/accounts/{accountId}/bookings\",method=\"POST\"}[" + w + "]))"
        ).orElse(Double.NaN);
        double consumerRate = queryScalar(
                "sum(rate(consumer_processed_total{outcome=\"processed\"}[" + w + "]))"
        ).orElse(Double.NaN);

        return new ServerMetricsSnapshot(
                Double.isNaN(p50Sec) ? Double.NaN : p50Sec * 1000.0,
                Double.isNaN(p95Sec) ? Double.NaN : p95Sec * 1000.0,
                Double.isNaN(p99Sec) ? Double.NaN : p99Sec * 1000.0,
                httpRate,
                consumerRate,
                sumMetricMatching(actuator, "consumer_processed_total", "outcome=\"processed\""),
                sumMetricMatching(actuator, "consumer_duplicates_rejected_total", "reason=\"duplicate\""),
                sumMetricMatching(actuator, "consumer_duplicates_rejected_total", "reason=\"poison\""),
                sumMetricMatching(actuator, "dlq_entries_unhandled", null),
                sumMetricMatching(actuator, "outbox_unsent", null)
        );
    }
}
