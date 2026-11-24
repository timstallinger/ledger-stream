package io.ledgerstream.app;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.adapter.in.rest.CorrelationIdFilter;
import io.ledgerstream.app.security.TestJwtSupport;
import io.ledgerstream.application.port.in.RelayOutboxUseCase;
import io.ledgerstream.application.port.out.AccountSummaryRepository;
import io.ledgerstream.domain.model.AccountId;
import net.logstash.logback.composite.JsonProvider;
import net.logstash.logback.composite.loggingevent.LogLevelJsonProvider;
import net.logstash.logback.composite.loggingevent.LoggerNameJsonProvider;
import net.logstash.logback.composite.loggingevent.LoggingEventFormattedTimestampJsonProvider;
import net.logstash.logback.composite.loggingevent.MdcJsonProvider;
import net.logstash.logback.composite.loggingevent.MessageJsonProvider;
import net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end verification of Phase 9 Observability & N3/N4:
 * - 3-stage correlationId propagation (HTTP -> outbox_events.correlation_id -> Relay -> Kafka header -> Consumer MDC)
 * - Distinct correlationIds for consecutive bookings
 * - Untrusted client X-Correlation-Id rejection (never truncated, correlationIdRejected=true logged)
 * - Structured JSON log format without tokens/PII
 * - /actuator/prometheus metrics with bounded cardinality labels only
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=true"
        }
)
@AutoConfigureObservability
@Testcontainers
final class ObservabilityAndCorrelationIT {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("kafka")
                    .withListener("kafka:19092");

    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.9.9"))
                    .withNetwork(NETWORK)
                    .dependsOn(KAFKA)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
                    .withEnv("SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL", "FULL")
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(90)));

    @DynamicPropertySource
    static void properties(final DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.producer.properties.schema.registry.url", ObservabilityAndCorrelationIT::srUrl);
        registry.add("spring.kafka.consumer.properties.schema.registry.url", ObservabilityAndCorrelationIT::srUrl);
        TestJwtSupport.registerProperties(registry);
    }

    private static String srUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private RelayOutboxUseCase relayOutboxUseCase;

    @Autowired
    private AccountSummaryRepository accountSummaryRepository;

    @Test
    void propagatesSingleCorrelationIdAcrossHttpRelayAndConsumerAndExposesBoundedPrometheusMetrics() throws Exception {
        Logger ledgerLogger = (Logger) LoggerFactory.getLogger("io.ledgerstream");
        ThreadSafeLogAppender appender = new ThreadSafeLogAppender();
        appender.start();
        ledgerLogger.addAppender(appender);

        try {
            String accountId = createAccount("DE-OBS-" + shortId());
            String clientCorrelationId = "corr-e2e-" + shortId();

            ResponseEntity<String> bookingResp = postBooking(
                    accountId,
                    "CREDIT",
                    "120.00",
                    "EUR",
                    "Observability trace",
                    "idem-obs-" + shortId(),
                    clientCorrelationId
            );
            assertThat(bookingResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            assertThat(bookingResp.getHeaders().getFirst(CorrelationIdFilter.HEADER_CORRELATION_ID))
                    .isEqualTo(clientCorrelationId);

            // Verify correlation_id persisted in dedicated outbox_events.correlation_id column
            String storedCorrelationId = jdbcClient.sql(
                            "SELECT correlation_id FROM outbox_events WHERE aggregate_id = :id ORDER BY created_at DESC LIMIT 1")
                    .param("id", UUID.fromString(accountId))
                    .query(String.class)
                    .single();
            assertThat(storedCorrelationId).isEqualTo(clientCorrelationId);

            // Stage 2: Relay outbox -> Kafka
            assertThat(relayOutboxUseCase.relayPendingBatch()).isGreaterThanOrEqualTo(1);

            // Stage 3: Wait for BOTH Consumer DB projection AND post-ack stage=consumer log event
            AccountId domainAccountId = AccountId.of(UUID.fromString(accountId));
            awaitCondition(
                    Duration.ofSeconds(20),
                    () -> accountSummaryRepository.findByAccountId(domainAccountId)
                            .map(s -> s.bookingCount() == 1L)
                            .orElse(false)
                            && appender.events().stream().anyMatch(ev ->
                                    clientCorrelationId.equals(ev.getMDCPropertyMap().get("correlationId"))
                                            && ev.getFormattedMessage().contains("stage=consumer"))
            );

            List<ILoggingEvent> matchingEvents = appender.events().stream()
                    .filter(ev -> clientCorrelationId.equals(ev.getMDCPropertyMap().get("correlationId")))
                    .toList();

            ILoggingEvent httpLog = matchingEvents.stream()
                    .filter(ev -> ev.getFormattedMessage().contains("stage=http"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing stage=http log with correlationId " + clientCorrelationId));
            ILoggingEvent relayLog = matchingEvents.stream()
                    .filter(ev -> ev.getFormattedMessage().contains("stage=relay"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing stage=relay log with correlationId " + clientCorrelationId));
            ILoggingEvent consumerLog = matchingEvents.stream()
                    .filter(ev -> ev.getFormattedMessage().contains("stage=consumer"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing stage=consumer log with correlationId " + clientCorrelationId));

            assertThat(httpLog.getMDCPropertyMap().get("correlationId")).isEqualTo(clientCorrelationId);
            assertThat(relayLog.getMDCPropertyMap().get("correlationId")).isEqualTo(clientCorrelationId);
            assertThat(consumerLog.getMDCPropertyMap().get("correlationId")).isEqualTo(clientCorrelationId);
            assertThat(relayLog.getMDCPropertyMap().get("eventId")).isNotBlank();
            assertThat(consumerLog.getMDCPropertyMap().get("eventId"))
                    .isEqualTo(relayLog.getMDCPropertyMap().get("eventId"));

            LoggingEventCompositeJsonEncoder jsonEncoder = buildJsonEncoder();
            for (ILoggingEvent stageLog : List.of(httpLog, relayLog, consumerLog)) {
                String jsonLine = new String(jsonEncoder.encode(stageLog), StandardCharsets.UTF_8).trim();
                JsonNode parsed = objectMapper.readTree(jsonLine);
                assertThat(parsed.has("ts")).isTrue();
                assertThat(parsed.has("level")).isTrue();
                assertThat(parsed.has("logger")).isTrue();
                assertThat(parsed.has("message")).isTrue();
                assertThat(parsed.get("correlationId").asText()).isEqualTo(clientCorrelationId);
                assertThat(jsonLine).doesNotContain("Bearer ");
                assertThat(jsonLine).doesNotContain("Authorization");
                System.out.println("CORRELATION_STAGE_JSON=" + jsonLine);
            }

            // Verify /actuator/prometheus contains all required Phase 9.1 metrics with bounded tags
            ResponseEntity<String> promResp = restTemplate.getForEntity("/actuator/prometheus", String.class);
            assertThat(promResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            String metricsBody = promResp.getBody();
            assertThat(metricsBody).contains("outbox_unsent");
            assertThat(metricsBody).contains("outbox_oldest_pending_age_seconds");
            assertThat(metricsBody).contains("outbox_events_failed_total");
            assertThat(metricsBody).contains("outbox_events_relayed_total{result=\"success\",type=\"BookingCreated\"}");
            assertThat(metricsBody).contains("consumer_processed_total{outcome=\"processed\"}");
            assertThat(metricsBody).contains("consumer_duplicates_rejected_total{reason=\"duplicate\"}");
            assertThat(metricsBody).contains("consumer_retries_total");
            assertThat(metricsBody).contains("dlq_entries_unhandled");
            assertThat(metricsBody).contains("dlq_replayed_total");
            assertThat(metricsBody).contains("http_server_requests_seconds");

            metricsBody.lines()
                    .filter(line -> !line.startsWith("#"))
                    .filter(line -> line.startsWith("outbox_")
                            || line.startsWith("consumer_")
                            || line.startsWith("dlq_"))
                    .forEach(line -> System.out.println("PROMETHEUS_METRIC=" + line));
        } finally {
            ledgerLogger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void generatesDistinctCorrelationIdsForTwoConsecutiveBookingsAndRejectsUntrustedHeader() throws Exception {
        Logger filterLogger = (Logger) LoggerFactory.getLogger(CorrelationIdFilter.class);
        ThreadSafeLogAppender appender = new ThreadSafeLogAppender();
        appender.start();
        filterLogger.addAppender(appender);

        try {
            String accountId = createAccount("DE-CID-" + shortId());

            ResponseEntity<String> booking1 = postBooking(
                    accountId, "CREDIT", "10.00", "EUR", "First", "idem-cid-1-" + shortId(), null);
            ResponseEntity<String> booking2 = postBooking(
                    accountId, "CREDIT", "20.00", "EUR", "Second", "idem-cid-2-" + shortId(), null);

            String cid1 = booking1.getHeaders().getFirst(CorrelationIdFilter.HEADER_CORRELATION_ID);
            String cid2 = booking2.getHeaders().getFirst(CorrelationIdFilter.HEADER_CORRELATION_ID);
            assertThat(cid1).isNotBlank();
            assertThat(cid2).isNotBlank();
            assertThat(cid1).isNotEqualTo(cid2);
            System.out.println("DISTINCT_CORRELATION_IDS: booking1=" + cid1 + " booking2=" + cid2);

            // Untrusted invalid client header -> NEVER truncated/reused; fresh UUID generated + correlationIdRejected=true
            String untrustedHeader = "bad<script>header-injection!";
            ResponseEntity<String> rejectedHeaderResp = postBooking(
                    accountId, "CREDIT", "5.00", "EUR", "Untrusted header", "idem-cid-3-" + shortId(), untrustedHeader);
            assertThat(rejectedHeaderResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            String replacedCid = rejectedHeaderResp.getHeaders().getFirst(CorrelationIdFilter.HEADER_CORRELATION_ID);
            assertThat(replacedCid).isNotNull();
            assertThat(replacedCid).doesNotContain("bad");
            assertThat(CorrelationIdFilter.VALID_PATTERN.matcher(replacedCid).matches()).isTrue();
            assertThat(appender.events())
                    .anyMatch(ev -> ev.getFormattedMessage().contains("correlationIdRejected=true")
                            && replacedCid.equals(ev.getMDCPropertyMap().get("correlationId")));
        } finally {
            filterLogger.detachAppender(appender);
            appender.stop();
        }
    }

    private static LoggingEventCompositeJsonEncoder buildJsonEncoder() {
        LoggingEventCompositeJsonEncoder encoder = new LoggingEventCompositeJsonEncoder();
        encoder.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        LoggingEventFormattedTimestampJsonProvider ts = new LoggingEventFormattedTimestampJsonProvider();
        ts.setFieldName("ts");
        ts.setTimeZone("UTC");
        LogLevelJsonProvider level = new LogLevelJsonProvider();
        level.setFieldName("level");
        LoggerNameJsonProvider logger = new LoggerNameJsonProvider();
        logger.setFieldName("logger");
        MessageJsonProvider msg = new MessageJsonProvider();
        msg.setFieldName("message");
        MdcJsonProvider mdc = new MdcJsonProvider();
        mdc.setIncludeMdcKeyNames(List.of("correlationId", "eventId"));

        for (JsonProvider<ILoggingEvent> provider : List.of(ts, level, logger, msg, mdc)) {
            encoder.getProviders().addProvider(provider);
        }
        encoder.start();
        return encoder;
    }

    private String createAccount(final String accountNumber) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(TestJwtSupport.instance().writerToken());
        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/accounts",
                new HttpEntity<>("{\"accountNumber\":\"" + accountNumber + "\",\"currency\":\"EUR\"}", headers),
                String.class
        );
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return objectMapper.readTree(response.getBody()).get("id").asText();
    }

    private ResponseEntity<String> postBooking(
            final String accountId,
            final String type,
            final String amount,
            final String currency,
            final String reference,
            final String idempotencyKey,
            final String correlationId
    ) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(TestJwtSupport.instance().writerToken());
        headers.set("Idempotency-Key", idempotencyKey);
        if (correlationId != null) {
            headers.set(CorrelationIdFilter.HEADER_CORRELATION_ID, correlationId);
        }
        String body = """
                {
                  "type": "%s",
                  "amount": "%s",
                  "currency": "%s",
                  "reference": "%s"
                }
                """.formatted(type, amount, currency, reference);
        return restTemplate.postForEntity(
                "/api/v1/accounts/" + accountId + "/bookings",
                new HttpEntity<>(body, headers),
                String.class
        );
    }

    private static void awaitCondition(final Duration timeout, final BooleanSupplier condition)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        assertThat(condition.getAsBoolean()).as("Condition not met within " + timeout).isTrue();
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static final class ThreadSafeLogAppender extends AppenderBase<ILoggingEvent> {
        private final CopyOnWriteArrayList<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(final ILoggingEvent eventObject) {
            eventObject.prepareForDeferredProcessing();
            events.add(eventObject);
        }

        List<ILoggingEvent> events() {
            return events;
        }
    }
}
