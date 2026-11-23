package io.ledgerstream.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.ledgerstream.adapter.out.messaging.KafkaTopicProperties;
import io.ledgerstream.adapter.out.messaging.avro.LedgerBookingEvent;
import io.ledgerstream.application.port.in.CreateBookingCommand;
import io.ledgerstream.application.port.in.CreateBookingUseCase;
import io.ledgerstream.application.port.in.RelayOutboxUseCase;
import io.ledgerstream.application.port.out.OutboxEvent;
import io.ledgerstream.application.port.out.OutboxEventPublisher;
import io.ledgerstream.application.port.out.OutboxRepository;
import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.application.service.OutboxRelayService;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.BookingType;
import io.ledgerstream.domain.model.CurrencyCode;
import io.ledgerstream.domain.model.IdempotencyKey;
import io.ledgerstream.domain.model.Money;
import io.ledgerstream.domain.model.Reference;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "ledger.outbox.relay.enabled=false"
)
@AutoConfigureObservability
@Testcontainers
final class TransactionalOutboxIT {

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
                    .withEnv("SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL", "BACKWARD")
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(90)));

    @DynamicPropertySource
    static void schemaRegistryProperties(final DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.producer.properties.schema.registry.url", TransactionalOutboxIT::schemaRegistryUrl);
    }

    private static String schemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private CreateBookingUseCase createBookingUseCase;

    @Autowired
    private RelayOutboxUseCase relayOutboxUseCase;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private OutboxEventPublisher outboxEventPublisher;

    @Autowired
    private TransactionRunner transactionRunner;

    @Autowired
    private Clock clock;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private KafkaTopicProperties topicProperties;

    @BeforeEach
    void cleanOutboxBeforeEachTest() {
        jdbcClient.sql("DELETE FROM outbox_events").update();
    }

    @Test
    void t1NoOutboxEventWithoutDatabaseCommit() throws Exception {
        String accountId = createAccount("DE-T1-" + UUID.randomUUID().toString().substring(0, 6));

        ResponseEntity<String> rejectedOverdraft = postBooking(
                accountId, "DEBIT", "50.00", "EUR", "Overdraft rollback", "idem-t1-overdraft"
        );
        assertThat(rejectedOverdraft.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

        assertThatThrownBy(() -> createBookingUseCase.execute(
                new CreateBookingCommand(
                        AccountId.of(UUID.fromString(accountId)),
                        BookingType.CREDIT,
                        Money.of(new BigDecimal("40.00"), CurrencyCode.of("EUR")),
                        Reference.of("Forced rollback"),
                        IdempotencyKey.of("idem-t1-forced-rollback")
                ),
                execution -> {
                    throw new IllegalStateException("Simulated crash before DB commit");
                }
        )).isInstanceOf(IllegalStateException.class);

        Integer outboxCount = jdbcClient.sql("SELECT COUNT(*) FROM outbox_events")
                .query(Integer.class)
                .single();
        Integer bookingCount = jdbcClient.sql("SELECT COUNT(*) FROM bookings WHERE account_id = :id")
                .param("id", UUID.fromString(accountId))
                .query(Integer.class)
                .single();

        assertThat(outboxCount).isZero();
        assertThat(bookingCount).isZero();
        assertThat(outboxUnsentGauge()).isEqualTo(0.0);
    }

    @Test
    void t2NoCommitWithoutOutboxEventAndEventDeliveredOnceRelayStarts() throws Exception {
        String accountId = createAccount("DE-T2-" + UUID.randomUUID().toString().substring(0, 6));

        ResponseEntity<String> created = postBooking(
                accountId, "CREDIT", "125.00", "EUR", "Outbox durable booking", "idem-t2-credit"
        );
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String bookingId = objectMapper.readTree(created.getBody()).get("id").asText();

        String statusBeforeRelay = jdbcClient.sql(
                        "SELECT status FROM outbox_events WHERE aggregate_id = :id")
                .param("id", UUID.fromString(accountId))
                .query(String.class)
                .single();
        assertThat(statusBeforeRelay).isEqualTo("PENDING");
        assertThat(outboxUnsentGauge()).isEqualTo(1.0);

        ResponseEntity<String> prometheus = restTemplate.getForEntity("/actuator/prometheus", String.class);
        assertThat(prometheus.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(prometheus.getBody()).contains("outbox_unsent 1.0");

        int relayed = relayOutboxUseCase.relayPendingBatch();
        assertThat(relayed).isEqualTo(1);

        String statusAfterRelay = jdbcClient.sql(
                        "SELECT status FROM outbox_events WHERE aggregate_id = :id")
                .param("id", UUID.fromString(accountId))
                .query(String.class)
                .single();
        assertThat(statusAfterRelay).isEqualTo("SENT");
        assertThat(outboxUnsentGauge()).isEqualTo(0.0);

        List<ConsumerRecord<String, LedgerBookingEvent>> records =
                consumeRecordsForAggregate(accountId, 1, Duration.ofSeconds(15));
        assertThat(records).hasSize(1);

        ConsumerRecord<String, LedgerBookingEvent> record = records.getFirst();
        assertThat(record.key()).isEqualTo(accountId);
        LedgerBookingEvent payload = record.value();
        assertThat(payload.getEventType()).isEqualTo("BookingCreated");
        assertThat(payload.getAggregateId()).isEqualTo(accountId);
        assertThat(payload.getBookingId()).isEqualTo(bookingId);
        assertThat(payload.getOriginalBookingId()).isNull();
        assertThat(payload.getBookingType()).isEqualTo("CREDIT");
        assertThat(payload.getAmount()).isEqualTo("125.00");
        assertThat(payload.getCurrency()).isEqualTo("EUR");
        assertThat(payload.getVersion()).isEqualTo(1L);
        assertThat(payload.getSchemaVersion()).isEqualTo(1);
        assertThat(new String(record.headers().lastHeader("eventId").value(), StandardCharsets.UTF_8))
                .isEqualTo(payload.getEventId());
    }

    @Test
    void t3PublishSucceedsButMarkSentFailsProvesAtLeastOnceAndIdempotentSingleEffect() throws Exception {
        String accountId = createAccount("DE-T3-" + UUID.randomUUID().toString().substring(0, 6));

        ResponseEntity<String> created = postBooking(
                accountId, "CREDIT", "50.00", "EUR", "At-least-once proof", "idem-t3-credit"
        );
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        AtomicBoolean failMarkSentOnce = new AtomicBoolean(true);
        OutboxRepository markSentFaultRepository = new OutboxRepository() {
            @Override
            public void save(final OutboxEvent event) {
                outboxRepository.save(event);
            }

            @Override
            public List<UUID> findPendingIds(final int limit) {
                return outboxRepository.findPendingIds(limit);
            }

            @Override
            public Optional<OutboxEvent> lockPendingById(final UUID eventId) {
                return outboxRepository.lockPendingById(eventId);
            }

            @Override
            public void markSent(final UUID eventId, final Instant sentAt) {
                if (failMarkSentOnce.compareAndSet(true, false)) {
                    throw new IllegalStateException("Forced DB failure immediately after Kafka ack");
                }
                outboxRepository.markSent(eventId, sentAt);
            }

            @Override
            public void recordFailure(final UUID eventId, final String errorMessage) {
                outboxRepository.recordFailure(eventId, errorMessage);
            }

            @Override
            public long countPending() {
                return outboxRepository.countPending();
            }
        };

        OutboxRelayService faultInjectedRelay = new OutboxRelayService(
                markSentFaultRepository,
                outboxEventPublisher,
                transactionRunner,
                clock,
                500
        );

        int firstAttemptRelayed = faultInjectedRelay.relayPendingBatch();
        assertThat(firstAttemptRelayed).isZero();
        assertThat(outboxUnsentGauge()).isEqualTo(1.0);

        String statusAfterFailure = jdbcClient.sql(
                        "SELECT status FROM outbox_events WHERE aggregate_id = :id")
                .param("id", UUID.fromString(accountId))
                .query(String.class)
                .single();
        Integer retryCount = jdbcClient.sql(
                        "SELECT retry_count FROM outbox_events WHERE aggregate_id = :id")
                .param("id", UUID.fromString(accountId))
                .query(Integer.class)
                .single();
        assertThat(statusAfterFailure).isEqualTo("PENDING");
        assertThat(retryCount).isEqualTo(1);

        int secondAttemptRelayed = faultInjectedRelay.relayPendingBatch();
        assertThat(secondAttemptRelayed).isEqualTo(1);
        assertThat(outboxUnsentGauge()).isEqualTo(0.0);

        List<ConsumerRecord<String, LedgerBookingEvent>> delivered =
                consumeRecordsForAggregate(accountId, 2, Duration.ofSeconds(15));
        assertThat(delivered).hasSize(2);
        assertThat(delivered.get(0).value().getEventId())
                .isEqualTo(delivered.get(1).value().getEventId());

        Set<String> processedEventIds = new HashSet<>();
        BigDecimal projectedBalance = BigDecimal.ZERO;
        int duplicatesRejected = 0;
        for (ConsumerRecord<String, LedgerBookingEvent> record : delivered) {
            LedgerBookingEvent event = record.value();
            if (processedEventIds.add(event.getEventId())) {
                projectedBalance = projectedBalance.add(new BigDecimal(event.getAmount()));
            } else {
                duplicatesRejected++;
            }
        }

        assertThat(projectedBalance).isEqualByComparingTo("50.00");
        assertThat(duplicatesRejected).isEqualTo(1);
    }

    @Test
    void cancellationPublishesBookingCancelledAvroEventWithOriginalBookingId() throws Exception {
        String accountId = createAccount("DE-CXL-" + UUID.randomUUID().toString().substring(0, 6));
        ResponseEntity<String> created = postBooking(
                accountId, "CREDIT", "80.00", "EUR", "Original order", "idem-cxl-orig"
        );
        String originalBookingId = objectMapper.readTree(created.getBody()).get("id").asText();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", "idem-cxl-storno");
        ResponseEntity<String> cancelled = restTemplate.postForEntity(
                "/api/v1/accounts/" + accountId + "/bookings/" + originalBookingId + "/cancel",
                new HttpEntity<>("{\"reference\":\"Storno\"}", headers),
                String.class
        );
        assertThat(cancelled.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        assertThat(relayOutboxUseCase.relayPendingBatch()).isEqualTo(2);

        List<ConsumerRecord<String, LedgerBookingEvent>> records =
                consumeRecordsForAggregate(accountId, 2, Duration.ofSeconds(15));
        assertThat(records).hasSize(2);
        LedgerBookingEvent cancelEvent = records.get(1).value();
        assertThat(cancelEvent.getEventType()).isEqualTo("BookingCancelled");
        assertThat(cancelEvent.getOriginalBookingId()).isEqualTo(originalBookingId);
        assertThat(cancelEvent.getBookingType()).isEqualTo("DEBIT");
        assertThat(cancelEvent.getAmount()).isEqualTo("80.00");
        assertThat(cancelEvent.getVersion()).isEqualTo(2L);
    }

    private double outboxUnsentGauge() {
        Gauge gauge = meterRegistry.find("outbox_unsent_total").gauge();
        assertThat(gauge).isNotNull();
        return gauge.value();
    }

    private List<ConsumerRecord<String, LedgerBookingEvent>> consumeRecordsForAggregate(
            final String aggregateId,
            final int expectedCount,
            final Duration timeout
    ) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                "schema.registry.url", schemaRegistryUrl(),
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true
        );

        List<ConsumerRecord<String, LedgerBookingEvent>> matched = new ArrayList<>();
        Instant deadline = Instant.now().plus(timeout);
        try (KafkaConsumer<String, LedgerBookingEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topicProperties.ledgerEvents()));
            while (Instant.now().isBefore(deadline) && matched.size() < expectedCount) {
                ConsumerRecords<String, LedgerBookingEvent> batch = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, LedgerBookingEvent> record : batch) {
                    if (aggregateId.equals(record.key())) {
                        matched.add(record);
                    }
                }
            }
        }
        return matched;
    }

    private String createAccount(final String accountNumber) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
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
            final String idempotencyKey
    ) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", idempotencyKey);
        String body = """
                {
                  "type": "%s",
                  "amount": %s,
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
}
