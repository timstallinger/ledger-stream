package io.ledgerstream.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.adapter.in.messaging.DlqRecordPersister;
import io.ledgerstream.adapter.in.messaging.RawPayloadPreservingAvroDeserializer;
import io.ledgerstream.adapter.out.messaging.KafkaTopicProperties;
import io.ledgerstream.adapter.out.messaging.avro.LedgerBookingEvent;
import io.ledgerstream.adapter.out.persistence.JdbcAccountSummaryRepository;
import io.ledgerstream.application.port.out.AccountSummary;
import io.ledgerstream.application.port.out.AccountSummaryRepository;
import io.ledgerstream.application.port.out.DlqEntry;
import io.ledgerstream.application.port.out.DlqRepository;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.AccountNumber;
import io.ledgerstream.domain.model.BookingType;
import io.ledgerstream.domain.model.CurrencyCode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
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

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=true",
                "ledger.admin.enabled=true",
                "ledger.kafka.retry.max-attempts=5",
                "ledger.kafka.retry.initial-delay-ms=400",
                "ledger.kafka.retry.max-delay-ms=1200"
        }
)
@Import(DlqAndRetryIT.FaultInjectionConfig.class)
@AutoConfigureObservability
@Testcontainers
final class DlqAndRetryIT {

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
        registry.add("spring.kafka.producer.properties.schema.registry.url", DlqAndRetryIT::schemaRegistryUrl);
        registry.add("spring.kafka.consumer.properties.schema.registry.url", DlqAndRetryIT::schemaRegistryUrl);
    }

    private static String schemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    @TestConfiguration
    static class FaultInjectionConfig {

        @Bean
        FaultSwitch faultSwitch() {
            return new FaultSwitch();
        }

        @Bean
        @Primary
        AccountSummaryRepository faultInjectableAccountSummaryRepository(
                final JdbcClient jdbcClient,
                final FaultSwitch faultSwitch
        ) {
            JdbcAccountSummaryRepository delegate = new JdbcAccountSummaryRepository(jdbcClient);
            return new AccountSummaryRepository() {
                @Override
                public void applyBookingDelta(
                        final AccountId accountId,
                        final AccountNumber accountNumber,
                        final CurrencyCode currency,
                        final BookingType bookingType,
                        final BigDecimal amount,
                        final UUID eventId,
                        final Instant occurredAt,
                        final Instant updatedAt
                ) {
                    if (faultSwitch.shouldFail(eventId)) {
                        faultSwitch.markFailed(eventId);
                        throw new TransientDataAccessResourceException("Simulated transient DB failure for " + eventId);
                    }
                    delegate.applyBookingDelta(
                            accountId, accountNumber, currency, bookingType, amount, eventId, occurredAt, updatedAt
                    );
                }

                @Override
                public Optional<AccountSummary> findByAccountId(final AccountId accountId) {
                    return delegate.findByAccountId(accountId);
                }

                @Override
                public List<AccountSummary> findAll() {
                    return delegate.findAll();
                }
            };
        }
    }

    static final class FaultSwitch {
        private final Set<UUID> failingEventIds = ConcurrentHashMap.newKeySet();
        private final Map<UUID, CountDownLatch> firstFailureLatches = new ConcurrentHashMap<>();

        void failEvent(final UUID eventId) {
            failingEventIds.add(eventId);
            firstFailureLatches.put(eventId, new CountDownLatch(1));
        }

        void clearFailure(final UUID eventId) {
            failingEventIds.remove(eventId);
        }

        boolean shouldFail(final UUID eventId) {
            return failingEventIds.contains(eventId);
        }

        void markFailed(final UUID eventId) {
            CountDownLatch latch = firstFailureLatches.get(eventId);
            if (latch != null) {
                latch.countDown();
            }
        }

        boolean awaitFirstFailure(final UUID eventId, final Duration timeout) throws InterruptedException {
            CountDownLatch latch = firstFailureLatches.get(eventId);
            return latch != null && latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private KafkaTemplate<String, LedgerBookingEvent> kafkaTemplate;

    @Autowired
    @Qualifier("rawBytesReplayKafkaTemplate")
    private KafkaTemplate<String, byte[]> rawBytesKafkaTemplate;

    @Autowired
    private KafkaTopicProperties topicProperties;

    @Autowired
    private AccountSummaryRepository accountSummaryRepository;

    @Autowired
    private DlqRepository dlqRepository;

    @Autowired
    private FaultSwitch faultSwitch;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void t1T3T4TechnicalFailureExhaustsFiveAttemptsLandsInDlqAndReplaysIdempotently() throws Exception {
        String accountId = createAccount("DE-T134-" + shortId());
        UUID failingEventId = UUID.randomUUID();
        String correlationId = "corr-t1-" + shortId();
        faultSwitch.failEvent(failingEventId);

        double retriesBefore = counterValue("consumer_retries_total");
        double replayedBefore = counterValue("dlq_replayed_total");
        double unhandledBefore = gaugeValue("dlq_entries_unhandled");

        LedgerBookingEvent event = avroEvent(failingEventId, accountId, "CREDIT", "111.00", "EUR", 1L);
        ProducerRecord<String, LedgerBookingEvent> producerRecord =
                new ProducerRecord<>(topicProperties.ledgerEvents(), accountId, event);
        producerRecord.headers().add(new RecordHeader("X-Correlation-Id", correlationId.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(producerRecord).get(10, TimeUnit.SECONDS);

        // T1: Wait until all 5 attempts fail and DLQ entry is persisted
        awaitCondition(Duration.ofSeconds(30), () -> findDlqEntryByEventId(failingEventId).isPresent());

        DlqEntry dlqEntry = findDlqEntryByEventId(failingEventId).orElseThrow();
        assertThat(dlqEntry.attemptCount()).isEqualTo(5);
        assertThat(dlqEntry.topic()).isEqualTo(topicProperties.ledgerEvents());
        assertThat(dlqEntry.errorClass()).contains("TransientDataAccessResourceException");
        assertThat(dlqEntry.payload()).isNotEmpty();
        assertThat(dlqEntry.replayedAt()).isNull();
        assertThat(dlqEntry.headers())
                .containsEntry(DlqRecordPersister.HEADER_ATTEMPT_COUNT, "5")
                .containsEntry(DlqRecordPersister.HEADER_ORIGINAL_TOPIC, topicProperties.ledgerEvents())
                .containsEntry(DlqRecordPersister.HEADER_CORRELATION_ID, correlationId)
                .containsKeys(
                        DlqRecordPersister.HEADER_ERROR_CLASS,
                        DlqRecordPersister.HEADER_ERROR_MESSAGE,
                        DlqRecordPersister.HEADER_ORIGINAL_PARTITION,
                        DlqRecordPersister.HEADER_ORIGINAL_OFFSET,
                        DlqRecordPersister.HEADER_FAILED_AT,
                        DlqRecordPersister.HEADER_SCHEMA_ID
                );
        assertThat(counterValue("consumer_retries_total")).isGreaterThanOrEqualTo(retriesBefore + 4.0);
        assertThat(gaugeValue("dlq_entries_unhandled")).isGreaterThanOrEqualTo(unhandledBefore + 1.0);
        assertThat(accountSummaryRepository.findByAccountId(AccountId.of(UUID.fromString(accountId)))).isEmpty();

        // T3: Fix underlying issue, replay DLQ entry via admin endpoint -> processed exactly once & marked replayed
        faultSwitch.clearFailure(failingEventId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> replayResponse = restTemplate.postForEntity(
                "/admin/dlq/" + dlqEntry.id() + "/replay",
                new HttpEntity<>("{\"replayedBy\":\"operator-test\",\"note\":\"DB recovered\"}", headers),
                String.class
        );
        assertThat(replayResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode replayJson = objectMapper.readTree(replayResponse.getBody());
        assertThat(replayJson.get("replayedAt").isNull()).isFalse();
        assertThat(replayJson.get("replayedBy").asText()).isEqualTo("operator-test");
        assertThat(replayJson.get("note").asText()).isEqualTo("DB recovered");
        assertThat(counterValue("dlq_replayed_total")).isEqualTo(replayedBefore + 1.0);

        AccountId domainAccountId = AccountId.of(UUID.fromString(accountId));
        awaitCondition(
                Duration.ofSeconds(15),
                () -> accountSummaryRepository.findByAccountId(domainAccountId)
                        .map(s -> s.bookingCount() == 1L)
                        .orElse(false)
        );
        AccountSummary afterReplay = accountSummaryRepository.findByAccountId(domainAccountId).orElseThrow();
        assertThat(afterReplay.balance()).isEqualByComparingTo("111.00");
        assertThat(afterReplay.bookingCount()).isEqualTo(1L);

        // Unknown DLQ id -> 404
        ResponseEntity<String> unknownReplay = restTemplate.postForEntity(
                "/admin/dlq/" + UUID.randomUUID() + "/replay",
                new HttpEntity<>("{}", headers),
                String.class
        );
        assertThat(unknownReplay.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // T4a: Replaying the exact same DLQ entry a second time -> 409 Conflict
        ResponseEntity<String> duplicateHttpReplay = restTemplate.postForEntity(
                "/admin/dlq/" + dlqEntry.id() + "/replay",
                new HttpEntity<>("{\"replayedBy\":\"operator-test\",\"note\":\"Accidental double click\"}", headers),
                String.class
        );
        assertThat(duplicateHttpReplay.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        // T4b: Even if a duplicate DLQ row with the same eventId is replayed -> processed_events prevents second effect
        double dupBefore = meterRegistry.find("consumer_duplicates_rejected_total")
                .tag("reason", "duplicate")
                .counter()
                .count();
        UUID secondDlqId = UUID.randomUUID();
        dlqRepository.save(new DlqEntry(
                secondDlqId,
                dlqEntry.topic(),
                dlqEntry.partition(),
                dlqEntry.offset() + 1000,
                dlqEntry.recordKey(),
                dlqEntry.eventId(),
                dlqEntry.errorClass(),
                dlqEntry.errorMessage(),
                dlqEntry.attemptCount(),
                dlqEntry.payload(),
                dlqEntry.headers(),
                Instant.now(),
                null,
                null,
                null
        ));
        ResponseEntity<String> secondReplay = restTemplate.postForEntity(
                "/admin/dlq/" + secondDlqId + "/replay",
                new HttpEntity<>("{\"replayedBy\":\"operator-test\",\"note\":\"Duplicate replay proof\"}", headers),
                String.class
        );
        assertThat(secondReplay.getStatusCode()).isEqualTo(HttpStatus.OK);

        awaitCondition(
                Duration.ofSeconds(15),
                () -> meterRegistry.find("consumer_duplicates_rejected_total")
                        .tag("reason", "duplicate")
                        .counter()
                        .count() >= dupBefore + 1.0
        );
        AccountSummary afterDoubleReplay = accountSummaryRepository.findByAccountId(domainAccountId).orElseThrow();
        assertThat(afterDoubleReplay.balance()).isEqualByComparingTo("111.00");
        assertThat(afterDoubleReplay.bookingCount()).isEqualTo(1L);

        // Print Prometheus metrics sample for verification report
        ResponseEntity<String> prometheus = restTemplate.getForEntity("/actuator/prometheus", String.class);
        assertThat(prometheus.getStatusCode()).isEqualTo(HttpStatus.OK);
        prometheus.getBody().lines()
                .filter(line -> !line.startsWith("#"))
                .filter(line -> line.startsWith("dlq_") || line.startsWith("consumer_retries"))
                .forEach(line -> System.out.println("PHASE5_PROMETHEUS: " + line));
    }

    @Test
    void t2PartitionContinuesProcessingValidMessageUnderTwoSecondsWhileFailingMessageRetries() throws Exception {
        String accountId = createAccount("DE-T2-" + shortId());
        UUID failingEventId = UUID.randomUUID();
        faultSwitch.failEvent(failingEventId);

        // 1) Publish failing event on partition(accountId)
        kafkaTemplate.send(
                topicProperties.ledgerEvents(),
                accountId,
                avroEvent(failingEventId, accountId, "CREDIT", "25.00", "EUR", 1L)
        ).get(10, TimeUnit.SECONDS);

        assertThat(faultSwitch.awaitFirstFailure(failingEventId, Duration.ofSeconds(15))).isTrue();
        long startNanos = System.nanoTime();

        // 2) Immediately publish valid event on the EXACT SAME key/partition while failingEventId is in retry
        kafkaTemplate.send(
                topicProperties.ledgerEvents(),
                accountId,
                avroEvent(UUID.randomUUID(), accountId, "CREDIT", "75.00", "EUR", 2L)
        ).get(10, TimeUnit.SECONDS);

        AccountId domainAccountId = AccountId.of(UUID.fromString(accountId));
        awaitCondition(
                Duration.ofSeconds(5),
                () -> accountSummaryRepository.findByAccountId(domainAccountId)
                        .map(s -> s.bookingCount() == 1L)
                        .orElse(false)
        );
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        System.out.println("T2_NON_BLOCKING_ELAPSED_MS=" + elapsedMs);

        // Valid event processed in < 2000 ms on the SAME partition while failing event is still retrying
        assertThat(elapsedMs).isLessThan(2000L);
        assertThat(accountSummaryRepository.findByAccountId(domainAccountId).orElseThrow().balance())
                .isEqualByComparingTo("75.00");

        // Eventually the failing event finishes its 5 attempts and lands in DLQ
        awaitCondition(Duration.ofSeconds(30), () -> findDlqEntryByEventId(failingEventId).isPresent());
        assertThat(findDlqEntryByEventId(failingEventId).orElseThrow().attemptCount()).isEqualTo(5);
    }

    @Test
    void t5DeserializationPoisonPreservesByteIdenticalRawPayloadAndSucceedsOnReplayAfterSchemaFix() throws Exception {
        String accountId = createAccount("DE-T5-" + shortId());
        UUID eventId = UUID.randomUUID();

        // Determine next unused Schema Registry ID so deserialization fails deterministically before schema registration
        int nextSchemaId = fetchHighestSchemaId() + 1;

        // Build a valid Avro schema (distinct doc so Schema Registry registers it as nextSchemaId on fix)
        String distinctSchemaJson = """
                {
                  "type": "record",
                  "name": "LedgerBookingEvent",
                  "namespace": "io.ledgerstream.adapter.out.messaging.avro",
                  "doc": "Schema registered during T5 recovery %s",
                  "fields": [
                    { "name": "eventId", "type": "string" },
                    { "name": "eventType", "type": "string" },
                    { "name": "aggregateId", "type": "string" },
                    { "name": "bookingId", "type": "string" },
                    { "name": "originalBookingId", "type": ["null", "string"], "default": null },
                    { "name": "bookingType", "type": "string" },
                    { "name": "amount", "type": "string" },
                    { "name": "currency", "type": "string" },
                    { "name": "reference", "type": "string" },
                    { "name": "version", "type": "long" },
                    { "name": "occurredAt", "type": "string" },
                    { "name": "schemaVersion", "type": "int", "default": 1 }
                  ]
                }
                """.formatted(shortId());

        byte[] rawWirePayload = encodeConfluentAvroBytes(
                new Schema.Parser().parse(distinctSchemaJson),
                nextSchemaId,
                eventId,
                accountId,
                "88.88"
        );

        // Publish raw bytes BEFORE schema nextSchemaId exists in Schema Registry -> real deserialization failure!
        rawBytesKafkaTemplate.send(topicProperties.ledgerEvents(), accountId, rawWirePayload).get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> findDlqEntryByRecordKey(accountId).isPresent());

        DlqEntry poisonEntry = findDlqEntryByRecordKey(accountId).orElseThrow();
        assertThat(poisonEntry.attemptCount()).isEqualTo(1);
        assertThat(poisonEntry.payload()).isEqualTo(rawWirePayload);
        assertThat(poisonEntry.headers()).containsKey(RawPayloadPreservingAvroDeserializer.HEADER_ORIGINAL_PAYLOAD);
        byte[] decodedHeaderBytes = Base64.getDecoder().decode(
                poisonEntry.headers().get(RawPayloadPreservingAvroDeserializer.HEADER_ORIGINAL_PAYLOAD)
        );
        assertThat(decodedHeaderBytes).isEqualTo(rawWirePayload);
        assertThat(poisonEntry.headers().get(DlqRecordPersister.HEADER_SCHEMA_ID))
                .isEqualTo(Integer.toString(nextSchemaId));

        // Log SQL row (without binary payload) for Phase 5 verification report
        List<Map<String, Object>> sqlRows = jdbcClient.sql(
                        "SELECT id, error_class, attempt_count FROM dlq_entries WHERE id = :id")
                .param("id", poisonEntry.id())
                .query()
                .listOfRows();
        System.out.println("T5_DLQ_SQL_ROW=" + sqlRows);

        // Schema Fix: register the missing schema in Schema Registry -> gets assigned nextSchemaId
        int registeredId = registerSchema("ledger.events-t5-fix-" + shortId() + "-value", distinctSchemaJson);
        assertThat(registeredId).isEqualTo(nextSchemaId);

        // Replay the preserved raw bytes -> consumer now deserializes cleanly and projects +88.88!
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> replayResponse = restTemplate.postForEntity(
                "/admin/dlq/" + poisonEntry.id() + "/replay",
                new HttpEntity<>("{\"replayedBy\":\"schema-admin\",\"note\":\"Registered missing schema\"}", headers),
                String.class
        );
        assertThat(replayResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        AccountId domainAccountId = AccountId.of(UUID.fromString(accountId));
        awaitCondition(
                Duration.ofSeconds(15),
                () -> accountSummaryRepository.findByAccountId(domainAccountId)
                        .map(s -> s.bookingCount() == 1L)
                        .orElse(false)
        );
        AccountSummary recovered = accountSummaryRepository.findByAccountId(domainAccountId).orElseThrow();
        assertThat(recovered.balance()).isEqualByComparingTo("88.88");
        assertThat(recovered.bookingCount()).isEqualTo(1L);
    }

    private Optional<DlqEntry> findDlqEntryByEventId(final UUID eventId) {
        return dlqRepository.findUnhandled().stream()
                .filter(entry -> eventId.equals(entry.eventId()))
                .findFirst();
    }

    private Optional<DlqEntry> findDlqEntryByRecordKey(final String recordKey) {
        return dlqRepository.findUnhandled().stream()
                .filter(entry -> recordKey.equals(entry.recordKey()))
                .findFirst();
    }

    private int fetchHighestSchemaId() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(schemaRegistryUrl() + "/schemas"))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode array = objectMapper.readTree(response.body());
        int maxId = 0;
        for (JsonNode item : array) {
            maxId = Math.max(maxId, item.get("id").asInt());
        }
        return maxId;
    }

    private int registerSchema(final String subject, final String schemaJson) throws Exception {
        String body = objectMapper.writeValueAsString(Map.of("schema", schemaJson));
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(schemaRegistryUrl() + "/subjects/" + subject + "/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return objectMapper.readTree(response.body()).get("id").asInt();
    }

    private static byte[] encodeConfluentAvroBytes(
            final Schema schema,
            final int schemaId,
            final UUID eventId,
            final String accountId,
            final String amount
    ) throws Exception {
        GenericRecord record = new GenericData.Record(schema);
        record.put("eventId", eventId.toString());
        record.put("eventType", "BookingCreated");
        record.put("aggregateId", accountId);
        record.put("bookingId", UUID.randomUUID().toString());
        record.put("originalBookingId", null);
        record.put("bookingType", "CREDIT");
        record.put("amount", amount);
        record.put("currency", "EUR");
        record.put("reference", "T5 schema recovery");
        record.put("version", 1L);
        record.put("occurredAt", Instant.now().toString());
        record.put("schemaVersion", 1);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x00);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private LedgerBookingEvent avroEvent(
            final UUID eventId,
            final String accountId,
            final String bookingType,
            final String amount,
            final String currency,
            final long version
    ) {
        return LedgerBookingEvent.newBuilder()
                .setEventId(eventId.toString())
                .setEventType("BookingCreated")
                .setAggregateId(accountId)
                .setBookingId(UUID.randomUUID().toString())
                .setOriginalBookingId(null)
                .setBookingType(bookingType)
                .setAmount(amount)
                .setCurrency(currency)
                .setReference("Phase 5 IT")
                .setVersion(version)
                .setOccurredAt(Instant.now().toString())
                .setSchemaVersion(1)
                .build();
    }

    private double counterValue(final String name) {
        Counter counter = meterRegistry.find(name).counter();
        assertThat(counter).isNotNull();
        return counter.count();
    }

    private double gaugeValue(final String name) {
        Gauge gauge = meterRegistry.find(name).gauge();
        assertThat(gauge).isNotNull();
        return gauge.value();
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
}
