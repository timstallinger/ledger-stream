package io.ledgerstream.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.ledgerstream.adapter.in.messaging.LedgerConsumerMessage;
import io.ledgerstream.adapter.in.messaging.LedgerEventRecordProcessor;
import io.ledgerstream.adapter.in.messaging.RawPayloadPreservingAvroDeserializer;
import io.ledgerstream.adapter.out.messaging.KafkaTopicProperties;
import io.ledgerstream.adapter.out.messaging.avro.LedgerBookingEvent;
import io.ledgerstream.app.security.TestJwtSupport;
import io.ledgerstream.application.port.out.AccountSummary;
import io.ledgerstream.application.port.out.AccountSummaryRepository;
import io.ledgerstream.domain.model.AccountId;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=true"
        }
)
@AutoConfigureObservability
@Testcontainers
final class IdempotentConsumerIT {

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
    static void schemaRegistryProperties(final DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.producer.properties.schema.registry.url", IdempotentConsumerIT::schemaRegistryUrl);
        registry.add("spring.kafka.consumer.properties.schema.registry.url", IdempotentConsumerIT::schemaRegistryUrl);
        TestJwtSupport.registerProperties(registry);
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
    private KafkaTemplate<String, LedgerBookingEvent> kafkaTemplate;

    @Autowired
    private KafkaTopicProperties topicProperties;

    @Autowired
    private AccountSummaryRepository accountSummaryRepository;

    @Autowired
    private LedgerEventRecordProcessor recordProcessor;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void tripleDeliveryProducesExactlyOneEffectAndCountsTwoDuplicatesWithReasonLabel() throws Exception {
        String accountId = createAccount("DE-DUP-" + shortId());
        double duplicatesBefore = rejectedCounter("duplicate");

        UUID sharedEventId = UUID.randomUUID();
        LedgerBookingEvent event = avroEvent(sharedEventId, accountId, "CREDIT", "100.00", "EUR", 1L);

        for (int i = 0; i < 3; i++) {
            kafkaTemplate.send(topicProperties.ledgerEvents(), accountId, event).get(10, TimeUnit.SECONDS);
        }

        awaitCondition(
                Duration.ofSeconds(15),
                () -> rejectedCounter("duplicate") >= duplicatesBefore + 2.0
                        && accountSummaryRepository.findByAccountId(AccountId.of(UUID.fromString(accountId))).isPresent()
        );

        AccountSummary summary = accountSummaryRepository
                .findByAccountId(AccountId.of(UUID.fromString(accountId)))
                .orElseThrow();
        assertThat(summary.balance()).isEqualByComparingTo("100.00");
        assertThat(summary.bookingCount()).isEqualTo(1L);
        assertThat(summary.creditTotal()).isEqualByComparingTo("100.00");
        assertThat(summary.debitTotal()).isEqualByComparingTo("0.00");
        assertThat(rejectedCounter("duplicate")).isEqualTo(duplicatesBefore + 2.0);

        ResponseEntity<String> prometheus = restTemplate.getForEntity("/actuator/prometheus", String.class);
        assertThat(prometheus.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(prometheus.getBody()).contains("consumer_duplicates_rejected_total{reason=\"duplicate\"}");
        assertThat(prometheus.getBody()).contains("consumer_duplicates_rejected_total{reason=\"poison\"}");
        prometheus.getBody().lines()
                .filter(line -> !line.startsWith("#"))
                .filter(line -> line.startsWith("outbox_") || line.startsWith("consumer_"))
                .forEach(line -> System.out.println("PROMETHEUS_SAMPLE: " + line));
    }

    @Test
    void c1CrashBetweenDbCommitAndOffsetCommitRedeliversAndDeduplicatesWithoutDoubleEffect() throws Exception {
        String topic = "ledger.events.c1-crash." + shortId();
        String groupId = "c1-crash-group-" + shortId();
        createTopic(topic, 1);

        String accountId = createAccount("DE-C1-" + shortId());
        UUID eventId = UUID.randomUUID();
        LedgerBookingEvent event = avroEvent(eventId, accountId, "CREDIT", "200.00", "EUR", 1L);
        kafkaTemplate.send(topic, accountId, event).get(10, TimeUnit.SECONDS);

        double duplicatesBefore = rejectedCounter("duplicate");

        // Consumer 1: processes event & commits DB transaction, then crashes BEFORE committing Kafka offset
        try (KafkaConsumer<String, LedgerConsumerMessage> consumer1 = createManualAvroConsumer(groupId, 1)) {
            consumer1.subscribe(List.of(topic));
            ConsumerRecord<String, LedgerConsumerMessage> firstDelivery = pollSingleRecord(consumer1, Duration.ofSeconds(15));
            assertThat(firstDelivery).isNotNull();

            assertThatThrownBy(() -> recordProcessor.processRecord(firstDelivery, () -> {
                throw new SimulatedCrashBeforeOffsetCommitException();
            })).isInstanceOf(SimulatedCrashBeforeOffsetCommitException.class);
            // Consumer 1 dies here WITHOUT committing offset
        }

        // Verify DB already committed while Kafka offset was NOT committed
        AccountSummary afterCrash = accountSummaryRepository
                .findByAccountId(AccountId.of(UUID.fromString(accountId)))
                .orElseThrow();
        assertThat(afterCrash.balance()).isEqualByComparingTo("200.00");
        assertThat(afterCrash.bookingCount()).isEqualTo(1L);
        assertThat(rejectedCounter("duplicate")).isEqualTo(duplicatesBefore);

        // Consumer 2 restarts in the EXACT SAME consumer group -> Kafka redelivers the uncommitted offset
        try (KafkaConsumer<String, LedgerConsumerMessage> consumer2 = createManualAvroConsumer(groupId, 1)) {
            consumer2.subscribe(List.of(topic));
            ConsumerRecord<String, LedgerConsumerMessage> redelivery = pollSingleRecord(consumer2, Duration.ofSeconds(15));
            assertThat(redelivery).isNotNull();
            assertThat(redelivery.offset()).isZero();

            recordProcessor.processRecord(redelivery, () -> consumer2.commitSync(Map.of(
                    new TopicPartition(redelivery.topic(), redelivery.partition()),
                    new OffsetAndMetadata(redelivery.offset() + 1)
            )));

            Map<TopicPartition, OffsetAndMetadata> committed =
                    consumer2.committed(Set.of(new TopicPartition(topic, 0)));
            assertThat(committed.get(new TopicPartition(topic, 0)).offset()).isEqualTo(1L);
        }

        AccountSummary afterRecovery = accountSummaryRepository
                .findByAccountId(AccountId.of(UUID.fromString(accountId)))
                .orElseThrow();
        assertThat(afterRecovery.balance()).isEqualByComparingTo("200.00");
        assertThat(afterRecovery.bookingCount()).isEqualTo(1L);
        assertThat(rejectedCounter("duplicate")).isEqualTo(duplicatesBefore + 1.0);
    }

    @Test
    void c2PoisonEventsBetweenValidEventsNeverBlockSamePartition() throws Exception {
        String accountId = createAccount("DE-C2-" + shortId());
        double poisonBefore = rejectedCounter("poison");

        // 1) Valid CREDIT 40.00 on partition(accountId)
        kafkaTemplate.send(
                topicProperties.ledgerEvents(),
                accountId,
                avroEvent(UUID.randomUUID(), accountId, "CREDIT", "40.00", "EUR", 1L)
        ).get(10, TimeUnit.SECONDS);

        // 2) Poison #1: Raw corrupt non-Avro bytes on the SAME key/partition (deserialization failure -> NEVER retry)
        try (KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        ))) {
            rawProducer.send(new ProducerRecord<>(
                    topicProperties.ledgerEvents(),
                    accountId,
                    new byte[]{0x00, 0x01, 0x7f, (byte) 0xfe, 0x42}
            )).get(10, TimeUnit.SECONDS);
        }

        // 3) Poison #2: Schema-valid Avro event with unknown account on the SAME partition key
        kafkaTemplate.send(
                topicProperties.ledgerEvents(),
                accountId,
                avroEvent(UUID.randomUUID(), UUID.randomUUID().toString(), "CREDIT", "15.00", "EUR", 1L)
        ).get(10, TimeUnit.SECONDS);

        // 4) Poison #3: Schema-valid Avro event with amount <= 0 on the SAME partition key
        kafkaTemplate.send(
                topicProperties.ledgerEvents(),
                accountId,
                avroEvent(UUID.randomUUID(), accountId, "DEBIT", "-5.00", "EUR", 2L)
        ).get(10, TimeUnit.SECONDS);

        // 5) Valid CREDIT 60.00 on the SAME key/partition (must NOT be blocked by the 3 poison records!)
        kafkaTemplate.send(
                topicProperties.ledgerEvents(),
                accountId,
                avroEvent(UUID.randomUUID(), accountId, "CREDIT", "60.00", "EUR", 3L)
        ).get(10, TimeUnit.SECONDS);

        AccountId domainAccountId = AccountId.of(UUID.fromString(accountId));
        awaitCondition(
                Duration.ofSeconds(15),
                () -> accountSummaryRepository.findByAccountId(domainAccountId)
                        .map(s -> s.bookingCount() == 2L)
                        .orElse(false)
                        && rejectedCounter("poison") >= poisonBefore + 3.0
        );

        AccountSummary summary = accountSummaryRepository.findByAccountId(domainAccountId).orElseThrow();
        assertThat(summary.balance()).isEqualByComparingTo("100.00");
        assertThat(summary.bookingCount()).isEqualTo(2L);
        assertThat(rejectedCounter("poison")).isEqualTo(poisonBefore + 3.0);
    }

    @Test
    void c3RebuildProjectionFromEventsMatchesLiveStateWithoutReadingBookings() throws Exception {
        String topic = "ledger.events.c3-rebuild." + shortId();
        createTopic(topic, 3);

        String account1 = createAccount("DE-C3A-" + shortId());
        String account2 = createAccount("DE-C3B-" + shortId());

        List<LedgerBookingEvent> streamEvents = List.of(
                avroEvent(UUID.randomUUID(), account1, "CREDIT", "150.00", "EUR", 1L),
                avroEvent(UUID.randomUUID(), account1, "DEBIT", "45.50", "EUR", 2L),
                avroEvent(UUID.randomUUID(), account2, "CREDIT", "300.00", "EUR", 1L),
                avroEvent(UUID.randomUUID(), account2, "DEBIT", "100.25", "EUR", 2L),
                avroEvent(UUID.randomUUID(), account1, "CREDIT", "20.50", "EUR", 3L)
        );
        for (LedgerBookingEvent event : streamEvents) {
            kafkaTemplate.send(topic, event.getAggregateId(), event).get(10, TimeUnit.SECONDS);
        }

        // First pass: live consumption into account_summary
        drainTopicWithManualConsumer(topic, "c3-live-" + shortId(), streamEvents.size());

        AccountSummary live1 = accountSummaryRepository.findByAccountId(AccountId.of(UUID.fromString(account1))).orElseThrow();
        AccountSummary live2 = accountSummaryRepository.findByAccountId(AccountId.of(UUID.fromString(account2))).orElseThrow();
        assertThat(live1.balance()).isEqualByComparingTo("125.00");
        assertThat(live1.bookingCount()).isEqualTo(3L);
        assertThat(live2.balance()).isEqualByComparingTo("199.75");
        assertThat(live2.bookingCount()).isEqualTo(2L);

        // Wipe projection + dedup state for these accounts and rebuild purely from the Kafka event stream
        jdbcClient.sql("DELETE FROM account_summary WHERE account_id IN (:a1, :a2)")
                .param("a1", UUID.fromString(account1))
                .param("a2", UUID.fromString(account2))
                .update();
        for (LedgerBookingEvent event : streamEvents) {
            jdbcClient.sql("DELETE FROM processed_events WHERE event_id = :id")
                    .param("id", UUID.fromString(event.getEventId()))
                    .update();
        }
        assertThat(accountSummaryRepository.findByAccountId(AccountId.of(UUID.fromString(account1)))).isEmpty();

        // Second pass: fresh consumer group replays from offset 0
        drainTopicWithManualConsumer(topic, "c3-rebuild-" + shortId(), streamEvents.size());

        AccountSummary rebuilt1 = accountSummaryRepository.findByAccountId(AccountId.of(UUID.fromString(account1))).orElseThrow();
        AccountSummary rebuilt2 = accountSummaryRepository.findByAccountId(AccountId.of(UUID.fromString(account2))).orElseThrow();

        assertThat(rebuilt1.balance()).isEqualByComparingTo(live1.balance());
        assertThat(rebuilt1.bookingCount()).isEqualTo(live1.bookingCount());
        assertThat(rebuilt1.creditTotal()).isEqualByComparingTo(live1.creditTotal());
        assertThat(rebuilt1.debitTotal()).isEqualByComparingTo(live1.debitTotal());

        assertThat(rebuilt2.balance()).isEqualByComparingTo(live2.balance());
        assertThat(rebuilt2.bookingCount()).isEqualTo(live2.bookingCount());
        assertThat(rebuilt2.creditTotal()).isEqualByComparingTo(live2.creditTotal());
        assertThat(rebuilt2.debitTotal()).isEqualByComparingTo(live2.debitTotal());
    }

    @Test
    void c5ConsumerRebalanceWithTwoJoiningConsumersMaintainsExactlyOnceEffect() throws Exception {
        String topic = "ledger.events.c5-rebalance." + shortId();
        String groupId = "c5-rebalance-group-" + shortId();
        createTopic(topic, 3);

        List<String> accounts = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            accounts.add(createAccount("DE-C5-" + i + "-" + shortId()));
        }

        int totalEvents = 30;
        for (int i = 0; i < totalEvents; i++) {
            String accountId = accounts.get(i % accounts.size());
            LedgerBookingEvent event = avroEvent(UUID.randomUUID(), accountId, "CREDIT", "10.00", "EUR", (i / 6) + 1L);
            kafkaTemplate.send(topic, accountId, event).get(10, TimeUnit.SECONDS);
        }

        AtomicInteger uniqueProcessed = new AtomicInteger(0);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch consumer1Started = new CountDownLatch(1);
        CountDownLatch triggerRebalance = new CountDownLatch(4);

        Runnable consumerLoop = () -> {
            try (KafkaConsumer<String, LedgerConsumerMessage> consumer = createManualAvroConsumer(groupId, 2)) {
                consumer.subscribe(List.of(topic));
                consumer1Started.countDown();
                while (!stop.get()) {
                    ConsumerRecords<String, LedgerConsumerMessage> records = consumer.poll(Duration.ofMillis(200));
                    for (ConsumerRecord<String, LedgerConsumerMessage> record : records) {
                        var outcome = recordProcessor.processRecord(record, () -> consumer.commitSync(Map.of(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        )));
                        if (outcome == io.ledgerstream.application.port.in.EventProcessingOutcome.PROCESSED) {
                            uniqueProcessed.incrementAndGet();
                            triggerRebalance.countDown();
                        }
                    }
                }
            }
        };

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> c1 = executor.submit(consumerLoop);
            assertThat(consumer1Started.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(triggerRebalance.await(15, TimeUnit.SECONDS)).isTrue();

            // Second consumer joins the SAME group mid-stream -> triggers real Kafka consumer group rebalance
            Future<?> c2 = executor.submit(consumerLoop);

            awaitCondition(Duration.ofSeconds(25), () -> uniqueProcessed.get() == totalEvents);
            stop.set(true);
            c1.get(15, TimeUnit.SECONDS);
            c2.get(15, TimeUnit.SECONDS);
        }

        BigDecimal totalBalance = BigDecimal.ZERO;
        long totalBookings = 0L;
        for (String accountId : accounts) {
            AccountSummary summary = accountSummaryRepository
                    .findByAccountId(AccountId.of(UUID.fromString(accountId)))
                    .orElseThrow();
            totalBalance = totalBalance.add(summary.balance());
            totalBookings += summary.bookingCount();
        }

        assertThat(totalBookings).isEqualTo(totalEvents);
        assertThat(totalBalance).isEqualByComparingTo("300.00");
    }

    private void drainTopicWithManualConsumer(
            final String topic,
            final String groupId,
            final int expectedCount
    ) {
        int processed = 0;
        Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
        try (KafkaConsumer<String, LedgerConsumerMessage> consumer = createManualAvroConsumer(groupId, 10)) {
            consumer.subscribe(List.of(topic));
            while (Instant.now().isBefore(deadline) && processed < expectedCount) {
                ConsumerRecords<String, LedgerConsumerMessage> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, LedgerConsumerMessage> record : records) {
                    recordProcessor.processRecord(record, () -> consumer.commitSync(Map.of(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    )));
                    processed++;
                }
            }
        }
        assertThat(processed).isEqualTo(expectedCount);
    }

    private KafkaConsumer<String, LedgerConsumerMessage> createManualAvroConsumer(
            final String groupId,
            final int maxPollRecords
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put("schema.registry.url", schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        KafkaAvroDeserializer avroDelegate = new KafkaAvroDeserializer();
        avroDelegate.configure(props, false);
        RawPayloadPreservingAvroDeserializer valueDeserializer =
                new RawPayloadPreservingAvroDeserializer(avroDelegate);
        return new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer);
    }

    private ConsumerRecord<String, LedgerConsumerMessage> pollSingleRecord(
            final KafkaConsumer<String, LedgerConsumerMessage> consumer,
            final Duration timeout
    ) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, LedgerConsumerMessage> batch = consumer.poll(Duration.ofMillis(250));
            if (!batch.isEmpty()) {
                return batch.iterator().next();
            }
        }
        return null;
    }

    private void createTopic(final String name, final int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()
        ))) {
            admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
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
                .setReference("IT event")
                .setVersion(version)
                .setOccurredAt(Instant.now().toString())
                .setSchemaVersion(2)
                .setInitiationChannel("API")
                .build();
    }

    private double rejectedCounter(final String reason) {
        Counter counter = meterRegistry.find("consumer_duplicates_rejected_total")
                .tag("reason", reason)
                .counter();
        assertThat(counter).isNotNull();
        return counter.count();
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

    private static void awaitCondition(final Duration timeout, final BooleanSupplier condition)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        assertThat(condition.getAsBoolean()).as("Condition not met within " + timeout).isTrue();
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static final class SimulatedCrashBeforeOffsetCommitException extends RuntimeException {
        private SimulatedCrashBeforeOffsetCommitException() {
            super("Simulated crash between DB commit and Kafka offset commit");
        }
    }
}
