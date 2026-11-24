package io.ledgerstream.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.adapter.in.messaging.LedgerEventRecordMapper;
import io.ledgerstream.adapter.in.messaging.RawPayloadPreservingAvroDeserializer;
import io.ledgerstream.adapter.out.messaging.KafkaTopicProperties;
import io.ledgerstream.adapter.out.messaging.avro.LedgerBookingEvent;
import io.ledgerstream.app.security.TestJwtSupport;
import io.ledgerstream.application.port.in.IncomingLedgerEvent;
import io.ledgerstream.application.port.in.RelayOutboxUseCase;
import io.ledgerstream.application.port.out.AccountSummary;
import io.ledgerstream.application.port.out.AccountSummaryRepository;
import io.ledgerstream.application.port.out.DlqEntry;
import io.ledgerstream.application.port.out.DlqRepository;
import io.ledgerstream.domain.model.AccountId;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=true",
                "ledger.admin.enabled=true"
        }
)
@Testcontainers
final class SchemaEvolutionIT {

    private static final Network NETWORK = Network.newNetwork();
    private static final String SUBJECT = "ledger.events-value";

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

    private static final ObjectMapper STATIC_MAPPER = new ObjectMapper();
    private static String v1SchemaJson;
    private static String v2SchemaJson;
    private static String incompatibleSchemaJson;
    private static int v1SchemaId;
    private static int v2SchemaId;

    @DynamicPropertySource
    static void schemaRegistryProperties(final DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.producer.properties.schema.registry.url", SchemaEvolutionIT::schemaRegistryUrl);
        registry.add("spring.kafka.consumer.properties.schema.registry.url", SchemaEvolutionIT::schemaRegistryUrl);
        TestJwtSupport.registerProperties(registry);
    }

    @BeforeAll
    static void registerV1ThenV2UnderFullCompatibility() throws Exception {
        v1SchemaJson = loadResource("/avro/LedgerBookingEventV1.avsc");
        v2SchemaJson = LedgerBookingEvent.getClassSchema().toString();
        incompatibleSchemaJson = loadResource("/avro/LedgerBookingEventIncompatible.avsc");

        // 1) Verify global compatibility level is FULL
        HttpResponse<String> configResp = httpGet(schemaRegistryUrl() + "/config");
        assertThat(configResp.statusCode()).isEqualTo(200);
        System.out.println("REGISTRY_CONFIG_OUTPUT=" + configResp.body());
        assertThat(STATIC_MAPPER.readTree(configResp.body()).get("compatibilityLevel").asText()).isEqualTo("FULL");

        // 2) Register v1 first
        HttpResponse<String> v1Reg = registerSchema(SUBJECT, v1SchemaJson);
        assertThat(v1Reg.statusCode()).isEqualTo(200);
        v1SchemaId = STATIC_MAPPER.readTree(v1Reg.body()).get("id").asInt();

        // 3) Run explicit Schema Registry FULL compatibility check for v2 against v1
        HttpResponse<String> v2Compat = checkCompatibility(SUBJECT, v2SchemaJson);
        assertThat(v2Compat.statusCode()).isEqualTo(200);
        assertThat(STATIC_MAPPER.readTree(v2Compat.body()).get("is_compatible").asBoolean()).isTrue();

        // 4) Register v2 -> receives a distinct schema ID from v1
        HttpResponse<String> v2Reg = registerSchema(SUBJECT, v2SchemaJson);
        assertThat(v2Reg.statusCode()).isEqualTo(200);
        v2SchemaId = STATIC_MAPPER.readTree(v2Reg.body()).get("id").asInt();
        assertThat(v2SchemaId).isNotEqualTo(v1SchemaId);

        System.out.println("SCHEMA_COMPATIBILITY_CHECK: executed subject=" + SUBJECT
                + " compatibilityLevel=FULL v1SchemaId=" + v1SchemaId
                + " v2SchemaId=" + v2SchemaId + " v1_to_v2_compatible=true");
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
    @Qualifier("rawBytesReplayKafkaTemplate")
    private KafkaTemplate<String, byte[]> rawBytesKafkaTemplate;

    @Autowired
    private KafkaTopicProperties topicProperties;

    @Autowired
    private AccountSummaryRepository accountSummaryRepository;

    @Autowired
    private DlqRepository dlqRepository;

    @Autowired
    private LedgerEventRecordMapper recordMapper;

    @Test
    void direction1_v2ConsumerReadsV1DataAndResolvesNonNullDefault() throws Exception {
        String accountId = createAccount("DE-V2R1-" + shortId());
        UUID eventId = UUID.randomUUID();
        Schema v1Schema = new Schema.Parser().parse(v1SchemaJson);
        Schema v2Schema = LedgerBookingEvent.getClassSchema();

        byte[] v1WireBytes = encodeV1WireBytes(v1Schema, v1SchemaId, eventId, accountId, "65.00");

        // Verify Avro v2 reader schema resolves non-null default "API" on v1 wire payload (which lacks initiationChannel)
        GenericRecord decodedWithV2Reader = decodeWithReaderSchema(v1WireBytes, v1Schema, v2Schema);
        assertThat(decodedWithV2Reader.get("initiationChannel")).isNotNull();
        assertThat(decodedWithV2Reader.get("initiationChannel").toString()).isEqualTo("API");

        // Also verify consumer mapper resolves non-null default when GenericRecord uses v1 schema directly
        GenericRecord decodedWithV1Reader = decodeWithReaderSchema(v1WireBytes, v1Schema, v1Schema);
        IncomingLedgerEvent mapped = recordMapper.map(decodedWithV1Reader).orElseThrow();
        assertThat(mapped.initiationChannel()).isEqualTo("API");

        // Publish v1 bytes onto ledger.events -> live v2 consumer projects it cleanly into account_summary
        rawBytesKafkaTemplate.send(topicProperties.ledgerEvents(), accountId, v1WireBytes).get(10, TimeUnit.SECONDS);

        AccountId domainAccountId = AccountId.of(UUID.fromString(accountId));
        awaitCondition(
                Duration.ofSeconds(15),
                () -> accountSummaryRepository.findByAccountId(domainAccountId)
                        .map(s -> s.bookingCount() == 1L)
                        .orElse(false)
        );
        AccountSummary summary = accountSummaryRepository.findByAccountId(domainAccountId).orElseThrow();
        assertThat(summary.balance()).isEqualByComparingTo("65.00");
        System.out.println("DIRECTION_V2_READS_V1: passed v1SchemaId=" + v1SchemaId
                + " resolvedInitiationChannel=" + decodedWithV2Reader.get("initiationChannel")
                + " projectedBalance=" + summary.balance());
    }

    @Test
    void direction2_v1ConsumerReadsV2DataAndPayloadSchemaVersionMatchesV2RegistryId() throws Exception {
        String accountId = createAccount("DE-V1R2-" + shortId());

        ResponseEntity<String> booked = postBooking(accountId, "CREDIT", "140.50", "EUR", "v2 booking", "idem-v2-" + shortId());
        assertThat(booked.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(relayOutboxUseCase.relayPendingBatch()).isGreaterThanOrEqualTo(1);

        // Consume raw Kafka bytes produced by Outbox Relay and inspect Confluent wire header + v1 reader compatibility
        byte[] rawV2Bytes = consumeRawBytesForKey(accountId, Duration.ofSeconds(15));
        assertThat(rawV2Bytes).isNotNull();

        Integer wireSchemaId = RawPayloadPreservingAvroDeserializer.extractConfluentSchemaId(rawV2Bytes);
        int latestRegistrySchemaId = fetchLatestSubjectSchemaId(SUBJECT);
        assertThat(wireSchemaId).isEqualTo(latestRegistrySchemaId);

        Schema schemaFromRegistry = fetchSchemaById(wireSchemaId);
        Schema v2Schema = LedgerBookingEvent.getClassSchema();
        List<String> registryFieldNames = schemaFromRegistry.getFields().stream().map(Schema.Field::name).toList();
        List<String> avscFieldNames = v2Schema.getFields().stream().map(Schema.Field::name).toList();
        assertThat(registryFieldNames).containsExactlyElementsOf(avscFieldNames);
        for (Schema.Field avscField : v2Schema.getFields()) {
            Schema.Field regField = schemaFromRegistry.getField(avscField.name());
            assertThat(regField).isNotNull();
            assertThat(regField.schema().getType()).isEqualTo(avscField.schema().getType());
        }

        Schema v1Schema = new Schema.Parser().parse(v1SchemaJson);

        // Decode with v2 schema -> verify application schemaVersion == 2 AND initiationChannel == "API"
        GenericRecord v2Record = decodeWithReaderSchema(rawV2Bytes, v2Schema, v2Schema);
        assertThat(((Number) v2Record.get("schemaVersion")).intValue()).isEqualTo(2);
        assertThat(v2Record.get("initiationChannel").toString()).isEqualTo("API");

        // Decode the EXACT SAME v2 bytes using a v1-stand Consumer (v1 reader schema) -> must succeed without error!
        GenericRecord v1ReadOfV2 = decodeWithReaderSchema(rawV2Bytes, v2Schema, v1Schema);
        assertThat(v1ReadOfV2.get("aggregateId").toString()).isEqualTo(accountId);
        assertThat(v1ReadOfV2.get("amount").toString()).isEqualTo("140.50");
        assertThat(((Number) v1ReadOfV2.get("schemaVersion")).intValue()).isEqualTo(2);
        assertThat(v1ReadOfV2.getSchema().getField("initiationChannel")).isNull();

        System.out.println("DIRECTION_V1_READS_V2: passed wireSchemaId=" + wireSchemaId
                + " latestRegistrySchemaId=" + latestRegistrySchemaId
                + " registryFieldsMatchAvsc=true"
                + " payloadSchemaVersion=" + v2Record.get("schemaVersion")
                + " v1ReadAmount=" + v1ReadOfV2.get("amount"));
    }

    @Test
    void negativeExample_incompatibleSchemaWithRequiredFieldWithoutDefaultIsRejectedByRegistry() throws Exception {
        HttpResponse<String> compatResp = checkCompatibility(SUBJECT, incompatibleSchemaJson);
        assertThat(compatResp.statusCode()).isEqualTo(200);
        assertThat(STATIC_MAPPER.readTree(compatResp.body()).get("is_compatible").asBoolean()).isFalse();

        HttpResponse<String> registerResp = registerSchema(SUBJECT, incompatibleSchemaJson);
        System.out.println("INCOMPATIBLE_SCHEMA_REJECTION: httpStatus=" + registerResp.statusCode()
                + " body=" + registerResp.body());

        assertThat(registerResp.statusCode()).isEqualTo(409);
        JsonNode errorJson = STATIC_MAPPER.readTree(registerResp.body());
        assertThat(errorJson.get("error_code").asInt()).isEqualTo(409);
        assertThat(errorJson.get("message").asText()).containsIgnoringCase("incompatible");
    }

    @Test
    void v2EventPreservesByteIdenticalRawPayloadInDlqAndReplaysCleanly() throws Exception {
        // Send a v2 event for a NOT-YET-CREATED account -> routes straight to DLQ preserving exact v2 raw bytes
        String unknownAccountId = UUID.randomUUID().toString();
        UUID eventId = UUID.randomUUID();
        Schema v2Schema = LedgerBookingEvent.getClassSchema();
        byte[] v2RawBytes = encodeV2WireBytes(v2Schema, v2SchemaId, eventId, unknownAccountId, "55.00", "WEB");

        rawBytesKafkaTemplate.send(topicProperties.ledgerEvents(), unknownAccountId, v2RawBytes).get(10, TimeUnit.SECONDS);

        awaitCondition(Duration.ofSeconds(15), () -> dlqRepository.findUnhandled().stream()
                .anyMatch(e -> eventId.equals(e.eventId())));

        DlqEntry entry = dlqRepository.findUnhandled().stream()
                .filter(e -> eventId.equals(e.eventId()))
                .findFirst()
                .orElseThrow();

        assertThat(entry.payload()).isEqualTo(v2RawBytes);
        assertThat(RawPayloadPreservingAvroDeserializer.extractConfluentSchemaId(entry.payload())).isEqualTo(v2SchemaId);

        // Remedy the missing account before replay -> v2 DLQ replay must project cleanly into account_summary
        UUID resolvedAccountId = UUID.fromString(unknownAccountId);
        jdbcClient.sql("""
                        INSERT INTO accounts (id, account_number, balance, currency, version)
                        VALUES (:id, :accountNumber, 0.00, 'EUR', 0)
                        """)
                .param("id", resolvedAccountId)
                .param("accountNumber", "DE-DLQV2-" + shortId())
                .update();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(TestJwtSupport.instance().operatorToken());
        ResponseEntity<String> replayResponse = restTemplate.postForEntity(
                "/admin/dlq/" + entry.id() + "/replay",
                new HttpEntity<>("{\"replayedBy\":\"schema-test\",\"note\":\"v2 replay after account created\"}", headers),
                String.class
        );
        assertThat(replayResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        AccountId domainAccountId = AccountId.of(resolvedAccountId);
        awaitCondition(
                Duration.ofSeconds(15),
                () -> accountSummaryRepository.findByAccountId(domainAccountId)
                        .map(s -> s.bookingCount() == 1L)
                        .orElse(false)
        );
        assertThat(accountSummaryRepository.findByAccountId(domainAccountId).orElseThrow().balance())
                .isEqualByComparingTo("55.00");
    }

    private static String schemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    private static HttpResponse<String> httpGet(final String url) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        return client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static int fetchLatestSubjectSchemaId(final String subject) throws Exception {
        HttpResponse<String> response = httpGet(schemaRegistryUrl() + "/subjects/" + subject + "/versions/latest");
        assertThat(response.statusCode()).isEqualTo(200);
        return STATIC_MAPPER.readTree(response.body()).get("id").asInt();
    }

    private static Schema fetchSchemaById(final int schemaId) throws Exception {
        HttpResponse<String> response = httpGet(schemaRegistryUrl() + "/schemas/ids/" + schemaId);
        assertThat(response.statusCode()).isEqualTo(200);
        String rawSchema = STATIC_MAPPER.readTree(response.body()).get("schema").asText();
        return new Schema.Parser().parse(rawSchema);
    }

    private static HttpResponse<String> registerSchema(final String subject, final String schemaJson) throws Exception {
        String body = STATIC_MAPPER.writeValueAsString(Map.of("schema", schemaJson));
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(schemaRegistryUrl() + "/subjects/" + subject + "/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> checkCompatibility(final String subject, final String schemaJson) throws Exception {
        String body = STATIC_MAPPER.writeValueAsString(Map.of("schema", schemaJson));
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(schemaRegistryUrl() + "/compatibility/subjects/" + subject + "/versions/latest"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static byte[] encodeV1WireBytes(
            final Schema v1Schema,
            final int schemaId,
            final UUID eventId,
            final String accountId,
            final String amount
    ) throws Exception {
        GenericRecord record = new GenericData.Record(v1Schema);
        record.put("eventId", eventId.toString());
        record.put("eventType", "BookingCreated");
        record.put("aggregateId", accountId);
        record.put("bookingId", UUID.randomUUID().toString());
        record.put("originalBookingId", null);
        record.put("bookingType", "CREDIT");
        record.put("amount", amount);
        record.put("currency", "EUR");
        record.put("reference", "v1 producer payload");
        record.put("version", 1L);
        record.put("occurredAt", Instant.now().toString());
        record.put("schemaVersion", 1);
        return writeConfluentAvro(v1Schema, schemaId, record);
    }

    private static byte[] encodeV2WireBytes(
            final Schema v2Schema,
            final int schemaId,
            final UUID eventId,
            final String accountId,
            final String amount,
            final String initiationChannel
    ) throws Exception {
        GenericRecord record = new GenericData.Record(v2Schema);
        record.put("eventId", eventId.toString());
        record.put("eventType", "BookingCreated");
        record.put("aggregateId", accountId);
        record.put("bookingId", UUID.randomUUID().toString());
        record.put("originalBookingId", null);
        record.put("bookingType", "CREDIT");
        record.put("amount", amount);
        record.put("currency", "EUR");
        record.put("reference", "v2 producer payload");
        record.put("version", 1L);
        record.put("occurredAt", Instant.now().toString());
        record.put("schemaVersion", 2);
        record.put("initiationChannel", initiationChannel);
        return writeConfluentAvro(v2Schema, schemaId, record);
    }

    private static byte[] writeConfluentAvro(
            final Schema schema,
            final int schemaId,
            final GenericRecord record
    ) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x00);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static GenericRecord decodeWithReaderSchema(
            final byte[] wireBytes,
            final Schema writerSchema,
            final Schema readerSchema
    ) throws Exception {
        byte[] avroPayload = Arrays.copyOfRange(wireBytes, 5, wireBytes.length);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroPayload, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
        return reader.read(null, decoder);
    }

    private byte[] consumeRawBytesForKey(final String key, final Duration timeout) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "schema-evo-raw-" + shortId(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
        );
        Instant deadline = Instant.now().plus(timeout);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topicProperties.ledgerEvents()));
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, byte[]> record : batch) {
                    if (key.equals(record.key())) {
                        return record.value();
                    }
                }
            }
        }
        return null;
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
            final String idempotencyKey
    ) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(TestJwtSupport.instance().writerToken());
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

    private static String loadResource(final String path) throws Exception {
        try (InputStream stream = SchemaEvolutionIT.class.getResourceAsStream(path)) {
            assertThat(stream).as("Missing test resource " + path).isNotNull();
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
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
