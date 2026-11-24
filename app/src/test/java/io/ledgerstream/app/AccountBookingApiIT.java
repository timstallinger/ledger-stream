package io.ledgerstream.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.app.security.TestJwtSupport;
import io.ledgerstream.application.port.out.AccountRepository;
import io.ledgerstream.domain.exception.InsufficientBalanceException;
import io.ledgerstream.domain.model.Account;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.CurrencyCode;
import io.ledgerstream.domain.model.Money;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=false"
        }
)
@Testcontainers
final class AccountBookingApiIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    @DynamicPropertySource
    static void securityProperties(final DynamicPropertyRegistry registry) {
        TestJwtSupport.registerProperties(registry);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private AccountRepository accountRepository;

    @Test
    void createsAccountBooksCreditAndDebitAndListsHistory() throws Exception {
        String accountNumber = "DE-" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = createAccount(accountNumber, "EUR");

        ResponseEntity<String> creditResponse = postBooking(
                accountId,
                "CREDIT",
                "150.50",
                "EUR",
                "Initial funding",
                "idem-credit-" + accountNumber
        );
        assertThat(creditResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<String> debitResponse = postBooking(
                accountId,
                "DEBIT",
                "50.25",
                "EUR",
                "Card payment",
                "idem-debit-" + accountNumber
        );
        assertThat(debitResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<String> accountGet = getAuthorized("/api/v1/accounts/" + accountId);
        assertThat(accountGet.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode accountJson = objectMapper.readTree(accountGet.getBody());
        assertThat(accountJson.get("balance").isTextual()).isTrue();
        assertThat(new BigDecimal(accountJson.get("balance").asText())).isEqualByComparingTo("100.25");
        assertThat(accountJson.get("version").asLong()).isEqualTo(2L);

        ResponseEntity<String> bookingsGet = getAuthorized("/api/v1/accounts/" + accountId + "/bookings");
        assertThat(bookingsGet.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode bookingsJson = objectMapper.readTree(bookingsGet.getBody());
        assertThat(bookingsJson.size()).isEqualTo(2);
        assertThat(bookingsJson.get(0).get("amount").isTextual()).isTrue();
    }

    @Test
    void rejectsNegativeBalanceWithRfc7807422AndDoesNotCacheRejection() throws Exception {
        String accountNumber = "DE-" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = createAccount(accountNumber, "EUR");
        String sharedKey = "idem-overdraft-" + accountNumber;

        ResponseEntity<String> rejected = postBooking(
                accountId,
                "DEBIT",
                "10.00",
                "EUR",
                "Overdraft attempt",
                sharedKey
        );

        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        JsonNode problem = objectMapper.readTree(rejected.getBody());
        assertThat(problem.get("status").asInt()).isEqualTo(422);
        assertThat(problem.get("errorCode").asText()).isEqualTo("INSUFFICIENT_BALANCE");
        assertThat(problem.get("type").asText()).contains("insufficient_balance");

        Integer cachedRows = jdbcClient.sql(
                        "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = :id AND idempotency_key = :key")
                .param("id", UUID.fromString(accountId))
                .param("key", sharedKey)
                .query(Integer.class)
                .single();
        assertThat(cachedRows).isZero();

        assertThat(postBooking(accountId, "CREDIT", "25.00", "EUR", "Fund", "idem-fund-" + accountNumber)
                .getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<String> succeededRetry = postBooking(
                accountId,
                "DEBIT",
                "10.00",
                "EUR",
                "Overdraft attempt",
                sharedKey
        );
        assertThat(succeededRetry.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void databaseCheckConstraintOnNegativeBalanceTranslatesToInsufficientBalanceNot500() throws Exception {
        String accountNumber = "DE-" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = createAccount(accountNumber, "EUR");
        Account existing = accountRepository.findById(AccountId.of(UUID.fromString(accountId))).orElseThrow();

        Account illegalNegative = new Account(
                existing.id(),
                existing.accountNumber(),
                Money.of(new BigDecimal("-0.01"), CurrencyCode.of("EUR")),
                existing.version() + 1,
                existing.createdAt(),
                Instant.now()
        );

        assertThatThrownBy(() -> accountRepository.updateWithOptimisticLock(illegalNegative, existing.version()))
                .isInstanceOf(InsufficientBalanceException.class);
    }

    @Test
    void duplicateIdempotencyKeyReturnsByteIdenticalResponseWithNoSecondEffect() throws Exception {
        String accountNumber = "DE-" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = createAccount(accountNumber, "EUR");
        String idempotencyKey = "idem-exact-" + accountNumber;

        ResponseEntity<String> first = postBooking(
                accountId,
                "CREDIT",
                "77.77",
                "EUR",
                "Exact replay test",
                idempotencyKey
        );
        ResponseEntity<String> second = postBooking(
                accountId,
                "CREDIT",
                "77.77",
                "EUR",
                "Exact replay test",
                idempotencyKey
        );

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(first.getStatusCode());
        assertThat(second.getBody()).isEqualTo(first.getBody());
        assertThat(second.getHeaders().getFirst("Idempotency-Replayed")).isEqualTo("true");

        Integer bookingCount = jdbcClient.sql("SELECT COUNT(*) FROM bookings WHERE account_id = :id")
                .param("id", UUID.fromString(accountId))
                .query(Integer.class)
                .single();
        assertThat(bookingCount).isEqualTo(1);
        System.out.println("DOUBLE_CLICK_PROTOCOL: key=" + idempotencyKey
                + " req1Status=" + first.getStatusCode().value()
                + " req2Status=" + second.getStatusCode().value()
                + " req2Replayed=" + second.getHeaders().getFirst("Idempotency-Replayed")
                + " dbBookingCount=" + bookingCount
                + " bodiesIdentical=" + first.getBody().equals(second.getBody())
                + " body=" + first.getBody());
    }

    @Test
    void concurrentIdenticalIdempotencyRequestsReturnByteIdenticalWinnerResponseWithout409() throws Exception {
        String accountNumber = "DE-" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = createAccount(accountNumber, "EUR");
        String sharedKey = "idem-concurrent-" + accountNumber;
        int threads = 10;

        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ResponseEntity<String>>> futures = new ArrayList<>();

        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return postBooking(accountId, "CREDIT", "10.00", "EUR", "Concurrent", sharedKey);
                }));
            }
            ready.await();
            start.countDown();

            List<ResponseEntity<String>> responses = new ArrayList<>();
            for (Future<ResponseEntity<String>> future : futures) {
                responses.add(future.get());
            }

            String expectedBody = responses.getFirst().getBody();
            assertThat(expectedBody).isNotBlank();
            for (ResponseEntity<String> response : responses) {
                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
                assertThat(response.getBody()).isEqualTo(expectedBody);
            }
        }

        Integer bookingCount = jdbcClient.sql("SELECT COUNT(*) FROM bookings WHERE account_id = :id")
                .param("id", UUID.fromString(accountId))
                .query(Integer.class)
                .single();
        assertThat(bookingCount).isEqualTo(1);

        ResponseEntity<String> accountGet = getAuthorized("/api/v1/accounts/" + accountId);
        assertThat(new BigDecimal(objectMapper.readTree(accountGet.getBody()).get("balance").asText()))
                .isEqualByComparingTo("10.00");
    }

    @Test
    void cancellationCreatesCounterBookingAndNeverDeletesOriginal() throws Exception {
        String accountNumber = "DE-" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = createAccount(accountNumber, "EUR");

        ResponseEntity<String> created = postBooking(
                accountId,
                "CREDIT",
                "90.00",
                "EUR",
                "Order 123",
                "idem-order-" + accountNumber
        );
        String bookingId = objectMapper.readTree(created.getBody()).get("id").asText();

        HttpHeaders headers = jsonAuthHeaders();
        headers.set("Idempotency-Key", "idem-cancel-" + accountNumber);
        ResponseEntity<String> cancelled = restTemplate.postForEntity(
                "/api/v1/accounts/" + accountId + "/bookings/" + bookingId + "/cancel",
                new HttpEntity<>("{\"reference\":\"Customer cancellation\"}", headers),
                String.class
        );

        assertThat(cancelled.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        JsonNode counterJson = objectMapper.readTree(cancelled.getBody());
        assertThat(counterJson.get("type").asText()).isEqualTo("DEBIT");
        assertThat(counterJson.get("originalBookingId").asText()).isEqualTo(bookingId);

        ResponseEntity<String> accountGet = getAuthorized("/api/v1/accounts/" + accountId);
        assertThat(new BigDecimal(objectMapper.readTree(accountGet.getBody()).get("balance").asText()))
                .isEqualByComparingTo("0.00");

        Integer totalRows = jdbcClient.sql("SELECT COUNT(*) FROM bookings WHERE account_id = :id")
                .param("id", UUID.fromString(accountId))
                .query(Integer.class)
                .single();
        assertThat(totalRows).isEqualTo(2);
    }

    @Test
    void returnsRfc7807BadRequestOnValidationFailureAndNotFoundForMissingAccount() throws Exception {
        String accountNumber = "DE-" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = createAccount(accountNumber, "EUR");

        HttpHeaders noIdempotencyHeader = jsonAuthHeaders();
        ResponseEntity<String> missingHeader = restTemplate.postForEntity(
                "/api/v1/accounts/" + accountId + "/bookings",
                new HttpEntity<>(
                        "{\"type\":\"CREDIT\",\"amount\":10.00,\"currency\":\"EUR\",\"reference\":\"Test\"}",
                        noIdempotencyHeader
                ),
                String.class
        );
        assertThat(missingHeader.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<String> tooManyDecimals = postBooking(
                accountId,
                "CREDIT",
                "10.123",
                "EUR",
                "Bad scale",
                "idem-bad-scale"
        );
        assertThat(tooManyDecimals.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<String> tooLongKey = postBooking(
                accountId,
                "CREDIT",
                "10.00",
                "EUR",
                "Long key",
                "k".repeat(129)
        );
        assertThat(tooLongKey.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<String> notFound = getAuthorized("/api/v1/accounts/" + UUID.randomUUID());
        assertThat(notFound.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(objectMapper.readTree(notFound.getBody()).get("errorCode").asText())
                .isEqualTo("ACCOUNT_NOT_FOUND");
    }

    private ResponseEntity<String> getAuthorized(final String path) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(TestJwtSupport.instance().viewerToken());
        return restTemplate.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), String.class);
    }

    private static HttpHeaders jsonAuthHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(TestJwtSupport.instance().writerToken());
        return headers;
    }

    private String createAccount(final String accountNumber, final String currency) throws Exception {
        HttpHeaders headers = jsonAuthHeaders();
        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/accounts",
                new HttpEntity<>(
                        "{\"accountNumber\":\"" + accountNumber + "\",\"currency\":\"" + currency + "\"}",
                        headers
                ),
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
        HttpHeaders headers = jsonAuthHeaders();
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
