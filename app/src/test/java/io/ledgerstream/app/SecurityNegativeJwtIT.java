package io.ledgerstream.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.app.security.TestJwtSupport;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies every individual JWT validation failure condition against the real production
 * SecurityFilterChain and NimbusJwtDecoder. Because expired / wrong-iss / wrong-aud tokens
 * are signed with the trusted RSA key, each test proves that its exact claim validator rejects
 * the token with HTTP 401 (not a signature failure).
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=false",
                "ledger.admin.enabled=true"
        }
)
@Testcontainers
final class SecurityNegativeJwtIT {

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

    @Test
    void expiredTokenReturns401WithRfc7807Body() throws Exception {
        String expired = TestJwtSupport.instance().expiredToken("ops", List.of("operator"));
        ResponseEntity<String> response = getWithBearer("/admin/dlq", expired);
        assertUnauthorizedRfc7807(response);
    }

    @Test
    void wrongIssuerTokenReturns401() throws Exception {
        String wrongIss = TestJwtSupport.instance().wrongIssuerToken("ops", List.of("operator"));
        ResponseEntity<String> response = getWithBearer("/admin/dlq", wrongIss);
        assertUnauthorizedRfc7807(response);
    }

    @Test
    void wrongAudienceTokenReturns401() throws Exception {
        String wrongAud = TestJwtSupport.instance().wrongAudienceToken("ops", List.of("operator"));
        ResponseEntity<String> response = getWithBearer("/admin/dlq", wrongAud);
        assertUnauthorizedRfc7807(response);
    }

    @Test
    void algNoneTokenReturns401() throws Exception {
        String algNone = TestJwtSupport.instance().algNoneToken(
                TestJwtSupport.instance().issuerUri(), "ops", List.of("operator"));
        ResponseEntity<String> response = getWithBearer("/admin/dlq", algNone);
        assertUnauthorizedRfc7807(response);
    }

    @Test
    void foreignKeySignedTokenReturns401() throws Exception {
        String foreignKey = TestJwtSupport.instance().foreignKeyToken(
                TestJwtSupport.instance().issuerUri(), "ops", List.of("operator"));
        ResponseEntity<String> response = getWithBearer("/admin/dlq", foreignKey);
        assertUnauthorizedRfc7807(response);
    }

    @Test
    void corsExposesIdempotencyReplayedAndCorrelationHeadersForAllowedOrigin() {
        HttpHeaders headers = new HttpHeaders();
        headers.setOrigin("http://localhost:5173");
        headers.setBearerAuth(TestJwtSupport.instance().viewerToken());

        ResponseEntity<String> response = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getHeaders().getAccessControlAllowOrigin()).isEqualTo("http://localhost:5173");
        assertThat(response.getHeaders().getAccessControlExposeHeaders())
                .contains("Idempotency-Replayed", "X-Correlation-Id", "Retry-After");
    }

    private ResponseEntity<String> getWithBearer(final String path, final String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        return restTemplate.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), String.class);
    }

    private void assertUnauthorizedRfc7807(final ResponseEntity<String> response) throws Exception {
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE)).contains("Bearer");
        JsonNode body = objectMapper.readTree(response.getBody());
        assertThat(body.get("status").asInt()).isEqualTo(401);
        assertThat(body.get("errorCode").asText()).isEqualTo("UNAUTHORIZED");
        assertThat(body.get("type").asText()).isEqualTo("urn:ledger-stream:error:unauthorized");
    }
}
