package io.ledgerstream.app;

import io.ledgerstream.app.security.AdminTestOnlyController;
import io.ledgerstream.app.security.TestJwtSupport;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=false",
                "ledger.admin.enabled=true"
        }
)
@ActiveProfiles("admin-route-invariant-test")
@Import(AdminTestOnlyController.class)
@Testcontainers
final class AdminPathInvariantEnabledIT {

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

    @Test
    void dummyAdminRouteRequiresOperatorRoleWhenAdminIsEnabled() {
        ResponseEntity<String> unauth = restTemplate.getForEntity("/admin/test-only", String.class);
        assertThat(unauth.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        HttpHeaders viewerHeaders = new HttpHeaders();
        viewerHeaders.setBearerAuth(TestJwtSupport.instance().viewerToken());
        ResponseEntity<String> viewer = restTemplate.exchange(
                "/admin/test-only", HttpMethod.GET, new HttpEntity<>(viewerHeaders), String.class);
        assertThat(viewer.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);

        HttpHeaders operatorHeaders = new HttpHeaders();
        operatorHeaders.setBearerAuth(TestJwtSupport.instance().operatorToken());
        ResponseEntity<String> operator = restTemplate.exchange(
                "/admin/test-only", HttpMethod.GET, new HttpEntity<>(operatorHeaders), String.class);
        assertThat(operator.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(operator.getBody()).isEqualTo("admin-test-only-ok");
    }

    @Test
    void corsPreflightAllowsConfiguredOriginWithAuthAndIdempotencyKeyAndRejectsUnknownOrigin() {
        String bookingsPath = "/api/v1/accounts/" + UUID.randomUUID() + "/bookings";

        HttpHeaders allowedPreflight = new HttpHeaders();
        allowedPreflight.setOrigin("http://localhost:5173");
        allowedPreflight.setAccessControlRequestMethod(HttpMethod.POST);
        allowedPreflight.setAccessControlRequestHeaders(java.util.List.of("Authorization", "Idempotency-Key", "Content-Type"));

        ResponseEntity<String> allowedResp = restTemplate.exchange(
                bookingsPath, HttpMethod.OPTIONS, new HttpEntity<>(allowedPreflight), String.class);
        assertThat(allowedResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(allowedResp.getHeaders().getAccessControlAllowOrigin()).isEqualTo("http://localhost:5173");
        assertThat(allowedResp.getHeaders().getAccessControlAllowOrigin()).isNotEqualTo("*");
        assertThat(allowedResp.getHeaders().getAccessControlAllowCredentials()).isTrue();
        assertThat(allowedResp.getHeaders().getAccessControlAllowHeaders())
                .contains("Authorization", "Idempotency-Key", "Content-Type");
        assertThat(allowedResp.getHeaders().getAccessControlAllowMethods()).contains(HttpMethod.POST);

        HttpHeaders evilPreflight = new HttpHeaders();
        evilPreflight.setOrigin("https://evil.example");
        evilPreflight.setAccessControlRequestMethod(HttpMethod.POST);
        evilPreflight.setAccessControlRequestHeaders(java.util.List.of("Authorization", "Idempotency-Key"));

        ResponseEntity<String> evilResp = restTemplate.exchange(
                bookingsPath, HttpMethod.OPTIONS, new HttpEntity<>(evilPreflight), String.class);
        assertThat(evilResp.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(evilResp.getHeaders().getAccessControlAllowOrigin()).isNull();
    }
}
