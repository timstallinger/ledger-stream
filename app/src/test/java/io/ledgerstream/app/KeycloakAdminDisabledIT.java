package io.ledgerstream.app;

import io.ledgerstream.app.security.KeycloakTestEnvironment;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the 404-before-401 guarantee when ledger.admin.enabled=false against REAL Keycloak 26 tokens:
 * unauthenticated, viewer, and operator all receive 404 Not Found on /admin/dlq.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=false",
                "ledger.admin.enabled=false"
        }
)
final class KeycloakAdminDisabledIT {

    @DynamicPropertySource
    static void registerKeycloakProperties(final DynamicPropertyRegistry registry) {
        KeycloakTestEnvironment.instance().registerProperties(registry);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void adminDisabledMatrixReturns404ForUnauthenticatedViewerAndOperatorWithRealKeycloakTokens() {
        KeycloakTestEnvironment env = KeycloakTestEnvironment.instance();

        ResponseEntity<String> unauth = restTemplate.getForEntity("/admin/dlq", String.class);
        ResponseEntity<String> viewer = getWithBearer("/admin/dlq", env.fetchUserToken(KeycloakTestEnvironment.USER_VIEWER));
        ResponseEntity<String> operator = getWithBearer("/admin/dlq", env.fetchUserToken(KeycloakTestEnvironment.USER_OPERATOR));

        System.out.println("ADMIN_DISABLED_MATRIX: unauth=" + unauth.getStatusCode().value()
                + " viewer=" + viewer.getStatusCode().value()
                + " operator=" + operator.getStatusCode().value());

        assertThat(unauth.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(viewer.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(operator.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private ResponseEntity<String> getWithBearer(final String path, final String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        return restTemplate.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), String.class);
    }
}
