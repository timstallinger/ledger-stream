package io.ledgerstream.app;

import io.ledgerstream.adapter.in.rest.DlqAdminController;
import io.ledgerstream.app.security.TestJwtSupport;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
                "ledger.admin.enabled=false"
        }
)
@Testcontainers
final class AdminEndpointDisabledIT {

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
    private ApplicationContext applicationContext;

    @Test
    void adminDisabledReturns404Before401ForUnauthenticatedViewerAndOperator() {
        assertThat(applicationContext.getBeansOfType(DlqAdminController.class)).isEmpty();

        // Unauthenticated -> 404 (NEVER 401!)
        assertThat(restTemplate.getForEntity("/admin/dlq", String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        HttpHeaders unauthJson = new HttpHeaders();
        unauthJson.setContentType(MediaType.APPLICATION_JSON);
        assertThat(restTemplate.postForEntity(
                "/admin/dlq/" + UUID.randomUUID() + "/replay",
                new HttpEntity<>("{}", unauthJson),
                String.class
        ).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // Viewer token -> 404 (NEVER 403!)
        HttpHeaders viewerHeaders = new HttpHeaders();
        viewerHeaders.setBearerAuth(TestJwtSupport.instance().viewerToken());
        assertThat(restTemplate.exchange("/admin/dlq", HttpMethod.GET, new HttpEntity<>(viewerHeaders), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // Operator token -> 404
        HttpHeaders operatorHeaders = new HttpHeaders();
        operatorHeaders.setBearerAuth(TestJwtSupport.instance().operatorToken());
        assertThat(restTemplate.exchange("/admin/dlq", HttpMethod.GET, new HttpEntity<>(operatorHeaders), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
