package io.ledgerstream.app;

import io.ledgerstream.adapter.in.rest.DlqAdminController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void t6AdminDlqEndpointsReturn404WhenAdminIsDisabledByDefault() {
        assertThat(applicationContext.getBeansOfType(DlqAdminController.class)).isEmpty();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<String> replayResponse = restTemplate.postForEntity(
                "/admin/dlq/" + UUID.randomUUID() + "/replay",
                new HttpEntity<>("{}", headers),
                String.class
        );
        assertThat(replayResponse.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        ResponseEntity<String> listResponse = restTemplate.getForEntity("/admin/dlq", String.class);
        assertThat(listResponse.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
