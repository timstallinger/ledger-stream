package io.ledgerstream.app;

import io.ledgerstream.app.security.AdminTestOnlyController;
import io.ledgerstream.app.security.TestJwtSupport;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
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

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=false",
                "ledger.admin.enabled=false"
        }
)
@ActiveProfiles("admin-route-invariant-test")
@Import(AdminTestOnlyController.class)
@Testcontainers
final class AdminPathInvariantDisabledIT {

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
    void dummyAdminRouteReturns404EvenWhenControllerBeanExistsAndAdminIsDisabled() {
        assertThat(applicationContext.getBeansOfType(AdminTestOnlyController.class)).hasSize(1);

        ResponseEntity<String> unauth = restTemplate.getForEntity("/admin/test-only", String.class);
        assertThat(unauth.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        HttpHeaders operatorHeaders = new HttpHeaders();
        operatorHeaders.setBearerAuth(TestJwtSupport.instance().operatorToken());
        ResponseEntity<String> operator = restTemplate.exchange(
                "/admin/test-only", HttpMethod.GET, new HttpEntity<>(operatorHeaders), String.class);
        assertThat(operator.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
