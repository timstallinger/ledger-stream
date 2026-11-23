package io.ledgerstream.app;

import io.ledgerstream.application.port.out.TransactionRunner;
import io.ledgerstream.domain.DomainException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
final class ApplicationSmokeIT {

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
    private JdbcClient jdbcClient;

    @Autowired
    private TransactionRunner transactionRunner;

    @Test
    void healthAndKubernetesProbeEndpointsReportUp() {
        ResponseEntity<String> health = restTemplate.getForEntity("/actuator/health", String.class);
        assertThat(health.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(health.getBody()).contains("\"status\":\"UP\"");

        ResponseEntity<String> liveness = restTemplate.getForEntity("/actuator/health/liveness", String.class);
        assertThat(liveness.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(liveness.getBody()).contains("\"status\":\"UP\"");

        ResponseEntity<String> readiness = restTemplate.getForEntity("/actuator/health/readiness", String.class);
        assertThat(readiness.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(readiness.getBody()).contains("\"status\":\"UP\"");
    }

    @Test
    void flywayAppliesBaselineSchemaWithRequiredTablesAndConstraints() {
        List<String> tables = jdbcClient.sql("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        ORDER BY table_name
                        """)
                .query(String.class)
                .list();

        assertThat(tables).contains("accounts", "bookings", "flyway_schema_history");

        Integer uniqueConstraintCount = jdbcClient.sql("""
                        SELECT COUNT(*)
                        FROM pg_indexes
                        WHERE schemaname = 'public'
                          AND tablename = 'bookings'
                          AND indexdef LIKE '%UNIQUE%'
                          AND indexdef LIKE '%(account_id, idempotency_key)%'
                        """)
                .query(Integer.class)
                .single();

        assertThat(uniqueConstraintCount).isEqualTo(1);
    }

    @Test
    void transactionRunnerRollsBackOnUncheckedDomainException() {
        UUID accountId = UUID.randomUUID();
        String accountNumber = "DE-" + accountId.toString().substring(0, 8);

        assertThatThrownBy(() -> transactionRunner.runInTransaction(() -> {
            jdbcClient.sql("""
                            INSERT INTO accounts (id, account_number, balance, currency, version)
                            VALUES (:id, :accountNumber, 0, 'EUR', 0)
                            """)
                    .param("id", accountId)
                    .param("accountNumber", accountNumber)
                    .update();
            throw new DomainException("SIMULATED_DOMAIN_ERROR", "Forced rollback check");
        }))
                .isInstanceOf(DomainException.class)
                .hasMessage("Forced rollback check");

        Integer count = jdbcClient.sql("SELECT COUNT(*) FROM accounts WHERE id = :id")
                .param("id", accountId)
                .query(Integer.class)
                .single();

        assertThat(count).isZero();
    }
}
