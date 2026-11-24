package io.ledgerstream.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.app.security.KeycloakTestEnvironment;
import io.ledgerstream.app.security.TestJwtSupport;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end OIDC security integration tests against a REAL Keycloak 26.2.5 container
 * backed by PostgreSQL 16 (start --optimized --import-realm, ledger.admin.enabled=true).
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "ledger.outbox.relay.enabled=false",
                "ledger.kafka.consumer.enabled=false",
                "ledger.admin.enabled=true"
        }
)
final class KeycloakSecurityIT {

    @DynamicPropertySource
    static void registerKeycloakProperties(final DynamicPropertyRegistry registry) {
        KeycloakTestEnvironment.instance().registerProperties(registry);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JwtDecoder jwtDecoder;

    @Autowired
    private JwtAuthenticationConverter jwtAuthenticationConverter;

    @Test
    void realKeycloakTokenMapsRealmRolesExplicitlyToRoleAuthorities() {
        KeycloakTestEnvironment env = KeycloakTestEnvironment.instance();
        String operatorRawToken = env.fetchUserToken(KeycloakTestEnvironment.USER_OPERATOR);

        Jwt decodedJwt = jwtDecoder.decode(operatorRawToken);
        AbstractAuthenticationToken authentication = jwtAuthenticationConverter.convert(decodedJwt);
        assertThat(authentication).isNotNull();
        List<String> authorities = authentication.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .toList();

        System.out.println("REAL_KEYCLOAK_ROLE_MAPPING: principal=" + authentication.getName()
                + " aud=" + decodedJwt.getAudience()
                + " authorities=" + authorities);

        assertThat(authentication.getName()).isEqualTo(KeycloakTestEnvironment.USER_OPERATOR);
        assertThat(decodedJwt.getAudience()).contains("ledger-stream-api");
        assertThat(authorities).contains("ROLE_viewer", "ROLE_writer", "ROLE_operator");
    }

    @Test
    void adminEnabledMatrixReturns401WithoutToken403ForViewerAnd200ForOperator() throws Exception {
        KeycloakTestEnvironment env = KeycloakTestEnvironment.instance();

        // 1) No token -> 401 RFC 7807
        ResponseEntity<String> unauth = restTemplate.getForEntity("/admin/dlq", String.class);
        System.out.println("RFC7807_401_BODY=" + unauth.getBody());
        assertThat(unauth.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        JsonNode body401 = objectMapper.readTree(unauth.getBody());
        assertThat(body401.get("status").asInt()).isEqualTo(401);
        assertThat(body401.get("errorCode").asText()).isEqualTo("UNAUTHORIZED");

        // 2) Real Keycloak viewer token -> 403 RFC 7807
        ResponseEntity<String> viewer = getWithBearer("/admin/dlq", env.fetchUserToken(KeycloakTestEnvironment.USER_VIEWER));
        System.out.println("RFC7807_403_BODY=" + viewer.getBody());
        assertThat(viewer.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        JsonNode body403 = objectMapper.readTree(viewer.getBody());
        assertThat(body403.get("status").asInt()).isEqualTo(403);
        assertThat(body403.get("errorCode").asText()).isEqualTo("FORBIDDEN");

        // 3) Real Keycloak operator token -> 200
        ResponseEntity<String> operator = getWithBearer("/admin/dlq", env.fetchUserToken(KeycloakTestEnvironment.USER_OPERATOR));
        assertThat(operator.getStatusCode()).isEqualTo(HttpStatus.OK);

        System.out.println("ADMIN_ENABLED_MATRIX: unauth=" + unauth.getStatusCode().value()
                + " viewer=" + viewer.getStatusCode().value()
                + " operator=" + operator.getStatusCode().value());
    }

    @Test
    void roleBasedAccessControlEnforcesViewerWriterNoRoleAndOperatorBoundaries() {
        KeycloakTestEnvironment env = KeycloakTestEnvironment.instance();
        String noRoleToken = env.fetchUserToken(KeycloakTestEnvironment.USER_NO_ROLE);
        String viewerToken = env.fetchUserToken(KeycloakTestEnvironment.USER_VIEWER);
        String writerToken = env.fetchUserToken(KeycloakTestEnvironment.USER_WRITER);

        // Valid real Keycloak token WITHOUT any realm role -> 403 on /admin/dlq and /api/v1/accounts
        assertThat(getWithBearer("/admin/dlq", noRoleToken).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(getWithBearer("/api/v1/accounts", noRoleToken).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);

        // Viewer -> 200 on GET /api/v1/accounts, 403 on POST /api/v1/accounts
        assertThat(getWithBearer("/api/v1/accounts", viewerToken).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(postAccount(viewerToken, "DE-VIEW-" + shortId()).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);

        // Writer -> 201 on POST /api/v1/accounts, 403 on /admin/dlq
        assertThat(postAccount(writerToken, "DE-WRT-" + shortId()).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(getWithBearer("/admin/dlq", writerToken).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void realKeycloakWrongAudienceAlgNoneAndForeignKeyReturn401AndEnvClientSecretWorks() {
        KeycloakTestEnvironment env = KeycloakTestEnvironment.instance();

        // Proves realm-export.json $(env.LEDGER_CLI_CLIENT_SECRET) placeholder was substituted at runtime
        String cliServiceToken = env.fetchCliServiceAccountToken();
        assertThat(cliServiceToken).isNotBlank();

        // Real Keycloak token issued for a client WITHOUT ledger-stream-api audience -> 401
        String wrongAudToken = env.fetchWrongAudienceToken(KeycloakTestEnvironment.USER_OPERATOR);
        assertThat(getWithBearer("/admin/dlq", wrongAudToken).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        // Unsigned alg=none token -> 401
        String algNone = TestJwtSupport.instance().algNoneToken(env.issuerUri(), "kc-operator", List.of("operator"));
        assertThat(getWithBearer("/admin/dlq", algNone).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        // Token signed by foreign RSA key -> 401
        String foreignKey = TestJwtSupport.instance().foreignKeyToken(env.issuerUri(), "kc-operator", List.of("operator"));
        assertThat(getWithBearer("/admin/dlq", foreignKey).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    private ResponseEntity<String> getWithBearer(final String path, final String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        return restTemplate.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), String.class);
    }

    private ResponseEntity<String> postAccount(final String token, final String accountNumber) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(token);
        return restTemplate.postForEntity(
                "/api/v1/accounts",
                new HttpEntity<>("{\"accountNumber\":\"" + accountNumber + "\",\"currency\":\"EUR\"}", headers),
                String.class
        );
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
