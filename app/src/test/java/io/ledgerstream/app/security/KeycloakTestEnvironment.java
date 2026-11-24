package io.ledgerstream.app.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Shared real Keycloak 26.2.5 + PostgreSQL 16 + Kafka 3.9.1 Testcontainers environment.
 * Runs Keycloak with start --optimized --import-realm backed by the SAME Postgres instance
 * (separate keycloak DB), importing keycloak/realm-export.json without any hardcoded secrets.
 */
public final class KeycloakTestEnvironment {

    public static final String REALM = "ledger-stream";
    public static final String CLI_CLIENT_ID = "ledger-stream-cli";
    public static final String WRONG_AUD_CLIENT_ID = "wrong-audience-client";
    public static final String USER_OPERATOR = "kc-operator";
    public static final String USER_WRITER = "kc-writer";
    public static final String USER_VIEWER = "kc-viewer";
    public static final String USER_NO_ROLE = "kc-norole";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static volatile KeycloakTestEnvironment instance;

    private final Network network;
    private final PostgreSQLContainer<?> postgres;
    private final KafkaContainer kafka;
    private final GenericContainer<?> keycloak;
    private final String adminPassword;
    private final String cliClientSecret;
    private final String wrongAudClientSecret;
    private final String userPassword;
    private final String baseUrl;

    private KeycloakTestEnvironment() {
        this.adminPassword = "adm-" + UUID.randomUUID();
        this.cliClientSecret = "cli-" + UUID.randomUUID();
        this.wrongAudClientSecret = "aud-" + UUID.randomUUID();
        this.userPassword = "usr-" + UUID.randomUUID();

        Path projectRoot = resolveProjectRoot();
        this.network = Network.newNetwork();

        this.postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withDatabaseName("ledger")
                .withUsername("ledger")
                .withPassword("ledger_test_pw")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(projectRoot.resolve("docker/postgres/init-keycloak-db.sql")),
                        "/docker-entrypoint-initdb.d/01-init-keycloak-db.sql"
                );
        this.postgres.start();

        this.kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))
                .withNetwork(network);
        this.kafka.start();

        ImageFromDockerfile keycloakImage = new ImageFromDockerfile("ledger-stream-keycloak-it", false)
                .withFileFromPath("Dockerfile", projectRoot.resolve("keycloak/Dockerfile"))
                .withFileFromPath("entrypoint.sh", projectRoot.resolve("keycloak/entrypoint.sh"));

        this.keycloak = new GenericContainer<>(keycloakImage)
                .withNetwork(network)
                .dependsOn(postgres)
                .withExposedPorts(8080, 9000)
                .withCommand("start", "--optimized", "--import-realm")
                .withEnv("KC_DB", "postgres")
                .withEnv("KC_CACHE", "local")
                .withEnv("KC_DB_URL", "jdbc:postgresql://postgres:5432/keycloak")
                .withEnv("KC_DB_USERNAME", "ledger")
                .withEnv("KC_DB_PASSWORD", "ledger_test_pw")
                .withEnv("KC_HTTP_ENABLED", "true")
                .withEnv("KC_HOSTNAME_STRICT", "false")
                .withEnv("KC_HEALTH_ENABLED", "true")
                .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
                .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", adminPassword)
                .withEnv("LEDGER_CLI_CLIENT_SECRET", cliClientSecret)
                .withEnv("JAVA_OPTS_APPEND", "-Xms128m -Xmx256m")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(projectRoot.resolve("keycloak/realm-export.json")),
                        "/opt/keycloak/data/import-template/realm-export.json"
                )
                .waitingFor(Wait.forHttp("/health/ready")
                        .forPort(9000)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofSeconds(180)));
        this.keycloak.start();

        this.baseUrl = "http://" + keycloak.getHost() + ":" + keycloak.getMappedPort(8080);
        provisionEphemeralTestUsersAndWrongAudienceClient();
    }

    public static synchronized KeycloakTestEnvironment instance() {
        if (instance == null) {
            instance = new KeycloakTestEnvironment();
        }
        return instance;
    }

    public void registerProperties(final DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", this::issuerUri);
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", this::jwkSetUri);
    }

    public String issuerUri() {
        return baseUrl + "/realms/" + REALM;
    }

    public String jwkSetUri() {
        return issuerUri() + "/protocol/openid-connect/certs";
    }

    public String fetchUserToken(final String username) {
        return fetchPasswordToken(CLI_CLIENT_ID, cliClientSecret, username, userPassword);
    }

    public String fetchWrongAudienceToken(final String username) {
        return fetchPasswordToken(WRONG_AUD_CLIENT_ID, wrongAudClientSecret, username, userPassword);
    }

    public String fetchCliServiceAccountToken() {
        try {
            String form = "grant_type=client_credentials"
                    + "&client_id=" + encode(CLI_CLIENT_ID)
                    + "&client_secret=" + encode(cliClientSecret);
            HttpResponse<String> response = postForm(issuerUri() + "/protocol/openid-connect/token", form);
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Client credentials token failed: "
                        + response.statusCode() + " " + response.body());
            }
            return MAPPER.readTree(response.body()).get("access_token").asText();
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to fetch service account token", exception);
        }
    }

    private String fetchPasswordToken(
            final String clientId,
            final String clientSecret,
            final String username,
            final String password
    ) {
        try {
            String form = "grant_type=password"
                    + "&client_id=" + encode(clientId)
                    + "&client_secret=" + encode(clientSecret)
                    + "&username=" + encode(username)
                    + "&password=" + encode(password);
            HttpResponse<String> response = postForm(issuerUri() + "/protocol/openid-connect/token", form);
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Password token failed for " + username + ": "
                        + response.statusCode() + " " + response.body());
            }
            return MAPPER.readTree(response.body()).get("access_token").asText();
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to fetch password token for " + username, exception);
        }
    }

    private void provisionEphemeralTestUsersAndWrongAudienceClient() {
        try {
            String adminToken = fetchAdminToken();
            createUserWithRoles(adminToken, USER_OPERATOR, List.of("viewer", "writer", "operator"));
            createUserWithRoles(adminToken, USER_WRITER, List.of("viewer", "writer"));
            createUserWithRoles(adminToken, USER_VIEWER, List.of("viewer"));
            createUserWithRoles(adminToken, USER_NO_ROLE, List.of());

            // Create a confidential client WITHOUT the ledger-stream-api audience mapper
            Map<String, Object> wrongAudClient = Map.of(
                    "clientId", WRONG_AUD_CLIENT_ID,
                    "enabled", true,
                    "protocol", "openid-connect",
                    "publicClient", false,
                    "secret", wrongAudClientSecret,
                    "directAccessGrantsEnabled", true,
                    "standardFlowEnabled", false
            );
            HttpResponse<String> clientResp = postJson(
                    baseUrl + "/admin/realms/" + REALM + "/clients",
                    adminToken,
                    MAPPER.writeValueAsString(wrongAudClient)
            );
            if (clientResp.statusCode() != 201 && clientResp.statusCode() != 409) {
                throw new IllegalStateException("Failed to create wrong-audience client: "
                        + clientResp.statusCode() + " " + clientResp.body());
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to provision Keycloak test users", exception);
        }
    }

    private void createUserWithRoles(
            final String adminToken,
            final String username,
            final List<String> roleNames
    ) throws Exception {
        Map<String, Object> userPayload = Map.of(
                "username", username,
                "email", username + "@ledger-stream.local",
                "firstName", username,
                "lastName", "Test",
                "enabled", true,
                "emailVerified", true,
                "credentials", List.of(Map.of(
                        "type", "password",
                        "value", userPassword,
                        "temporary", false
                ))
        );
        HttpResponse<String> createResp = postJson(
                baseUrl + "/admin/realms/" + REALM + "/users",
                adminToken,
                MAPPER.writeValueAsString(userPayload)
        );
        if (createResp.statusCode() != 201) {
            throw new IllegalStateException("Failed to create user " + username + ": "
                    + createResp.statusCode() + " " + createResp.body());
        }

        HttpResponse<String> findResp = getJson(
                baseUrl + "/admin/realms/" + REALM + "/users?exact=true&username=" + encode(username),
                adminToken
        );
        String userId = MAPPER.readTree(findResp.body()).get(0).get("id").asText();

        if (!roleNames.isEmpty()) {
            List<Map<String, Object>> roleRepresentations = roleNames.stream()
                    .map(roleName -> fetchRoleRepresentation(adminToken, roleName))
                    .toList();
            HttpResponse<String> assignResp = postJson(
                    baseUrl + "/admin/realms/" + REALM + "/users/" + userId + "/role-mappings/realm",
                    adminToken,
                    MAPPER.writeValueAsString(roleRepresentations)
            );
            if (assignResp.statusCode() != 204) {
                throw new IllegalStateException("Failed to assign roles " + roleNames + " to " + username + ": "
                        + assignResp.statusCode() + " " + assignResp.body());
            }
        }
    }

    private Map<String, Object> fetchRoleRepresentation(final String adminToken, final String roleName) {
        try {
            HttpResponse<String> resp = getJson(
                    baseUrl + "/admin/realms/" + REALM + "/roles/" + encode(roleName),
                    adminToken
            );
            if (resp.statusCode() != 200) {
                throw new IllegalStateException("Missing realm role " + roleName + ": " + resp.statusCode());
            }
            JsonNode node = MAPPER.readTree(resp.body());
            return Map.of("id", node.get("id").asText(), "name", node.get("name").asText());
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to fetch realm role " + roleName, exception);
        }
    }

    private String fetchAdminToken() throws Exception {
        String form = "grant_type=password"
                + "&client_id=admin-cli"
                + "&username=admin"
                + "&password=" + encode(adminPassword);
        HttpResponse<String> response = postForm(baseUrl + "/realms/master/protocol/openid-connect/token", form);
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Admin token failed: " + response.statusCode() + " " + response.body());
        }
        return MAPPER.readTree(response.body()).get("access_token").asText();
    }

    private static HttpResponse<String> postForm(final String url, final String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> postJson(
            final String url,
            final String bearerToken,
            final String json
    ) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> getJson(final String url, final String bearerToken) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", "Bearer " + bearerToken)
                .GET()
                .build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static String encode(final String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static Path resolveProjectRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        if (cwd.resolve("keycloak/Dockerfile").toFile().exists()) {
            return cwd;
        }
        Path parent = cwd.getParent();
        if (parent != null && parent.resolve("keycloak/Dockerfile").toFile().exists()) {
            return parent;
        }
        throw new IllegalStateException("Cannot locate keycloak/Dockerfile from " + cwd);
    }
}
