package io.ledgerstream.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Thread-safe OIDC access token provider with automatic proactive refresh before expiry.
 */
public final class OidcTokenProvider {

    private static final long REFRESH_MARGIN_SECONDS = 45L;

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final String username;
    private final String password;

    private volatile String cachedToken;
    private volatile Instant expiresAt = Instant.EPOCH;

    public OidcTokenProvider(
            final HttpClient httpClient,
            final ObjectMapper objectMapper,
            final String tokenUrl,
            final String clientId,
            final String clientSecret,
            final String username,
            final String password
    ) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.tokenUrl = Objects.requireNonNull(tokenUrl, "tokenUrl must not be null");
        this.clientId = Objects.requireNonNull(clientId, "clientId must not be null");
        this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret must not be null");
        this.username = username == null ? "" : username;
        this.password = password == null ? "" : password;
    }

    public String getAccessToken() {
        String token = cachedToken;
        if (token != null && Instant.now().isBefore(expiresAt.minusSeconds(REFRESH_MARGIN_SECONDS))) {
            return token;
        }
        return refreshToken();
    }

    private synchronized String refreshToken() {
        if (cachedToken != null && Instant.now().isBefore(expiresAt.minusSeconds(REFRESH_MARGIN_SECONDS))) {
            return cachedToken;
        }
        try {
            String formBody;
            if (!username.isBlank() && !password.isBlank()) {
                formBody = "grant_type=password"
                        + "&client_id=" + encode(clientId)
                        + "&client_secret=" + encode(clientSecret)
                        + "&username=" + encode(username)
                        + "&password=" + encode(password);
            } else {
                formBody = "grant_type=client_credentials"
                        + "&client_id=" + encode(clientId)
                        + "&client_secret=" + encode(clientSecret);
            }

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(tokenUrl))
                    .timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(formBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "Failed to obtain OIDC access token from " + tokenUrl
                                + " (HTTP " + response.statusCode() + "): " + response.body()
                );
            }
            JsonNode json = objectMapper.readTree(response.body());
            String accessToken = json.get("access_token").asText();
            long expiresIn = json.has("expires_in") ? json.get("expires_in").asLong(300L) : 300L;
            this.cachedToken = accessToken;
            this.expiresAt = Instant.now().plusSeconds(expiresIn);
            return accessToken;
        } catch (Exception exception) {
            throw new IllegalStateException("OIDC token acquisition failed", exception);
        }
    }

    private static String encode(final String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
