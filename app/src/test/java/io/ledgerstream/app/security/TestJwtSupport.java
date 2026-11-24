package io.ledgerstream.app.security;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Lightweight test-only RSA-2048 JWKS endpoint and JWT minter.
 * Allows integration tests to exercise the real production Spring Security filter chain
 * and NimbusJwtDecoder without any production test switches.
 */
public final class TestJwtSupport {

    public static final String DEFAULT_AUDIENCE = "ledger-stream-api";
    private static final TestJwtSupport INSTANCE = new TestJwtSupport();

    private final RSAKey signingKey;
    private final RSAKey foreignKey;
    private final String issuerUri;
    private final String jwkSetUri;

    private TestJwtSupport() {
        this.signingKey = generateRsaKey("test-key-1");
        this.foreignKey = generateRsaKey("foreign-key-99");
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            String jwksJson = new JWKSet(signingKey.toPublicJWK()).toString();
            server.createContext("/realms/ledger-stream/protocol/openid-connect/certs", exchange -> {
                byte[] body = jwksJson.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            });
            server.setExecutor(null);
            server.start();
            int port = server.getAddress().getPort();
            this.issuerUri = "http://127.0.0.1:" + port + "/realms/ledger-stream";
            this.jwkSetUri = this.issuerUri + "/protocol/openid-connect/certs";
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to start local test JWKS server", exception);
        }
    }

    public static TestJwtSupport instance() {
        return INSTANCE;
    }

    public String issuerUri() {
        return issuerUri;
    }

    public String jwkSetUri() {
        return jwkSetUri;
    }

    public static void registerProperties(final org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> INSTANCE.issuerUri());
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", () -> INSTANCE.jwkSetUri());
    }

    public String operatorToken() {
        return token("test-operator", List.of("viewer", "writer", "operator"));
    }

    public String writerToken() {
        return token("test-writer", List.of("viewer", "writer"));
    }

    public String viewerToken() {
        return token("test-viewer", List.of("viewer"));
    }

    public String token(final String username, final List<String> roles) {
        return mintSignedToken(
                signingKey,
                issuerUri,
                List.of(DEFAULT_AUDIENCE),
                username,
                roles,
                Instant.now().plusSeconds(300)
        );
    }

    public String expiredToken(final String username, final List<String> roles) {
        return mintSignedToken(
                signingKey,
                issuerUri,
                List.of(DEFAULT_AUDIENCE),
                username,
                roles,
                Instant.now().minusSeconds(300)
        );
    }

    public String wrongIssuerToken(final String username, final List<String> roles) {
        return mintSignedToken(
                signingKey,
                "https://evil-issuer.example/realms/fake",
                List.of(DEFAULT_AUDIENCE),
                username,
                roles,
                Instant.now().plusSeconds(300)
        );
    }

    public String wrongAudienceToken(final String username, final List<String> roles) {
        return mintSignedToken(
                signingKey,
                issuerUri,
                List.of("some-other-api"),
                username,
                roles,
                Instant.now().plusSeconds(300)
        );
    }

    public String foreignKeyToken(final String issuer, final String username, final List<String> roles) {
        return mintSignedToken(
                foreignKey,
                issuer,
                List.of(DEFAULT_AUDIENCE),
                username,
                roles,
                Instant.now().plusSeconds(300)
        );
    }

    public String algNoneToken(final String issuer, final String username, final List<String> roles) {
        JWTClaimsSet claims = buildClaims(issuer, List.of(DEFAULT_AUDIENCE), username, roles, Instant.now().plusSeconds(300));
        return new PlainJWT(claims).serialize();
    }

    private static String mintSignedToken(
            final RSAKey key,
            final String issuer,
            final List<String> audience,
            final String username,
            final List<String> roles,
            final Instant expiresAt
    ) {
        try {
            JWTClaimsSet claims = buildClaims(issuer, audience, username, roles, expiresAt);
            JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256)
                    .keyID(key.getKeyID())
                    .build();
            SignedJWT jwt = new SignedJWT(header, claims);
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to sign test JWT", exception);
        }
    }

    private static JWTClaimsSet buildClaims(
            final String issuer,
            final List<String> audience,
            final String username,
            final List<String> roles,
            final Instant expiresAt
    ) {
        Instant issuedAt = expiresAt.minusSeconds(60);
        return new JWTClaimsSet.Builder()
                .jwtID(UUID.randomUUID().toString())
                .subject(UUID.randomUUID().toString())
                .issuer(issuer)
                .audience(audience)
                .issueTime(Date.from(issuedAt))
                .expirationTime(Date.from(expiresAt))
                .claim("preferred_username", username)
                .claim("realm_access", Map.of("roles", roles))
                .build();
    }

    private static RSAKey generateRsaKey(final String keyId) {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            KeyPair keyPair = generator.generateKeyPair();
            return new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
                    .privateKey((RSAPrivateKey) keyPair.getPrivate())
                    .keyUse(KeyUse.SIGNATURE)
                    .keyID(keyId)
                    .build();
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to generate test RSA key", exception);
        }
    }
}
