package io.ledgerstream.app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "ledger.security")
public record LedgerSecurityProperties(
        String expectedAudience,
        List<String> corsAllowedOrigins
) {

    private static final List<String> DEFAULT_CORS_ORIGINS = List.of(
            "http://localhost:5173",
            "http://localhost:3000"
    );

    public LedgerSecurityProperties {
        if (expectedAudience == null || expectedAudience.isBlank()) {
            expectedAudience = "ledger-stream-api";
        }
        if (corsAllowedOrigins == null || corsAllowedOrigins.isEmpty()) {
            corsAllowedOrigins = DEFAULT_CORS_ORIGINS;
        } else {
            for (String origin : corsAllowedOrigins) {
                if ("*".equals(origin.trim())) {
                    throw new IllegalArgumentException(
                            "Wildcard '*' is forbidden in ledger.security.cors-allowed-origins when credentials are allowed"
                    );
                }
            }
            corsAllowedOrigins = List.copyOf(corsAllowedOrigins);
        }
    }
}
