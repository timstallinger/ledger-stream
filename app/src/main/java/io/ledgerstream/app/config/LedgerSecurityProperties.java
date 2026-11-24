package io.ledgerstream.app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ledger.security")
public record LedgerSecurityProperties(String expectedAudience) {

    public LedgerSecurityProperties {
        if (expectedAudience == null || expectedAudience.isBlank()) {
            expectedAudience = "ledger-stream-api";
        }
    }
}
