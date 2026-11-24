package io.ledgerstream.app.security;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Test-profile-only dummy route under /admin/** WITHOUT @ConditionalOnProperty(ledger.admin.enabled).
 * Proves the P4 invariant: even if a controller exists under /admin/**, SecurityConfiguration
 * enforces 404-before-401 when ledger.admin.enabled=false and requires ROLE_operator when enabled=true.
 */
@Profile("admin-route-invariant-test")
@RestController
@RequestMapping("/admin/test-only")
public final class AdminTestOnlyController {

    @GetMapping
    public ResponseEntity<String> testOnly() {
        return ResponseEntity.ok("admin-test-only-ok");
    }
}
