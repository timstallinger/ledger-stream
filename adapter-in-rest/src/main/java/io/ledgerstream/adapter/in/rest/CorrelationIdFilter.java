package io.ledgerstream.adapter.in.rest;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Validates incoming X-Correlation-Id strictly as untrusted client input against ^[A-Za-z0-9._-]{8,64}$.
 * Invalid values are NEVER truncated/reused; instead a fresh UUID is generated and correlationIdRejected=true is logged.
 */
public final class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String HEADER_CORRELATION_ID = "X-Correlation-Id";
    public static final String MDC_CORRELATION_ID = "correlationId";
    public static final Pattern VALID_PATTERN = Pattern.compile("^[A-Za-z0-9._-]{8,64}$");

    private static final Logger LOG = LoggerFactory.getLogger(CorrelationIdFilter.class);

    @Override
    protected void doFilterInternal(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final FilterChain filterChain
    ) throws ServletException, IOException {
        String incoming = request.getHeader(HEADER_CORRELATION_ID);
        String correlationId;
        boolean rejected = false;

        if (incoming == null || incoming.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        } else if (VALID_PATTERN.matcher(incoming).matches()) {
            correlationId = incoming;
        } else {
            correlationId = UUID.randomUUID().toString();
            rejected = true;
        }

        MDC.put(MDC_CORRELATION_ID, correlationId);
        response.setHeader(HEADER_CORRELATION_ID, correlationId);
        if (rejected) {
            LOG.warn(
                    "Invalid client X-Correlation-Id header rejected; generated fresh correlationId: correlationIdRejected=true method={} path={}",
                    request.getMethod(),
                    request.getRequestURI()
            );
        }
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.clear();
        }
    }
}
