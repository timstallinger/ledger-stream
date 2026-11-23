package io.ledgerstream.adapter.in.rest;

import io.ledgerstream.domain.DomainException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;

@RestControllerAdvice
public final class ProblemDetailExceptionHandler {

    private static final String ERROR_TYPE_PREFIX = "urn:ledger-stream:error:";

    @ExceptionHandler(DomainException.class)
    public ProblemDetail handleDomainException(
            final DomainException exception,
            final HttpServletRequest request
    ) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.UNPROCESSABLE_ENTITY,
                exception.getMessage()
        );
        problem.setType(URI.create(ERROR_TYPE_PREFIX + exception.errorCode().toLowerCase()));
        problem.setTitle("Domain rule violation");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("errorCode", exception.errorCode());
        return problem;
    }
}
