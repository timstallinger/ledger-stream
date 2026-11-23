package io.ledgerstream.adapter.in.rest;

import io.ledgerstream.domain.DomainException;
import io.ledgerstream.domain.exception.AccountNotFoundException;
import io.ledgerstream.domain.exception.BookingNotFoundException;
import io.ledgerstream.domain.exception.DuplicateAccountNumberException;
import io.ledgerstream.domain.exception.InvalidDomainValueException;
import io.ledgerstream.domain.exception.OptimisticLockConflictException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public final class ProblemDetailExceptionHandler {

    private static final String ERROR_TYPE_PREFIX = "urn:ledger-stream:error:";

    @ExceptionHandler(InvalidDomainValueException.class)
    public ProblemDetail handleInvalidDomainValue(
            final InvalidDomainValueException exception,
            final HttpServletRequest request
    ) {
        return buildProblem(HttpStatus.BAD_REQUEST, "Invalid input", exception.errorCode(), exception.getMessage(), request);
    }

    @ExceptionHandler({AccountNotFoundException.class, BookingNotFoundException.class})
    public ProblemDetail handleNotFound(
            final DomainException exception,
            final HttpServletRequest request
    ) {
        return buildProblem(HttpStatus.NOT_FOUND, "Resource not found", exception.errorCode(), exception.getMessage(), request);
    }

    @ExceptionHandler({DuplicateAccountNumberException.class, OptimisticLockConflictException.class})
    public ProblemDetail handleConflict(
            final DomainException exception,
            final HttpServletRequest request
    ) {
        return buildProblem(HttpStatus.CONFLICT, "Conflict", exception.errorCode(), exception.getMessage(), request);
    }

    @ExceptionHandler(DomainException.class)
    public ProblemDetail handleDomainException(
            final DomainException exception,
            final HttpServletRequest request
    ) {
        return buildProblem(
                HttpStatus.UNPROCESSABLE_ENTITY,
                "Domain rule violation",
                exception.errorCode(),
                exception.getMessage(),
                request
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidationErrors(
            final MethodArgumentNotValidException exception,
            final HttpServletRequest request
    ) {
        ProblemDetail problem = buildProblem(
                HttpStatus.BAD_REQUEST,
                "Validation failed",
                "VALIDATION_FAILED",
                "Request validation failed",
                request
        );
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        for (FieldError fieldError : exception.getBindingResult().getFieldErrors()) {
            fieldErrors.put(fieldError.getField(), fieldError.getDefaultMessage());
        }
        problem.setProperty("errors", fieldErrors);
        return problem;
    }

    @ExceptionHandler({
            MissingRequestHeaderException.class,
            HandlerMethodValidationException.class,
            HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class
    })
    public ProblemDetail handleBadRequest(
            final Exception exception,
            final HttpServletRequest request
    ) {
        return buildProblem(
                HttpStatus.BAD_REQUEST,
                "Bad request",
                "BAD_REQUEST",
                exception.getMessage(),
                request
        );
    }

    private ProblemDetail buildProblem(
            final HttpStatus status,
            final String title,
            final String errorCode,
            final String detail,
            final HttpServletRequest request
    ) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create(ERROR_TYPE_PREFIX + errorCode.toLowerCase()));
        problem.setTitle(title);
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("errorCode", errorCode);
        return problem;
    }
}
