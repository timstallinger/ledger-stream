package io.ledgerstream.adapter.in.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerstream.application.port.in.BookingExecution;
import io.ledgerstream.application.port.in.CancelBookingCommand;
import io.ledgerstream.application.port.in.CancelBookingUseCase;
import io.ledgerstream.application.port.in.CreateBookingCommand;
import io.ledgerstream.application.port.in.CreateBookingUseCase;
import io.ledgerstream.application.port.in.IdempotentExecutionResult;
import io.ledgerstream.application.port.in.ListBookingsUseCase;
import io.ledgerstream.application.port.in.StoredIdempotentResponse;
import io.ledgerstream.domain.model.AccountId;
import io.ledgerstream.domain.model.BookingId;
import io.ledgerstream.domain.model.CurrencyCode;
import io.ledgerstream.domain.model.IdempotencyKey;
import io.ledgerstream.domain.model.Money;
import io.ledgerstream.domain.model.Reference;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/accounts/{accountId}/bookings")
public final class BookingController {

    private static final Logger LOG = LoggerFactory.getLogger(BookingController.class);
    private static final String HEADER_IDEMPOTENCY_KEY = "Idempotency-Key";
    private static final String HEADER_IDEMPOTENCY_REPLAYED = "Idempotency-Replayed";

    private final CreateBookingUseCase createBookingUseCase;
    private final CancelBookingUseCase cancelBookingUseCase;
    private final ListBookingsUseCase listBookingsUseCase;
    private final ObjectMapper objectMapper;

    public BookingController(
            final CreateBookingUseCase createBookingUseCase,
            final CancelBookingUseCase cancelBookingUseCase,
            final ListBookingsUseCase listBookingsUseCase,
            final ObjectMapper objectMapper
    ) {
        this.createBookingUseCase = Objects.requireNonNull(createBookingUseCase, "createBookingUseCase must not be null");
        this.cancelBookingUseCase = Objects.requireNonNull(cancelBookingUseCase, "cancelBookingUseCase must not be null");
        this.listBookingsUseCase = Objects.requireNonNull(listBookingsUseCase, "listBookingsUseCase must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createBooking(
            @PathVariable final UUID accountId,
            @RequestHeader(HEADER_IDEMPOTENCY_KEY) final String idempotencyKeyHeader,
            @Valid @RequestBody final CreateBookingRequest request
    ) {
        String correlationId = MDC.get(CorrelationIdFilter.MDC_CORRELATION_ID);
        CreateBookingCommand command = new CreateBookingCommand(
                AccountId.of(accountId),
                request.type(),
                Money.of(request.amount(), CurrencyCode.of(request.currency())),
                Reference.of(request.reference()),
                IdempotencyKey.of(idempotencyKeyHeader),
                correlationId
        );
        IdempotentExecutionResult result = createBookingUseCase.execute(command, this::serializeCreatedBooking);
        LOG.info(
                "HTTP booking request completed: stage=http type={} status={} replayed={}",
                request.type(),
                result.response().statusCode(),
                result.replayed()
        );
        return toHttpResponse(result);
    }

    @PostMapping(value = "/{bookingId}/cancel", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> cancelBooking(
            @PathVariable final UUID accountId,
            @PathVariable final UUID bookingId,
            @RequestHeader(HEADER_IDEMPOTENCY_KEY) final String idempotencyKeyHeader,
            @Valid @RequestBody final CancelBookingRequest request
    ) {
        String correlationId = MDC.get(CorrelationIdFilter.MDC_CORRELATION_ID);
        CancelBookingCommand command = new CancelBookingCommand(
                AccountId.of(accountId),
                BookingId.of(bookingId),
                Reference.of(request.reference()),
                IdempotencyKey.of(idempotencyKeyHeader),
                correlationId
        );
        IdempotentExecutionResult result = cancelBookingUseCase.execute(command, this::serializeCreatedBooking);
        LOG.info(
                "HTTP cancellation request completed: stage=http status={} replayed={}",
                result.response().statusCode(),
                result.replayed()
        );
        return toHttpResponse(result);
    }

    @GetMapping
    public ResponseEntity<List<BookingResponse>> listBookings(@PathVariable final UUID accountId) {
        List<BookingResponse> bookings = listBookingsUseCase.listByAccountId(AccountId.of(accountId)).stream()
                .map(BookingResponse::from)
                .toList();
        return ResponseEntity.ok(bookings);
    }

    private StoredIdempotentResponse serializeCreatedBooking(final BookingExecution execution) {
        try {
            String json = objectMapper.writeValueAsString(BookingResponse.from(execution.booking()));
            return new StoredIdempotentResponse(HttpStatus.CREATED.value(), json);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize booking response for idempotency store", exception);
        }
    }

    private ResponseEntity<String> toHttpResponse(final IdempotentExecutionResult result) {
        ResponseEntity.BodyBuilder builder = ResponseEntity
                .status(result.response().statusCode())
                .contentType(MediaType.APPLICATION_JSON);
        if (result.replayed()) {
            builder.header(HEADER_IDEMPOTENCY_REPLAYED, "true");
        }
        return builder.body(result.response().responseBody());
    }
}
