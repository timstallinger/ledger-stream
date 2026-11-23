package io.ledgerstream.application.port.in;

import java.util.function.Function;

public interface CreateBookingUseCase {

    IdempotentExecutionResult execute(
            CreateBookingCommand command,
            Function<BookingExecution, StoredIdempotentResponse> responseSerializer
    );
}
