package io.ledgerstream.application.port.in;

import java.util.function.Function;

public interface CancelBookingUseCase {

    IdempotentExecutionResult execute(
            CancelBookingCommand command,
            Function<BookingExecution, StoredIdempotentResponse> responseSerializer
    );
}
