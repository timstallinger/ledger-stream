package io.ledgerstream.adapter.in.rest;

import io.ledgerstream.domain.model.BookingType;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record CreateBookingRequest(
        @NotNull(message = "type must not be null")
        BookingType type,

        @NotNull(message = "amount must not be null")
        @DecimalMin(value = "0.01", message = "amount must be at least 0.01")
        @Digits(integer = 17, fraction = 2, message = "amount must have at most 17 integer and 2 fractional digits")
        BigDecimal amount,

        @NotBlank(message = "currency must not be blank")
        @Size(min = 3, max = 3, message = "currency must be a 3-letter ISO-4217 code")
        String currency,

        @NotBlank(message = "reference must not be blank")
        @Size(max = 255, message = "reference length must not exceed 255 characters")
        String reference
) {
}
