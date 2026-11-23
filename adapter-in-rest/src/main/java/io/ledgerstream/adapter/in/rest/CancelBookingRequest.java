package io.ledgerstream.adapter.in.rest;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CancelBookingRequest(
        @NotBlank(message = "reference must not be blank")
        @Size(max = 255, message = "reference length must not exceed 255 characters")
        String reference
) {
}
