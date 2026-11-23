package io.ledgerstream.adapter.in.rest;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record CreateAccountRequest(
        @NotBlank(message = "accountNumber must not be blank")
        @Size(min = 3, max = 64, message = "accountNumber length must be between 3 and 64")
        @Pattern(regexp = "^[A-Za-z0-9\\-_]+$", message = "accountNumber contains unsupported characters")
        String accountNumber,

        @NotBlank(message = "currency must not be blank")
        @Size(min = 3, max = 3, message = "currency must be a 3-letter ISO-4217 code")
        String currency
) {
}
