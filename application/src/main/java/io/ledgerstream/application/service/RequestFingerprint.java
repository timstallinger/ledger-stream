package io.ledgerstream.application.service;

import io.ledgerstream.application.port.in.CancelBookingCommand;
import io.ledgerstream.application.port.in.CreateBookingCommand;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Deterministic SHA-256 fingerprint over canonical business fields (NOT the raw HTTP body)
 * so whitespace or JSON field ordering differences never cause false mismatches, while any
 * semantic difference (amount, type, currency, reference, target booking) is rejected with 422.
 */
public final class RequestFingerprint {

    private RequestFingerprint() {
    }

    public static String forCreateBooking(final CreateBookingCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        String normalizedAmount = command.amount().amount().stripTrailingZeros().toPlainString();
        String canonical = String.join(
                "|",
                "CREATE",
                command.accountId().value().toString(),
                command.type().name(),
                normalizedAmount,
                command.amount().currency().value(),
                command.reference().value()
        );
        return sha256Hex(canonical);
    }

    public static String forCancelBooking(final CancelBookingCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        String canonical = String.join(
                "|",
                "CANCEL",
                command.accountId().value().toString(),
                command.bookingId().value().toString(),
                command.reference().value()
        );
        return sha256Hex(canonical);
    }

    private static String sha256Hex(final String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", exception);
        }
    }
}
