package io.ledgerstream.domain.model;

public enum BookingType {
    DEBIT,
    CREDIT;

    public BookingType opposite() {
        return switch (this) {
            case DEBIT -> CREDIT;
            case CREDIT -> DEBIT;
        };
    }
}
