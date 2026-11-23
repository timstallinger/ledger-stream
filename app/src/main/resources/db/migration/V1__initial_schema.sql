CREATE TABLE accounts (
    id             UUID           PRIMARY KEY,
    account_number VARCHAR(64)    NOT NULL,
    balance        NUMERIC(19, 4) NOT NULL CHECK (balance >= 0),
    currency       CHAR(3)        NOT NULL,
    version        BIGINT         NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_accounts_account_number UNIQUE (account_number)
);

CREATE TABLE bookings (
    id                  UUID           PRIMARY KEY,
    account_id          UUID           NOT NULL REFERENCES accounts (id),
    booking_type        VARCHAR(16)    NOT NULL CHECK (booking_type IN ('DEBIT', 'CREDIT')),
    amount              NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    currency            CHAR(3)        NOT NULL,
    reference           VARCHAR(255)   NOT NULL,
    status              VARCHAR(16)    NOT NULL CHECK (status IN ('POSTED', 'CANCELLED')),
    idempotency_key     VARCHAR(128)   NOT NULL,
    original_booking_id UUID           NULL REFERENCES bookings (id),
    created_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_bookings_account_idempotency_key
    ON bookings (account_id, idempotency_key);

CREATE INDEX idx_bookings_account_created_at
    ON bookings (account_id, created_at DESC);
