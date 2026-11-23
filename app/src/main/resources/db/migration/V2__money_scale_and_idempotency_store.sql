ALTER TABLE accounts
    DROP CONSTRAINT IF EXISTS accounts_balance_check,
    ALTER COLUMN balance TYPE NUMERIC(19, 2),
    ADD CONSTRAINT chk_accounts_balance_non_negative CHECK (balance >= 0);

ALTER TABLE bookings
    ALTER COLUMN amount TYPE NUMERIC(19, 2),
    ADD CONSTRAINT chk_bookings_idempotency_key_length CHECK (char_length(idempotency_key) BETWEEN 1 AND 128);

CREATE TABLE idempotency_keys (
    account_id      UUID         NOT NULL REFERENCES accounts (id),
    idempotency_key VARCHAR(128) NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 128),
    booking_id      UUID         NOT NULL REFERENCES bookings (id),
    response_status SMALLINT     NOT NULL CHECK (response_status BETWEEN 200 AND 299),
    response_body   TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_idempotency_keys PRIMARY KEY (account_id, idempotency_key)
);
