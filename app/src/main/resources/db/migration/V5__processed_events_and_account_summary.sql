CREATE TABLE processed_events (
    event_id     UUID        PRIMARY KEY,
    event_type   VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE account_summary (
    account_id     UUID           PRIMARY KEY,
    account_number VARCHAR(64)    NOT NULL,
    balance        NUMERIC(19, 2) NOT NULL DEFAULT 0.00,
    currency       CHAR(3)        NOT NULL,
    booking_count  BIGINT         NOT NULL DEFAULT 0,
    credit_total   NUMERIC(19, 2) NOT NULL DEFAULT 0.00,
    debit_total    NUMERIC(19, 2) NOT NULL DEFAULT 0.00,
    last_event_id  UUID           NULL,
    last_event_at  TIMESTAMPTZ    NULL,
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);
