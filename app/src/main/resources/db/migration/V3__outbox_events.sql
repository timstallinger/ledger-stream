CREATE TABLE outbox_events (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(64)  NOT NULL,
    aggregate_id   UUID         NOT NULL,
    event_type     VARCHAR(64)  NOT NULL,
    schema_version INT          NOT NULL,
    payload        TEXT         NOT NULL,
    correlation_id VARCHAR(128) NULL,
    status         VARCHAR(16)  NOT NULL CHECK (status IN ('PENDING', 'SENT')),
    occurred_at    TIMESTAMPTZ  NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    sent_at        TIMESTAMPTZ  NULL,
    retry_count    INT          NOT NULL DEFAULT 0,
    last_error     TEXT         NULL
);

CREATE INDEX idx_outbox_events_status_created_at
    ON outbox_events (status, created_at);
