CREATE TABLE dlq_entries (
    id            UUID         PRIMARY KEY,
    topic         VARCHAR(255) NOT NULL,
    partition     INT          NOT NULL,
    "offset"      BIGINT       NOT NULL,
    record_key    VARCHAR(255) NULL,
    event_id      UUID         NULL,
    error_class   VARCHAR(255) NOT NULL,
    error_message TEXT         NOT NULL,
    attempt_count INT          NOT NULL DEFAULT 1,
    payload       BYTEA        NOT NULL,
    headers       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    replayed_at   TIMESTAMPTZ  NULL,
    replayed_by   VARCHAR(128) NULL,
    note          TEXT         NULL
);

CREATE INDEX idx_dlq_entries_unhandled
    ON dlq_entries (created_at ASC)
    WHERE replayed_at IS NULL;
