ALTER TABLE outbox_events
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE outbox_events
    DROP CONSTRAINT IF EXISTS outbox_events_status_check,
    ADD CONSTRAINT chk_outbox_events_status CHECK (status IN ('PENDING', 'SENT', 'FAILED'));

CREATE INDEX idx_outbox_events_pending_next_attempt
    ON outbox_events (status, next_attempt_at)
    WHERE status = 'PENDING';
