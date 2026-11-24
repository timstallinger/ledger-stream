ALTER TABLE dlq_entries
    RENAME COLUMN replayed_by TO last_replayed_by;

ALTER TABLE dlq_entries
    DROP COLUMN note;

ALTER TABLE dlq_entries
    ADD COLUMN payload_sha256 CHAR(64);

UPDATE dlq_entries
SET payload_sha256 = encode(sha256(payload), 'hex')
WHERE payload_sha256 IS NULL;

ALTER TABLE dlq_entries
    ALTER COLUMN payload_sha256 SET NOT NULL;

DROP INDEX IF EXISTS uq_dlq_entries_unhandled_event_id;

CREATE UNIQUE INDEX uq_dlq_entries_unhandled_dedup_key
    ON dlq_entries ((COALESCE(event_id::text, payload_sha256)))
    WHERE replayed_at IS NULL;

CREATE TABLE dlq_replay_attempts (
    id            UUID         PRIMARY KEY,
    dlq_entry_id  UUID         NOT NULL REFERENCES dlq_entries (id),
    attempted_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    attempted_by  VARCHAR(128) NULL,
    outcome       VARCHAR(32)  NOT NULL CHECK (outcome IN ('DISPATCHED', 'FAILED')),
    error_class   VARCHAR(255) NULL,
    note          TEXT         NULL
);

CREATE INDEX idx_dlq_replay_attempts_entry
    ON dlq_replay_attempts (dlq_entry_id, attempted_at ASC);
