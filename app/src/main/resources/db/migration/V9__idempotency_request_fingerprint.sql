ALTER TABLE idempotency_keys
    ADD COLUMN request_fingerprint CHAR(64);

UPDATE idempotency_keys
SET request_fingerprint = repeat('0', 64)
WHERE request_fingerprint IS NULL;

ALTER TABLE idempotency_keys
    ALTER COLUMN request_fingerprint SET NOT NULL;
