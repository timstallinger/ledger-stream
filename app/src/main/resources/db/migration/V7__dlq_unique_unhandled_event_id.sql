CREATE UNIQUE INDEX uq_dlq_entries_unhandled_event_id
    ON dlq_entries (event_id)
    WHERE event_id IS NOT NULL AND replayed_at IS NULL;
