CREATE TABLE IF NOT EXISTS raw_message_queue (
    id BIGSERIAL PRIMARY KEY,
    source_kind TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload BYTEA NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
