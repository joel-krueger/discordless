CREATE TABLE IF NOT EXISTS discord_messages (
    message_id BIGINT PRIMARY KEY,
    channel_id BIGINT NOT NULL,
    guild_id BIGINT,
    author_id BIGINT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    source_kind TEXT NOT NULL,
    raw_message JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS discord_messages_channel_created_idx
    ON discord_messages (channel_id, created_at);

CREATE INDEX IF NOT EXISTS discord_messages_author_created_idx
    ON discord_messages (author_id, created_at);
