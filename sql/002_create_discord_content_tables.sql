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

CREATE TABLE IF NOT EXISTS discord_authors (
    author_id BIGINT PRIMARY KEY,
    username TEXT,
    global_name TEXT,
    discriminator TEXT,
    avatar TEXT,
    is_bot BOOLEAN,
    raw_author JSONB,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS discord_channels (
    channel_id BIGINT PRIMARY KEY,
    guild_id BIGINT,
    channel_name TEXT,
    channel_type INTEGER,
    raw_channel JSONB,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS discord_channels_guild_idx
    ON discord_channels (guild_id);

CREATE TABLE IF NOT EXISTS discord_guilds (
    guild_id BIGINT PRIMARY KEY,
    guild_name TEXT,
    icon TEXT,
    raw_guild JSONB,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
