import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import psycopg
from psycopg import sql


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("db_exporter")

REQUIRED_ENVIRONMENT_VARIABLES = (
    "DISCORDLESS_DB_HOST",
    "DISCORDLESS_DB_PORT",
    "DISCORDLESS_DB_NAME",
    "DISCORDLESS_DB_USER",
    "DISCORDLESS_DB_PASSWORD",
)
DISCORD_EPOCH_MS = 1420070400000  # 2015-01-01T00:00:00Z
REST_MESSAGES_URL_PATTERN = re.compile(r"https://discord.com/api/v\d+/channels/(\d+)/messages(?:\?|$)")


def load_required_environment():
    missing = [name for name in REQUIRED_ENVIRONMENT_VARIABLES if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing required database environment variables: {', '.join(missing)}")


def validated_table_name(variable_name: str, default: str) -> str:
    table_name = os.environ.get(variable_name, default)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise RuntimeError(f"Invalid table name in {variable_name}: {table_name}")
    return table_name


def connect():
    connection_arguments = {
        "host": os.environ["DISCORDLESS_DB_HOST"],
        "port": int(os.environ["DISCORDLESS_DB_PORT"]),
        "dbname": os.environ["DISCORDLESS_DB_NAME"],
        "user": os.environ["DISCORDLESS_DB_USER"],
        "password": os.environ["DISCORDLESS_DB_PASSWORD"],
        "connect_timeout": int(os.environ.get("DISCORDLESS_DB_CONNECT_TIMEOUT_SECONDS", "5")),
    }
    return psycopg.connect(**connection_arguments)


def snowflake_to_timestamp(snowflake: int) -> datetime:
    timestamp_seconds = ((snowflake >> 22) + DISCORD_EPOCH_MS) / 1000
    return datetime.fromtimestamp(timestamp_seconds, timezone.utc)


class DatabaseExporter:
    def __init__(self):
        load_required_environment()
        self.queue_table = validated_table_name("DISCORDLESS_DB_QUEUE_TABLE", "raw_message_queue")
        self.message_table = validated_table_name("DISCORDLESS_DB_MESSAGES_TABLE", "discord_messages")
        self.author_table = validated_table_name("DISCORDLESS_DB_AUTHORS_TABLE", "discord_authors")
        self.channel_table = validated_table_name("DISCORDLESS_DB_CHANNELS_TABLE", "discord_channels")
        self.guild_table = validated_table_name("DISCORDLESS_DB_GUILDS_TABLE", "discord_guilds")
        self.poll_interval_seconds = float(os.environ.get("DISCORDLESS_DB_EXPORTER_POLL_INTERVAL_SECONDS", "1"))
        self.connection = connect()
        self.connection.autocommit = False

    def reconnect(self):
        try:
            self.connection.close()
        except Exception:
            pass
        self.connection = connect()
        self.connection.autocommit = False

    def pop_queue_batch(self, batch_size=100):
        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "WITH next_items AS ("
                    " SELECT id, source_kind, observed_at, metadata, payload"
                    " FROM {queue_table}"
                    " ORDER BY id"
                    " FOR UPDATE SKIP LOCKED"
                    " LIMIT %s"
                    ")"
                    " DELETE FROM {queue_table} AS queue"
                    " USING next_items"
                    " WHERE queue.id = next_items.id"
                    " RETURNING next_items.source_kind, next_items.observed_at, next_items.metadata, next_items.payload"
                ).format(queue_table=sql.Identifier(self.queue_table)),
                (batch_size,),
            )
            return cursor.fetchall()

    def _prepare_message_row(self, observed_at, source_kind: str, message_payload: dict):
        message_id = int(message_payload["id"])
        channel_id = int(message_payload["channel_id"])
        guild_id = message_payload.get("guild_id")
        if guild_id is not None:
            guild_id = int(guild_id)
        author_id = int(message_payload["author"]["id"])
        content = message_payload.get("content", "")
        created_at = snowflake_to_timestamp(message_id)
        return (
            message_id, channel_id, guild_id, author_id, content,
            created_at, observed_at, source_kind, json.dumps(message_payload),
        )

    def _prepare_lookup_rows(self, observed_at, message_payload: dict):
        author = message_payload.get("author")
        if not isinstance(author, dict) or "id" not in author:
            return None, None, None
        channel_id_value = message_payload.get("channel_id")
        if channel_id_value is None:
            return None, None, None
        try:
            channel_id = int(channel_id_value)
            author_id = int(author["id"])
        except (TypeError, ValueError):
            return None, None, None

        guild_id_value = message_payload.get("guild_id")
        guild_id = None
        if guild_id_value is not None:
            try:
                guild_id = int(guild_id_value)
            except (TypeError, ValueError):
                pass
        guild_data = message_payload.get("guild") if isinstance(message_payload.get("guild"), dict) else None
        channel_data = message_payload.get("channel") if isinstance(message_payload.get("channel"), dict) else None

        guild_row = None
        if guild_id is not None:
            guild_name = guild_data.get("name") if guild_data is not None else None
            guild_icon = guild_data.get("icon") if guild_data is not None else None
            raw_guild = json.dumps(guild_data) if guild_data is not None else None
            guild_row = (guild_id, guild_name, guild_icon, raw_guild, observed_at, observed_at)

        channel_name = channel_data.get("name") if channel_data is not None else None
        channel_type = channel_data.get("type") if channel_data is not None else None
        raw_channel = json.dumps(channel_data) if channel_data is not None else None
        channel_row = (channel_id, guild_id, channel_name, channel_type, raw_channel, observed_at, observed_at)

        author_row = (
            author_id, author.get("username"), author.get("global_name"),
            author.get("discriminator"), author.get("avatar"), author.get("bot"),
            json.dumps(author), observed_at, observed_at,
        )
        return guild_row, channel_row, author_row

    def flush_batch(self, message_rows, guild_rows, channel_rows, author_rows):
        if not message_rows:
            return
        with self.connection.cursor() as cursor:
            if guild_rows:
                cursor.executemany(
                    sql.SQL(
                        "INSERT INTO {guild_table}"
                        " (guild_id, guild_name, icon, raw_guild, first_seen_at, last_seen_at)"
                        " VALUES (%s, %s, %s, %s::jsonb, %s, %s)"
                        " ON CONFLICT (guild_id) DO UPDATE SET"
                        " guild_name = EXCLUDED.guild_name,"
                        " icon = EXCLUDED.icon,"
                        " raw_guild = EXCLUDED.raw_guild,"
                        " last_seen_at = GREATEST({guild_table}.last_seen_at, EXCLUDED.last_seen_at)"
                    ).format(guild_table=sql.Identifier(self.guild_table)),
                    guild_rows,
                )

            cursor.executemany(
                sql.SQL(
                    "INSERT INTO {channel_table}"
                    " (channel_id, guild_id, channel_name, channel_type, raw_channel, first_seen_at, last_seen_at)"
                    " VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)"
                    " ON CONFLICT (channel_id) DO UPDATE SET"
                    " guild_id = EXCLUDED.guild_id,"
                    " channel_name = EXCLUDED.channel_name,"
                    " channel_type = EXCLUDED.channel_type,"
                    " raw_channel = EXCLUDED.raw_channel,"
                    " last_seen_at = GREATEST({channel_table}.last_seen_at, EXCLUDED.last_seen_at)"
                ).format(channel_table=sql.Identifier(self.channel_table)),
                channel_rows,
            )

            cursor.executemany(
                sql.SQL(
                    "INSERT INTO {author_table}"
                    " (author_id, username, global_name, discriminator, avatar, is_bot, raw_author, first_seen_at, last_seen_at)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)"
                    " ON CONFLICT (author_id) DO UPDATE SET"
                    " username = EXCLUDED.username,"
                    " global_name = EXCLUDED.global_name,"
                    " discriminator = EXCLUDED.discriminator,"
                    " avatar = EXCLUDED.avatar,"
                    " is_bot = EXCLUDED.is_bot,"
                    " raw_author = EXCLUDED.raw_author,"
                    " last_seen_at = GREATEST({author_table}.last_seen_at, EXCLUDED.last_seen_at)"
                ).format(author_table=sql.Identifier(self.author_table)),
                author_rows,
            )

            cursor.executemany(
                sql.SQL(
                    "INSERT INTO {message_table}"
                    " (message_id, channel_id, guild_id, author_id, content, created_at, observed_at, source_kind, raw_message)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
                    " ON CONFLICT (message_id) DO UPDATE SET"
                    " channel_id = EXCLUDED.channel_id,"
                    " guild_id = EXCLUDED.guild_id,"
                    " author_id = EXCLUDED.author_id,"
                    " content = EXCLUDED.content,"
                    " created_at = EXCLUDED.created_at,"
                    " observed_at = EXCLUDED.observed_at,"
                    " source_kind = EXCLUDED.source_kind,"
                    " raw_message = EXCLUDED.raw_message"
                ).format(message_table=sql.Identifier(self.message_table)),
                message_rows,
            )

    def collect_rest_messages(self, observed_at, metadata, payload, message_rows, guild_rows, channel_rows, author_rows):
        request_url = (metadata or {}).get("url", "")
        match = REST_MESSAGES_URL_PATTERN.match(request_url)
        if not match:
            return

        channel_id = int(match.group(1))
        try:
            decoded_payload = payload.decode("utf-8", errors="strict")
            parsed = json.loads(decoded_payload)
        except Exception as error:
            logger.warning("Failed to parse rest_response at %s for url %s: %s", observed_at, request_url, error)
            return
        messages = parsed if isinstance(parsed, list) else [parsed]

        for message in messages:
            if not isinstance(message, dict) or "id" not in message or "author" not in message:
                continue
            message.setdefault("channel_id", channel_id)
            guild_row, channel_row, author_row = self._prepare_lookup_rows(observed_at, message)
            if channel_row is not None:
                if guild_row is not None:
                    guild_rows.append(guild_row)
                channel_rows.append(channel_row)
                author_rows.append(author_row)
            message_rows.append(self._prepare_message_row(observed_at, "rest_response", message))

    def collect_gateway_message_create(self, observed_at, payload, message_rows, guild_rows, channel_rows, author_rows):
        try:
            decoded_payload = payload.decode("utf-8", errors="strict")
            gateway_event = json.loads(decoded_payload)
        except Exception as error:
            logger.warning("Failed to parse gateway_chunk at %s: %s", observed_at, error)
            return
        if gateway_event.get("t") != "MESSAGE_CREATE":
            return
        message_data = gateway_event.get("d")
        if not isinstance(message_data, dict):
            return
        if "id" not in message_data or "author" not in message_data or "channel_id" not in message_data:
            return
        guild_row, channel_row, author_row = self._prepare_lookup_rows(observed_at, message_data)
        if channel_row is not None:
            if guild_row is not None:
                guild_rows.append(guild_row)
            channel_rows.append(channel_row)
            author_rows.append(author_row)
        message_rows.append(self._prepare_message_row(observed_at, "gateway_chunk", message_data))

    def process_queue_batch(self, queue_items):
        message_rows = []
        guild_rows = []
        channel_rows = []
        author_rows = []
        for source_kind, observed_at, metadata, payload in queue_items:
            if source_kind == "rest_response":
                self.collect_rest_messages(observed_at, metadata, payload, message_rows, guild_rows, channel_rows, author_rows)
            elif source_kind == "gateway_chunk":
                self.collect_gateway_message_create(observed_at, payload, message_rows, guild_rows, channel_rows, author_rows)
        self.flush_batch(message_rows, guild_rows, channel_rows, author_rows)

    def _setup_listen(self):
        self.connection.execute("LISTEN raw_message_queue_channel")

    def run_forever(self):
        logger.info("DB exporter started (queue table: %s)", self.queue_table)
        self._setup_listen()
        while True:
            try:
                queue_items = None
                with self.connection.transaction():
                    queue_items = self.pop_queue_batch()
                    if queue_items:
                        self.process_queue_batch(queue_items)
                if not queue_items:
                    # Drain any pending notifications, then wait for the next one.
                    # Use poll_interval_seconds as the timeout so we still
                    # periodically re-check in case a notification was missed.
                    for _notify in self.connection.notifies(
                        timeout=self.poll_interval_seconds,
                        stop_after=1,
                    ):
                        break
            except KeyboardInterrupt:
                logger.info("DB exporter stopped")
                return
            except Exception:
                logger.exception("Failed while processing queue batch")
                self.reconnect()
                try:
                    self._setup_listen()
                except Exception:
                    logger.exception("Failed to re-establish LISTEN after reconnect")
                time.sleep(self.poll_interval_seconds)


if __name__ == "__main__":
    DatabaseExporter().run_forever()
