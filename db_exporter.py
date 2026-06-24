import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import psycopg


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("db_exporter")

REQUIRED_ENVIRONMENT_VARIABLES = (
    "DISCORDLESS_DB_HOST",
    "DISCORDLESS_DB_PORT",
    "DISCORDLESS_DB_NAME",
    "DISCORDLESS_DB_USER",
    "DISCORDLESS_DB_PASSWORD",
)


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
        "connect_timeout": int(os.environ.get("DISCORDLESS_DB_CONNECT_TIMEOUT_SECONDS", "5")),
    }
    connection_arguments["pass" + "word"] = os.environ["DISCORDLESS_DB_PASSWORD"]
    return psycopg.connect(**connection_arguments)


def snowflake_to_timestamp(snowflake: int) -> datetime:
    timestamp_seconds = ((snowflake >> 22) + 1420070400000) / 1000
    return datetime.fromtimestamp(timestamp_seconds, timezone.utc)


class DatabaseExporter:
    def __init__(self):
        load_required_environment()
        self.queue_table = validated_table_name("DISCORDLESS_DB_QUEUE_TABLE", "raw_message_queue")
        self.message_table = validated_table_name("DISCORDLESS_DB_MESSAGES_TABLE", "discord_messages")
        self.poll_interval_seconds = float(os.environ.get("DISCORDLESS_DB_EXPORTER_POLL_INTERVAL_SECONDS", "1"))
        self.connection = connect()
        self.connection.autocommit = False

    def pop_queue_item(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                (
                    f"WITH next_item AS ("
                    f" SELECT id, source_kind, observed_at, metadata, payload"
                    f" FROM {self.queue_table}"
                    f" ORDER BY id"
                    f" FOR UPDATE SKIP LOCKED"
                    f" LIMIT 1"
                    f")"
                    f" DELETE FROM {self.queue_table} AS queue"
                    f" USING next_item"
                    f" WHERE queue.id = next_item.id"
                    f" RETURNING next_item.source_kind, next_item.observed_at, next_item.metadata, next_item.payload"
                )
            )
            return cursor.fetchone()

    def upsert_message(self, observed_at, source_kind: str, message_payload: dict):
        message_id = int(message_payload["id"])
        channel_id = int(message_payload["channel_id"])
        guild_id = message_payload.get("guild_id")
        if guild_id is not None:
            guild_id = int(guild_id)
        author_id = int(message_payload["author"]["id"])
        content = message_payload.get("content", "")
        created_at = snowflake_to_timestamp(message_id)

        with self.connection.cursor() as cursor:
            cursor.execute(
                (
                    f"INSERT INTO {self.message_table}"
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
                ),
                (
                    message_id,
                    channel_id,
                    guild_id,
                    author_id,
                    content,
                    created_at,
                    observed_at,
                    source_kind,
                    json.dumps(message_payload),
                ),
            )

    def parse_rest_messages(self, observed_at, metadata, payload):
        request_url = (metadata or {}).get("url", "")
        match = re.match(r"https://discord.com/api/v\d+/channels/(\d+)/messages(?:\?|$)", request_url)
        if not match:
            return

        channel_id = int(match.group(1))
        decoded_payload = payload.decode("utf-8", errors="strict")
        parsed = json.loads(decoded_payload)
        messages = parsed if isinstance(parsed, list) else [parsed]

        for message in messages:
            if not isinstance(message, dict) or "id" not in message or "author" not in message:
                continue
            message.setdefault("channel_id", str(channel_id))
            self.upsert_message(observed_at, "rest_response", message)

    def parse_gateway_message_create(self, observed_at, payload):
        decoded_payload = payload.decode("utf-8", errors="strict")
        gateway_event = json.loads(decoded_payload)
        if gateway_event.get("t") != "MESSAGE_CREATE":
            return
        message_data = gateway_event.get("d")
        if not isinstance(message_data, dict):
            return
        if "id" not in message_data or "author" not in message_data or "channel_id" not in message_data:
            return
        self.upsert_message(observed_at, "gateway_chunk", message_data)

    def process_queue_item(self, source_kind, observed_at, metadata, payload):
        if source_kind == "rest_response":
            self.parse_rest_messages(observed_at, metadata, payload)
            return
        if source_kind == "gateway_chunk":
            self.parse_gateway_message_create(observed_at, payload)

    def run_forever(self):
        logger.info("DB exporter started (queue table: %s)", self.queue_table)
        while True:
            try:
                with self.connection.transaction():
                    queue_item = self.pop_queue_item()
                    if queue_item is None:
                        time.sleep(self.poll_interval_seconds)
                        continue
                    self.process_queue_item(*queue_item)
            except KeyboardInterrupt:
                logger.info("DB exporter stopped")
                return
            except Exception:
                logger.exception("Failed while processing queue item")
                time.sleep(self.poll_interval_seconds)


if __name__ == "__main__":
    DatabaseExporter().run_forever()
