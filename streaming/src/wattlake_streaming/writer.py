"""Reads the eco2mix-realtime topic and appends its messages to the bronze table.

Loop:
1. collect messages for up to BATCH_INTERVAL_SECONDS (or MAX_BATCH_MESSAGES);
2. if there are any, append them to the Iceberg table in one commit, with the
   next offset of each partition in the snapshot summary (bronze.py);
3. commit the same offsets to the Kafka consumer group, for monitoring only:
   tools such as kafka-consumer-groups.sh then show the writer's lag.

At startup, the offsets come from the table, not from Kafka: what is in the
table and where to resume always agree, whatever the crash point.
Run with: python -m wattlake_streaming.writer (configuration: Settings).
"""

import logging
import os
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Any, Protocol

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, Message, TopicPartition
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from wattlake_streaming.bronze import TABLE, KafkaRecord, append, ensure_table, next_offsets, to_batch

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    """Configuration, read from environment variables (defaults: the cluster's values)."""

    bootstrap_servers: str = "wattlake-kafka-bootstrap:9092"
    topic: str = "eco2mix-realtime"
    # PyIceberg catalog, configured by PYICEBERG_CATALOG__WATTLAKE__* variables.
    catalog: str = "wattlake"
    table: str = TABLE
    # The producer publishes every 15 minutes at most: one commit per refresh,
    # about 100 snapshots a day.
    batch_interval: timedelta = timedelta(minutes=5)
    # Caps a batch on the first run, when the whole topic is waiting.
    max_batch_messages: int = 10_000
    heartbeat_file: Path = Path("/tmp/heartbeat")

    @classmethod
    def from_env(cls) -> "Settings":
        settings = cls()
        env = os.environ
        for field, var in [
            ("bootstrap_servers", "KAFKA_BOOTSTRAP_SERVERS"),
            ("topic", "KAFKA_TOPIC"),
            ("table", "ICEBERG_TABLE"),
        ]:
            if var in env:
                settings = replace(settings, **{field: env[var]})
        if "BATCH_INTERVAL_SECONDS" in env:
            settings = replace(settings, batch_interval=timedelta(seconds=int(env["BATCH_INTERVAL_SECONDS"])))
        if "MAX_BATCH_MESSAGES" in env:
            settings = replace(settings, max_batch_messages=int(env["MAX_BATCH_MESSAGES"]))
        if "HEARTBEAT_FILE" in env:
            settings = replace(settings, heartbeat_file=Path(env["HEARTBEAT_FILE"]))
        return settings


class ConsumerLike(Protocol):
    """The part of confluent_kafka.Consumer used here (a fake in the tests)."""

    def consume(self, num_messages: int, timeout: float) -> list[Message]: ...
    def commit(self, offsets: list[TopicPartition], asynchronous: bool) -> Any: ...


def consumer_config(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "wattlake-writer",
        # Offsets are committed by hand, after the Iceberg commit.
        "enable.auto.commit": False,
        # Only used if a stored offset no longer exists in the topic.
        "auto.offset.reset": "earliest",
    }


def collect(
    consumer: ConsumerLike,
    interval: timedelta,
    max_messages: int,
    stop: threading.Event,
    clock: Callable[[], float] = time.monotonic,
) -> list[KafkaRecord]:
    """Return the messages received within interval, at most max_messages."""
    deadline = clock() + interval.total_seconds()
    records: list[KafkaRecord] = []
    while len(records) < max_messages and not stop.is_set():
        remaining = deadline - clock()
        if remaining <= 0:
            break
        for message in consumer.consume(
            num_messages=min(500, max_messages - len(records)), timeout=min(1.0, remaining)
        ):
            if message.error() is not None:
                raise KafkaException(message.error())
            records.append(KafkaRecord(partition=message.partition(), offset=message.offset(), value=message.value()))
    return records


def write(table: Table, records: list[KafkaRecord], topic: str, offsets: dict[int, int]) -> dict[int, int]:
    """Append records to table and return the new next offsets (offsets updated)."""
    batch = to_batch(records, ingested_at=datetime.now(UTC))
    new_offsets = dict(offsets)
    for record in records:
        new_offsets[record.partition] = max(new_offsets.get(record.partition, 0), record.offset + 1)
    append(table, batch, topic, new_offsets)
    return new_offsets


def run_batch(
    consumer: ConsumerLike, table: Table, settings: Settings, offsets: dict[int, int], stop: threading.Event
) -> tuple[dict[int, int], int]:
    """Collect one batch, write it, and return the new offsets and the number of rows written."""
    records = collect(consumer, settings.batch_interval, settings.max_batch_messages, stop)
    if not records:
        return offsets, 0
    offsets = write(table, records, settings.topic, offsets)
    # Monitoring only: the table is the reference (see the module docstring).
    consumer.commit(
        offsets=[TopicPartition(settings.topic, p, o) for p, o in sorted(offsets.items())], asynchronous=False
    )
    return offsets, len(records)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()

    # Kubernetes sends SIGTERM before stopping the pod: write the messages
    # already collected instead of dropping the batch.
    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        logger.info("Received signal %s, stopping after the current batch", signum)
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    settings.heartbeat_file.touch()

    table = ensure_table(load_catalog(settings.catalog), settings.table)
    offsets = next_offsets(table, settings.topic)
    logger.info("Table %s, resuming %s at offsets %s", settings.table, settings.topic, offsets or "of the start")

    consumer = Consumer(consumer_config(settings.bootstrap_servers))
    try:
        metadata = consumer.list_topics(settings.topic, timeout=30)
        topic_metadata = metadata.topics.get(settings.topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise KafkaException(f"cannot read topic {settings.topic}")
        consumer.assign(
            [TopicPartition(settings.topic, p, offsets.get(p, OFFSET_BEGINNING)) for p in topic_metadata.partitions]
        )
        while not stop.is_set():
            offsets, written = run_batch(consumer, table, settings, offsets, stop)
            if written:
                logger.info("Appended %s rows to %s, next offsets %s", written, settings.table, offsets)
            settings.heartbeat_file.touch()
    finally:
        consumer.close()
    logger.info("Stopped")


if __name__ == "__main__":
    main()
