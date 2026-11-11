"""Polls the real-time eCO2mix dataset and publishes its measured rows to Kafka.

Each cycle:
1. reads the dataset's data_processed; if it has not changed since the last
   cycle, there is nothing new and the cycle stops there (one cheap call);
2. reads the measured rows from the cursor minus an overlap window, so that
   values RTE revised in the last hours are published again;
3. publishes them oldest first and waits until Kafka has them all;
4. moves the cursor to the latest quarter-hour published.

At startup, the cursor is the latest quarter-hour of the topic; on an empty
topic, the whole dataset is published (from September 1st).
Run with: python -m wattlake_streaming.producer (configuration: Settings).
"""

import logging
import os
import signal
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Protocol

from wattlake_streaming.kafka import Publisher, read_cursor
from wattlake_streaming.messages import encode, row_time
from wattlake_streaming.odre import DATASET, OdreClient, OdreError, Row

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    """Configuration, read from environment variables (defaults: the cluster's values)."""

    bootstrap_servers: str = "wattlake-kafka-bootstrap:9092"
    topic: str = "eco2mix-realtime"
    # The dataset is refreshed every 15 minutes: polling every 5 minutes
    # publishes a refresh at most 5 minutes late, for about 8,600 calls a month
    # (quota: 50,000 per month on the dataset).
    poll_interval: timedelta = timedelta(minutes=5)
    # Rows of the last 3 hours are published again on each refresh.
    overlap: timedelta = timedelta(hours=3)
    # Touched on every cycle; the liveness probe checks its age (health.py).
    heartbeat_file: Path = Path("/tmp/heartbeat")

    @classmethod
    def from_env(cls) -> "Settings":
        settings = cls()
        env = os.environ
        if "KAFKA_BOOTSTRAP_SERVERS" in env:
            settings = replace(settings, bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"])
        if "KAFKA_TOPIC" in env:
            settings = replace(settings, topic=env["KAFKA_TOPIC"])
        if "POLL_INTERVAL_SECONDS" in env:
            settings = replace(settings, poll_interval=timedelta(seconds=int(env["POLL_INTERVAL_SECONDS"])))
        if "OVERLAP_HOURS" in env:
            settings = replace(settings, overlap=timedelta(hours=int(env["OVERLAP_HOURS"])))
        if "HEARTBEAT_FILE" in env:
            settings = replace(settings, heartbeat_file=Path(env["HEARTBEAT_FILE"]))
        return settings


@dataclass(frozen=True)
class State:
    """Where the producer stands between two cycles."""

    # Latest quarter-hour published, None before the first publication.
    cursor: datetime | None = None
    # data_processed of the dataset when it was last read, None at startup.
    source_updated_at: str | None = None


class Source(Protocol):
    """The part of OdreClient used by a cycle (a fake in the tests)."""

    def updated_at(self) -> str: ...
    def measured_rows(self, since: datetime | None = None) -> list[Row]: ...


def run_cycle(
    source: Source,
    publish_all: Callable[[list[tuple[bytes, bytes]]], None],
    state: State,
    overlap: timedelta,
) -> tuple[State, int]:
    """Run one cycle and return the new state and the number of messages published."""
    updated_at = source.updated_at()
    if updated_at == state.source_updated_at:
        return state, 0

    since = None if state.cursor is None else state.cursor - overlap
    rows = source.measured_rows(since)
    publish_all([encode(row, dataset=DATASET, source_updated_at=updated_at) for row in rows])

    # Only once Kafka has every message: a failed cycle is replayed whole.
    cursor = max([row_time(row) for row in rows], default=None)
    if state.cursor is not None and (cursor is None or cursor < state.cursor):
        cursor = state.cursor
    return State(cursor=cursor, source_updated_at=updated_at), len(rows)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()

    # Kubernetes sends SIGTERM before stopping the pod: finish the current
    # cycle (its messages are flushed) instead of dying in the middle.
    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        logger.info("Received signal %s, stopping after the current cycle", signum)
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    # Before the first cycle, which may read the whole dataset.
    settings.heartbeat_file.touch()
    cursor = read_cursor(settings.bootstrap_servers, settings.topic)
    logger.info("Starting from %s", cursor or "an empty topic: publishing the whole dataset")
    state = State(cursor=cursor)
    publisher = Publisher.connect(settings.bootstrap_servers, settings.topic)

    with OdreClient() as source:
        while not stop.is_set():
            try:
                state, published = run_cycle(source, publisher.publish_all, state, settings.overlap)
                if published:
                    logger.info(
                        "Published %s rows (dataset refreshed at %s), cursor at %s",
                        published,
                        state.source_updated_at,
                        state.cursor,
                    )
            except OdreError as exc:
                # The API being down is not the producer's fault: restarting
                # would not help. Try again on the next cycle.
                logger.warning("Cycle skipped: %s", exc)
            # Kafka errors are not caught: the pod restarts and resumes from the topic.
            settings.heartbeat_file.touch()
            stop.wait(settings.poll_interval.total_seconds())
    logger.info("Stopped")


if __name__ == "__main__":
    main()
