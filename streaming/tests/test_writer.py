import threading
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaException
from conftest import message

from wattlake_streaming.bronze import TABLE, ensure_table, next_offsets
from wattlake_streaming.writer import Settings, collect, run_batch, write

TOPIC = "eco2mix-realtime"


def kafka_message(record, error=None):
    return SimpleNamespace(
        error=lambda: error,
        partition=lambda: record.partition,
        offset=lambda: record.offset,
        value=lambda: record.value,
    )


class FakeClock:
    """Each call moves time forward by step seconds."""

    def __init__(self, step: float = 1.0) -> None:
        self.now = 0.0
        self.step = step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


class FakeConsumer:
    """Returns the queued batches one consume() call at a time, then nothing."""

    def __init__(self, batches: list[list] | None = None) -> None:
        self.batches = list(batches or [])
        self.commits: list[list] = []
        self.num_messages: list[int] = []

    def consume(self, num_messages, timeout):
        self.num_messages.append(num_messages)
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets, asynchronous):
        assert asynchronous is False
        self.commits.append([(tp.topic, tp.partition, tp.offset) for tp in offsets])


def test_collect_stops_at_the_deadline():
    consumer = FakeConsumer([[kafka_message(message(0))], [kafka_message(message(1))]])
    records = collect(consumer, timedelta(seconds=10), 100, threading.Event(), clock=FakeClock())
    assert [r.offset for r in records] == [0, 1]


def test_collect_stops_at_max_messages():
    consumer = FakeConsumer([[kafka_message(message(i)) for i in range(3)], [kafka_message(message(3))]])
    records = collect(consumer, timedelta(hours=1), 3, threading.Event(), clock=FakeClock())
    assert len(records) == 3
    assert consumer.num_messages == [3]


def test_collect_stops_when_asked():
    stop = threading.Event()
    stop.set()
    assert collect(FakeConsumer([[kafka_message(message(0))]]), timedelta(hours=1), 10, stop) == []


def test_collect_raises_on_kafka_error():
    consumer = FakeConsumer([[kafka_message(message(0), error="broker down")]])
    with pytest.raises(KafkaException):
        collect(consumer, timedelta(seconds=10), 10, threading.Event(), clock=FakeClock())


def test_write_appends_and_moves_offsets(catalog):
    table = ensure_table(catalog)
    offsets = write(table, [message(5), message(6, partition=1)], TOPIC, {0: 5, 2: 40})

    # Partitions without new messages keep their offset.
    assert offsets == {0: 6, 1: 7, 2: 40}
    assert next_offsets(catalog.load_table(TABLE), TOPIC) == offsets
    assert table.scan().to_arrow().num_rows == 2


def test_run_batch_writes_then_commits_offsets(catalog):
    table = ensure_table(catalog)
    consumer = FakeConsumer([[kafka_message(message(0)), kafka_message(message(1))]])
    settings = Settings(batch_interval=timedelta(seconds=5))

    offsets, written = run_batch(consumer, table, settings, {}, threading.Event())

    assert (offsets, written) == ({0: 2}, 2)
    assert consumer.commits == [[(TOPIC, 0, 2)]]


def test_run_batch_without_messages_writes_nothing(catalog):
    table = ensure_table(catalog)
    consumer = FakeConsumer()
    settings = Settings(batch_interval=timedelta(seconds=2))

    assert run_batch(consumer, table, settings, {0: 7}, threading.Event()) == ({0: 7}, 0)
    assert table.current_snapshot() is None
    assert consumer.commits == []


def test_settings_from_environment(monkeypatch):
    assert Settings.from_env() == Settings()
    monkeypatch.setenv("KAFKA_TOPIC", "other")
    monkeypatch.setenv("ICEBERG_TABLE", "bronze.other")
    monkeypatch.setenv("BATCH_INTERVAL_SECONDS", "30")
    monkeypatch.setenv("MAX_BATCH_MESSAGES", "50")
    monkeypatch.setenv("HEARTBEAT_FILE", "/tmp/hb")

    settings = Settings.from_env()

    assert (settings.topic, settings.table) == ("other", "bronze.other")
    assert settings.batch_interval == timedelta(seconds=30)
    assert settings.max_batch_messages == 50
    assert settings.heartbeat_file == Path("/tmp/hb")
