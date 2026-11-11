import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wattlake_streaming.kafka import KafkaPublishError
from wattlake_streaming.producer import Settings, State, run_cycle

OVERLAP = timedelta(hours=3)


def row(hour: int, minute: int = 0) -> dict:
    return {"date_heure": datetime(2026, 11, 11, hour, minute, tzinfo=UTC).isoformat(), "consommation": 47000}


class FakeSource:
    def __init__(self, updated_at: str, rows: list[dict]) -> None:
        self._updated_at = updated_at
        self._rows = rows
        self.since_calls: list[datetime | None] = []

    def updated_at(self) -> str:
        return self._updated_at

    def measured_rows(self, since=None):
        self.since_calls.append(since)
        return [r for r in self._rows if since is None or datetime.fromisoformat(r["date_heure"]) >= since]


class Recorder:
    def __init__(self, fail: bool = False) -> None:
        self.messages: list[tuple[bytes, bytes]] = []
        self.fail = fail

    def __call__(self, messages):
        if self.fail:
            raise KafkaPublishError("broker down")
        self.messages.extend(messages)


def test_first_cycle_publishes_the_whole_dataset():
    source = FakeSource("v1", [row(15), row(16)])
    publish = Recorder()

    state, published = run_cycle(source, publish, State(), OVERLAP)

    assert source.since_calls == [None]
    assert published == 2
    assert [key for key, _ in publish.messages] == [b"2026-11-11T15:00:00+00:00", b"2026-11-11T16:00:00+00:00"]
    assert json.loads(publish.messages[0][1])["source_updated_at"] == "v1"
    assert state == State(cursor=datetime(2026, 11, 11, 16, tzinfo=UTC), source_updated_at="v1")


def test_unchanged_dataset_reads_no_rows():
    source = FakeSource("v1", [row(16)])
    state = State(cursor=datetime(2026, 11, 11, 16, tzinfo=UTC), source_updated_at="v1")

    new_state, published = run_cycle(source, Recorder(), state, OVERLAP)

    assert (new_state, published) == (state, 0)
    assert source.since_calls == []


def test_refresh_republishes_the_overlap_window():
    rows = [row(h) for h in range(10, 17)] + [row(16, 15)]
    source = FakeSource("v2", rows)
    publish = Recorder()
    state = State(cursor=datetime(2026, 11, 11, 16, tzinfo=UTC), source_updated_at="v1")

    new_state, published = run_cycle(source, publish, state, OVERLAP)

    assert source.since_calls == [datetime(2026, 11, 11, 13, tzinfo=UTC)]
    # 13:00 to 16:00 again (possible revisions), then the new 16:15.
    assert published == 5
    assert new_state.cursor == datetime(2026, 11, 11, 16, 15, tzinfo=UTC)


def test_cursor_never_moves_back():
    # The source lost its latest rows (should not happen, but must not rewind the producer).
    source = FakeSource("v2", [row(14)])
    state = State(cursor=datetime(2026, 11, 11, 16, tzinfo=UTC), source_updated_at="v1")

    new_state, _ = run_cycle(source, Recorder(), state, OVERLAP)

    assert new_state.cursor == state.cursor


def test_failed_publication_keeps_the_state():
    state = State(cursor=datetime(2026, 11, 11, 15, tzinfo=UTC), source_updated_at="v1")
    with pytest.raises(KafkaPublishError):
        run_cycle(FakeSource("v2", [row(16)]), Recorder(fail=True), state, OVERLAP)


def test_settings_defaults_and_environment(monkeypatch):
    assert Settings.from_env() == Settings()
    monkeypatch.setenv("KAFKA_TOPIC", "other")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "60")
    monkeypatch.setenv("OVERLAP_HOURS", "1")
    monkeypatch.setenv("HEARTBEAT_FILE", "/tmp/hb")

    settings = Settings.from_env()

    assert settings.topic == "other"
    assert settings.poll_interval == timedelta(seconds=60)
    assert settings.overlap == timedelta(hours=1)
    assert settings.heartbeat_file == Path("/tmp/hb")
    assert settings.bootstrap_servers == "wattlake-kafka-bootstrap:9092"
