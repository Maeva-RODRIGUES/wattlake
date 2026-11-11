import os

from wattlake_streaming.health import is_alive


def test_recent_heartbeat_is_alive(tmp_path):
    heartbeat = tmp_path / "heartbeat"
    heartbeat.touch()
    os.utime(heartbeat, (1000, 1000))
    assert is_alive(heartbeat, max_age_seconds=900, now=1900)


def test_stale_heartbeat_is_not_alive(tmp_path):
    heartbeat = tmp_path / "heartbeat"
    heartbeat.touch()
    os.utime(heartbeat, (1000, 1000))
    assert not is_alive(heartbeat, max_age_seconds=900, now=1901)


def test_missing_heartbeat_is_not_alive(tmp_path):
    assert not is_alive(tmp_path / "heartbeat", max_age_seconds=900, now=0)
