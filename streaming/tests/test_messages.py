import json
from datetime import UTC, datetime

import pytest

from wattlake_streaming.messages import encode, key_time, row_time

ROW = {"date_heure": "2026-11-11T16:00:00+00:00", "consommation": 47025, "hydraulique_fil_eau_eclusee": "3492"}


def test_encode_keys_by_quarter_hour_and_wraps_the_raw_row():
    key, value = encode(ROW, dataset="eco2mix-national-tr", source_updated_at="2026-11-11T16:15:07+00:00")

    assert key == b"2026-11-11T16:00:00+00:00"
    assert json.loads(value) == {
        "dataset": "eco2mix-national-tr",
        "source_updated_at": "2026-11-11T16:15:07+00:00",
        # Unchanged, text-typed numbers included: cleaning belongs to silver.
        "record": ROW,
    }


def test_encode_keeps_accents_readable():
    _, value = encode(
        {"date_heure": "2026-11-11T16:00:00+00:00", "nature": "Données temps réel"}, dataset="d", source_updated_at="s"
    )
    assert "Données temps réel".encode() in value


def test_encode_rejects_row_without_time():
    with pytest.raises(ValueError, match="date_heure"):
        encode({"consommation": 1}, dataset="d", source_updated_at="s")


def test_key_and_row_times_are_timezone_aware():
    expected = datetime(2026, 11, 11, 16, 0, tzinfo=UTC)
    assert row_time(ROW) == expected
    assert key_time(b"2026-11-11T16:00:00+00:00") == expected


def test_naive_time_is_rejected():
    with pytest.raises(ValueError, match="time zone"):
        key_time(b"2026-11-11T16:00:00")
