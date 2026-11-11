"""Format of the Kafka messages published by the producer.

Key: the quarter-hour of the row, its date_heure as published by the API
("2026-11-11T16:00:00+00:00", UTC). Each quarter-hour always gets the same key:
the topic is compacted, so Kafka keeps only its latest version.

Value, JSON encoded in UTF-8:
    {
        "dataset": "eco2mix-national-tr",
        "source_updated_at": "2026-11-11T16:15:07+00:00",
        "record": {...}
    }
record is the row exactly as returned by the API, field names and types
included (some numeric fields are typed as text upstream, see docs/sources.md):
cleaning belongs to the silver layer. source_updated_at is the dataset's
data_processed when the row was read: it orders the versions of a quarter-hour
that RTE revised.
"""

import json
from datetime import datetime

from wattlake_streaming.odre import Row

TIME_FIELD = "date_heure"


def encode(row: Row, *, dataset: str, source_updated_at: str) -> tuple[bytes, bytes]:
    """Return the key and value of the message carrying row."""
    if not row.get(TIME_FIELD):
        raise ValueError(f"row without {TIME_FIELD}: {row}")
    value = {"dataset": dataset, "source_updated_at": source_updated_at, "record": row}
    return row[TIME_FIELD].encode(), json.dumps(value, ensure_ascii=False).encode()


def row_time(row: Row) -> datetime:
    """Return the quarter-hour of row as a timezone-aware datetime."""
    return parse_time(row[TIME_FIELD])


def key_time(key: bytes) -> datetime:
    """Return the quarter-hour of a message key."""
    return parse_time(key.decode())


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"{TIME_FIELD} without time zone: {value}")
    return parsed
