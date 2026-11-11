"""Bronze table of the real-time stream: one row per Kafka message, append only.

Every version of a quarter-hour that the producer published is kept, with the
dataset's refresh time and the Kafka offset it came from: the silver layer
picks the latest version. Values are kept as the source publishes them, the
numeric field typed as text included (see docs/sources.md).

Exactly once: each append records, in the summary of its Iceberg snapshot, the
next Kafka offset to read for every partition. Data and offsets are committed
together or not at all, and the writer resumes from them (next_offsets).

The table is not partitioned: about 35,000 rows a year, like the history table
(see ingestion/src/wattlake_ingestion/bronze.py).
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from wattlake_streaming.messages import parse_time

NAMESPACE = "bronze"
TABLE = f"{NAMESPACE}.eco2mix_national_realtime"

# Snapshot summary keys written by the writer.
TOPIC_PROPERTY = "wattlake.kafka.topic"
# JSON object: partition number (as text) -> next offset to read.
OFFSETS_PROPERTY = "wattlake.kafka.next-offsets"

_TEXT_FIELDS = ["perimetre", "nature", "date", "heure"]
# Numeric field that the real-time dataset declares as text (see docs/sources.md).
_TEXT_TYPED_NUMBERS = {"hydraulique_fil_eau_eclusee"}
# In the order of the API; the last four only exist in the real-time dataset.
_NUMERIC_FIELDS = [
    "consommation",
    "prevision_j1",
    "prevision_j",
    "fioul",
    "charbon",
    "gaz",
    "nucleaire",
    "eolien",
    "eolien_terrestre",
    "eolien_offshore",
    "solaire",
    "hydraulique",
    "pompage",
    "bioenergies",
    "ech_physiques",
    "taux_co2",
    "ech_comm_angleterre",
    "ech_comm_espagne",
    "ech_comm_italie",
    "ech_comm_suisse",
    "ech_comm_allemagne_belgique",
    "fioul_tac",
    "fioul_cogen",
    "fioul_autres",
    "gaz_tac",
    "gaz_cogen",
    "gaz_ccg",
    "gaz_autres",
    "hydraulique_fil_eau_eclusee",
    "hydraulique_lacs",
    "hydraulique_step_turbinage",
    "bioenergies_dechets",
    "bioenergies_biomasse",
    "bioenergies_biogaz",
    "stockage_batterie",
    "destockage_batterie",
]

_TIMESTAMP = pa.timestamp("us", tz="UTC")

# Columns of the API rows (the "record" of each message), with their types.
SOURCE_SCHEMA = pa.schema(
    [(name, pa.string()) for name in _TEXT_FIELDS]
    + [("date_heure", _TIMESTAMP)]
    + [(name, pa.string() if name in _TEXT_TYPED_NUMBERS else pa.int64()) for name in _NUMERIC_FIELDS]
)
# Technical columns: where each row comes from.
BRONZE_SCHEMA = pa.schema(
    list(SOURCE_SCHEMA)
    + [
        ("_source_dataset", pa.string()),
        # data_processed of the dataset when the producer read the row.
        ("_source_updated_at", _TIMESTAMP),
        ("_kafka_partition", pa.int32()),
        ("_kafka_offset", pa.int64()),
        ("_ingested_at", _TIMESTAMP),
    ]
)


class BronzeSchemaError(ValueError):
    """A message does not have the expected format."""


@dataclass(frozen=True)
class KafkaRecord:
    """A consumed Kafka message, reduced to what the bronze table needs."""

    partition: int
    offset: int
    value: bytes


def to_row(record: KafkaRecord, ingested_at: datetime) -> dict:
    """Return the bronze row of one message (format: messages.py).

    Fails on a missing or unexpected field, so that a format change at the
    source stops the writer instead of silently dropping or adding data.
    """
    try:
        envelope = json.loads(record.value)
        row = envelope["record"]
        dataset = envelope["dataset"]
        updated_at = envelope["source_updated_at"]
    except (ValueError, KeyError, TypeError) as exc:
        raise BronzeSchemaError(f"offset {record.offset}: not a producer message: {exc}") from exc

    expected, actual = set(SOURCE_SCHEMA.names), set(row)
    if expected != actual:
        raise BronzeSchemaError(
            f"offset {record.offset}: missing fields: {sorted(expected - actual)}, "
            f"unexpected fields: {sorted(actual - expected)}"
        )
    return {
        **row,
        "date_heure": parse_time(row["date_heure"]),
        "_source_dataset": dataset,
        "_source_updated_at": parse_time(updated_at),
        "_kafka_partition": record.partition,
        "_kafka_offset": record.offset,
        "_ingested_at": ingested_at,
    }


def to_batch(records: Iterable[KafkaRecord], ingested_at: datetime) -> pa.Table:
    """Return the messages as an Arrow table in the bronze schema."""
    return pa.Table.from_pylist([to_row(record, ingested_at) for record in records], schema=BRONZE_SCHEMA)


def ensure_table(catalog: Catalog, identifier: str = TABLE) -> Table:
    """Return the bronze table, creating it (and its namespace) on the first run."""
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return catalog.create_table_if_not_exists(identifier, schema=BRONZE_SCHEMA)


def append(table: Table, batch: pa.Table, topic: str, next_offsets: dict[int, int]) -> None:
    """Append batch and record next_offsets in the same snapshot."""
    table.append(
        batch,
        snapshot_properties={
            TOPIC_PROPERTY: topic,
            OFFSETS_PROPERTY: json.dumps({str(p): o for p, o in sorted(next_offsets.items())}),
        },
    )


def next_offsets(table: Table, topic: str) -> dict[int, int]:
    """Return the next offset to read for each partition of topic, {} for an empty table.

    Walks back from the current snapshot to the latest one written by the
    writer: a maintenance operation (compaction, for example) adds snapshots
    without these properties. Through Nessie, a table only exposes its current
    snapshot (the history is in the Nessie commits), so the walk usually stops
    there. When no snapshot carries the offsets, they are read from the data
    itself (_kafka_offset), which is slower but always right.
    """
    snapshot = table.current_snapshot()
    while snapshot is not None:
        summary = snapshot.summary
        if summary is not None and summary[TOPIC_PROPERTY] == topic and summary[OFFSETS_PROPERTY]:
            return {int(p): int(o) for p, o in json.loads(summary[OFFSETS_PROPERTY]).items()}
        parent = snapshot.parent_snapshot_id
        snapshot = table.snapshot_by_id(parent) if parent is not None else None

    if table.current_snapshot() is None:
        return {}
    data = table.scan(selected_fields=("_kafka_partition", "_kafka_offset")).to_arrow()
    if data.num_rows == 0:
        return {}
    grouped = data.group_by("_kafka_partition").aggregate([("_kafka_offset", "max")])
    partitions = grouped["_kafka_partition"].to_pylist()
    offsets = pc.add(grouped["_kafka_offset_max"], 1).to_pylist()
    return dict(zip(partitions, offsets, strict=True))
