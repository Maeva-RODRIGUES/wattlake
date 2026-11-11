import json

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from wattlake_streaming.bronze import SOURCE_SCHEMA, KafkaRecord


@pytest.fixture
def catalog(tmp_path):
    """A local Iceberg catalog: SQLite for the metadata, files in tmp_path."""
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )


def api_row(date_heure: str = "2026-11-11T16:00:00+00:00", consommation: int = 47025) -> dict:
    """A measured row as the real-time API returns it (41 fields)."""
    row = {name: 0 for name in SOURCE_SCHEMA.names}
    row.update(
        perimetre="France",
        nature="Données temps réel",
        date="2026-11-11",
        heure="17:00",
        date_heure=date_heure,
        consommation=consommation,
        hydraulique_fil_eau_eclusee="3492",
    )
    return row


def message(offset: int, row: dict | None = None, partition: int = 0, updated_at: str = "2026-11-11T16:15:07+00:00"):
    value = {"dataset": "eco2mix-national-tr", "source_updated_at": updated_at, "record": row or api_row()}
    return KafkaRecord(partition=partition, offset=offset, value=json.dumps(value).encode())
