from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog

from wattlake_ingestion.bronze import (
    BRONZE_SCHEMA,
    BRONZE_TABLE,
    SOURCE_SCHEMA,
    BronzeSchemaError,
    load_export_file,
    load_period,
    prepare_batch,
)

PARIS = ZoneInfo("Europe/Paris")
AUGUST = (datetime(2026, 8, 1, tzinfo=PARIS), datetime(2026, 9, 1, tzinfo=PARIS))
JULY = (datetime(2026, 7, 1, tzinfo=PARIS), datetime(2026, 8, 1, tzinfo=PARIS))
INGESTED_AT = datetime(2026, 11, 5, 12, tzinfo=UTC)
DATASET = "eco2mix-national-cons-def"

# The export's own types: timestamps in milliseconds, as ODRE publishes them.
EXPORT_SCHEMA = SOURCE_SCHEMA.set(
    SOURCE_SCHEMA.get_field_index("date_heure"), pa.field("date_heure", pa.timestamp("ms", tz="UTC"))
)


def export_rows(start: datetime, count: int, consommation: int = 50_000) -> pa.Table:
    """Rows shaped like an ODRE export, one per half hour from start."""
    rows = []
    for i in range(count):
        row = {name: None for name in EXPORT_SCHEMA.names}
        row |= {
            "perimetre": "France",
            "nature": "Données consolidées",
            "date_heure": start + timedelta(minutes=30 * i),
            "consommation": consommation,
            "gaz_cogen": "-3170",
        }
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=EXPORT_SCHEMA)


@pytest.fixture
def catalog(tmp_path: Path) -> Iterator[Catalog]:
    """A local Iceberg catalog: SQLite for the catalog, the temporary directory for the files."""
    yield SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}/warehouse")


def bronze_rows(catalog: Catalog) -> pa.Table:
    return catalog.load_table(BRONZE_TABLE).scan().to_arrow()


class TestPrepareBatch:
    def test_casts_timestamps_and_adds_technical_columns(self) -> None:
        batch = prepare_batch(export_rows(AUGUST[0], 2), DATASET, INGESTED_AT)

        assert batch.schema == BRONZE_SCHEMA
        assert batch["date_heure"].type == pa.timestamp("us", tz="UTC")
        assert batch["_source_dataset"].to_pylist() == [DATASET, DATASET]
        assert batch["_ingested_at"].to_pylist() == [INGESTED_AT, INGESTED_AT]
        # Text-typed numbers stay as the source publishes them.
        assert batch["gaz_cogen"].to_pylist() == ["-3170", "-3170"]

    def test_unexpected_or_missing_columns_are_rejected(self) -> None:
        rows = export_rows(AUGUST[0], 1).drop_columns(["taux_co2"]).append_column("new_field", pa.array([1]))

        with pytest.raises(BronzeSchemaError, match=r"missing columns: \['taux_co2'\], unexpected columns: \['new_field'\]"):
            prepare_batch(rows, DATASET, INGESTED_AT)


class TestLoadPeriod:
    def test_creates_an_unpartitioned_table_with_the_bronze_schema(self, catalog: Catalog) -> None:
        written = load_period(catalog, prepare_batch(export_rows(AUGUST[0], 48), DATASET, INGESTED_AT), *AUGUST)

        table = catalog.load_table(BRONZE_TABLE)
        assert written == 48
        assert table.scan().to_arrow().num_rows == 48
        assert table.spec().is_unpartitioned()
        assert [field.name for field in table.schema().fields] == BRONZE_SCHEMA.names

    def test_loading_a_period_twice_gives_the_same_rows(self, catalog: Catalog) -> None:
        batch = prepare_batch(export_rows(AUGUST[0], 48), DATASET, INGESTED_AT)

        load_period(catalog, batch, *AUGUST)
        load_period(catalog, batch, *AUGUST)

        assert bronze_rows(catalog).num_rows == 48

    def test_reloading_a_period_replaces_it_and_keeps_the_others(self, catalog: Catalog) -> None:
        load_period(catalog, prepare_batch(export_rows(JULY[0], 10, consommation=1), DATASET, INGESTED_AT), *JULY)
        load_period(catalog, prepare_batch(export_rows(AUGUST[0], 10, consommation=2), DATASET, INGESTED_AT), *AUGUST)
        # August is published again with new values (e.g. consolidated data).
        load_period(catalog, prepare_batch(export_rows(AUGUST[0], 12, consommation=3), DATASET, INGESTED_AT), *AUGUST)

        rows = bronze_rows(catalog)
        by_value = pc.value_counts(rows["consommation"]).to_pylist()
        assert sorted((v["values"], v["counts"]) for v in by_value) == [(1, 10), (3, 12)]

    def test_an_empty_batch_erases_nothing(self, catalog: Catalog) -> None:
        load_period(catalog, prepare_batch(export_rows(AUGUST[0], 48), DATASET, INGESTED_AT), *AUGUST)

        empty = prepare_batch(export_rows(AUGUST[0], 0), DATASET, INGESTED_AT)
        assert load_period(catalog, empty, *AUGUST) == 0

        assert bronze_rows(catalog).num_rows == 48

    def test_rows_outside_the_period_are_rejected(self, catalog: Catalog) -> None:
        # Starts one hour before August, Paris time.
        batch = prepare_batch(export_rows(AUGUST[0] - timedelta(hours=1), 4), DATASET, INGESTED_AT)

        with pytest.raises(ValueError, match="outside"):
            load_period(catalog, batch, *AUGUST)

    def test_naive_bounds_are_rejected(self, catalog: Catalog) -> None:
        batch = prepare_batch(export_rows(AUGUST[0], 1), DATASET, INGESTED_AT)

        with pytest.raises(ValueError, match="timezone-aware"):
            load_period(catalog, batch, datetime(2026, 8, 1), datetime(2026, 9, 1))


class TestLoadExportFile:
    def test_loads_a_parquet_export(self, catalog: Catalog, tmp_path: Path) -> None:
        export = tmp_path / "export.parquet"
        pq.write_table(export_rows(AUGUST[0], 48), export)

        assert load_export_file(catalog, export, DATASET, *AUGUST, INGESTED_AT) == 48
        assert bronze_rows(catalog).num_rows == 48

    def test_an_empty_export_writes_nothing(self, catalog: Catalog, tmp_path: Path) -> None:
        export = tmp_path / "export.parquet"
        # An empty export may not even list the columns.
        pq.write_table(pa.table({}), export)

        assert load_export_file(catalog, export, DATASET, *AUGUST, INGESTED_AT) == 0
        assert not catalog.table_exists(BRONZE_TABLE)
