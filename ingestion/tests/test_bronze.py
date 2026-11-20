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
    recent_months,
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

        expected = r"missing columns: \['taux_co2'\], unexpected columns: \['new_field'\]"
        with pytest.raises(BronzeSchemaError, match=expected):
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


class TestSkipUnchanged:
    def test_an_identical_period_writes_nothing(self, catalog: Catalog) -> None:
        load_period(catalog, prepare_batch(export_rows(AUGUST[0], 48), DATASET, INGESTED_AT), *AUGUST)
        snapshot = catalog.load_table(BRONZE_TABLE).current_snapshot()

        # Same source rows, in another order and loaded later.
        rows = export_rows(AUGUST[0], 48)
        again = prepare_batch(rows.take(list(range(47, -1, -1))), DATASET, INGESTED_AT + timedelta(days=7))

        assert load_period(catalog, again, *AUGUST, skip_unchanged=True) == 0
        assert catalog.load_table(BRONZE_TABLE).current_snapshot() == snapshot

    def test_a_revised_period_is_replaced(self, catalog: Catalog) -> None:
        load_period(catalog, prepare_batch(export_rows(AUGUST[0], 48, consommation=1), DATASET, INGESTED_AT), *AUGUST)

        revised = prepare_batch(export_rows(AUGUST[0], 48, consommation=2), DATASET, INGESTED_AT)

        assert load_period(catalog, revised, *AUGUST, skip_unchanged=True) == 48
        assert set(bronze_rows(catalog)["consommation"].to_pylist()) == {2}

    def test_a_nature_change_alone_is_a_revision(self, catalog: Catalog) -> None:
        load_period(catalog, prepare_batch(export_rows(AUGUST[0], 4), DATASET, INGESTED_AT), *AUGUST)

        rows = export_rows(AUGUST[0], 4)
        final = rows.set_column(rows.schema.get_field_index("nature"), "nature", pa.array(["Données définitives"] * 4))

        assert load_period(catalog, prepare_batch(final, DATASET, INGESTED_AT), *AUGUST, skip_unchanged=True) == 4
        assert set(bronze_rows(catalog)["nature"].to_pylist()) == {"Données définitives"}

    def test_a_duplicated_row_is_a_revision(self, catalog: Catalog) -> None:
        # Same set of rows but not the same multiset: duplicates count.
        rows = export_rows(AUGUST[0], 4)
        load_period(catalog, prepare_batch(rows, DATASET, INGESTED_AT), *AUGUST)

        doubled = pa.concat_tables([rows.slice(0, 3), rows.slice(0, 1)])

        assert load_period(catalog, prepare_batch(doubled, DATASET, INGESTED_AT), *AUGUST, skip_unchanged=True) == 4

    def test_a_month_not_loaded_yet_is_written(self, catalog: Catalog) -> None:
        load_period(catalog, prepare_batch(export_rows(JULY[0], 10), DATASET, INGESTED_AT), *JULY)

        batch = prepare_batch(export_rows(AUGUST[0], 10), DATASET, INGESTED_AT)

        assert load_period(catalog, batch, *AUGUST, skip_unchanged=True) == 10
        assert bronze_rows(catalog).num_rows == 20


class TestRecentMonths:
    def test_returns_the_full_months_before_the_reference_oldest_first(self) -> None:
        months = recent_months(datetime(2026, 11, 22, 4, tzinfo=PARIS), 3)

        assert months == [
            (datetime(2026, 8, 1, tzinfo=PARIS), datetime(2026, 9, 1, tzinfo=PARIS)),
            (datetime(2026, 9, 1, tzinfo=PARIS), datetime(2026, 10, 1, tzinfo=PARIS)),
            (datetime(2026, 10, 1, tzinfo=PARIS), datetime(2026, 11, 1, tzinfo=PARIS)),
        ]

    def test_crosses_years_and_follows_the_reference_timezone(self) -> None:
        months = recent_months(datetime(2026, 1, 1, 0, 30, tzinfo=PARIS), 24)

        assert len(months) == 24
        assert months[0] == (datetime(2024, 1, 1, tzinfo=PARIS), datetime(2024, 2, 1, tzinfo=PARIS))
        assert months[-1] == (datetime(2025, 12, 1, tzinfo=PARIS), datetime(2026, 1, 1, tzinfo=PARIS))
        # December 2025 starts at 23:00 UTC the day before, as RTE's months do.
        assert months[-1][0].astimezone(UTC) == datetime(2025, 11, 30, 23, tzinfo=UTC)

    def test_rejects_naive_references_and_empty_windows(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            recent_months(datetime(2026, 11, 22), 3)
        with pytest.raises(ValueError, match="positive"):
            recent_months(datetime(2026, 11, 22, tzinfo=PARIS), 0)


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
