"""Bronze layer: the raw eCO2mix history in an Iceberg table.

Each load replaces one period of the table with a single atomic overwrite, so
loading the same period twice gives the same result. Values are kept as the
source publishes them, including the numeric fields typed as text: cleaning
belongs to the silver layer (see docs/sources.md).

The table is not partitioned. The history is about 500,000 rows, a few tens of
MB of Parquet, and each month is written to its own files: Iceberg's per-file
min/max statistics on date_heure already let overwrites and filtered queries
skip the other months. Partitioning will be reevaluated with the real-time
ingestion of step 5, which adds a row every quarter hour; Iceberg can evolve
the partition spec later without rewriting existing data.
"""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.table import Table

# Defined in lakehouse.py, which the DAGs import at parse time, and re-exported
# here for the callers of this module.
from wattlake_ingestion.lakehouse import BRONZE_NAMESPACE, BRONZE_TABLE

_TEXT_FIELDS = ["perimetre", "nature", "date", "heure"]
# Numeric fields that the source declares as text (see docs/sources.md).
_TEXT_TYPED_NUMBERS = {"ech_comm_allemagne_belgique", "gaz_cogen"}
_NUMERIC_FIELDS = [
    "consommation",
    "prevision_j1",
    "prevision_j",
    "fioul",
    "charbon",
    "gaz",
    "nucleaire",
    "eolien",
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
]

# Columns of the eco2mix-national-cons-def Parquet export, with their types.
# Iceberg stores timestamps in microseconds: the source's milliseconds are cast.
SOURCE_SCHEMA = pa.schema(
    [(name, pa.string()) for name in _TEXT_FIELDS]
    + [("date_heure", pa.timestamp("us", tz="UTC"))]
    + [(name, pa.string() if name in _TEXT_TYPED_NUMBERS else pa.int64()) for name in _NUMERIC_FIELDS]
)
# Technical columns added at load time.
BRONZE_SCHEMA = pa.schema(
    list(SOURCE_SCHEMA) + [("_source_dataset", pa.string()), ("_ingested_at", pa.timestamp("us", tz="UTC"))]
)


class BronzeSchemaError(ValueError):
    """The source data does not have the expected columns."""


def prepare_batch(rows: pa.Table, source_dataset: str, ingested_at: datetime) -> pa.Table:
    """Return the source rows in the bronze schema, with the technical columns.

    Fails on a missing or unexpected column, so that a schema change at the
    source stops the load instead of silently dropping or adding data.
    """
    expected, actual = set(SOURCE_SCHEMA.names), set(rows.column_names)
    if expected != actual:
        raise BronzeSchemaError(
            f"missing columns: {sorted(expected - actual)}, unexpected columns: {sorted(actual - expected)}"
        )
    batch = rows.select(SOURCE_SCHEMA.names).cast(SOURCE_SCHEMA)
    batch = batch.append_column(BRONZE_SCHEMA.field("_source_dataset"), pa.repeat(source_dataset, batch.num_rows))
    return batch.append_column(
        BRONZE_SCHEMA.field("_ingested_at"),
        pa.repeat(pa.scalar(ingested_at, type=pa.timestamp("us", tz="UTC")), batch.num_rows),
    )


def recent_months(reference: datetime, count: int) -> list[tuple[datetime, datetime]]:
    """Return the count full calendar months before reference's month, oldest first.

    Months are in reference's timezone (Paris for RTE), as [start, end) pairs:
    the month of reference itself is excluded, the history never contains it.
    """
    if reference.tzinfo is None:
        raise ValueError("reference must be a timezone-aware datetime")
    if count < 1:
        raise ValueError(f"count must be positive, got {count}")
    tz = reference.tzinfo
    year, month = reference.year, reference.month
    months = []
    for _ in range(count):
        end = datetime(year, month, 1, tzinfo=tz)
        year, month = (year - 1, 12) if month == 1 else (year, month - 1)
        months.append((datetime(year, month, 1, tzinfo=tz), end))
    return months[::-1]


def load_period(
    catalog: Catalog,
    batch: pa.Table,
    start: datetime,
    end: datetime,
    *,
    skip_unchanged: bool = False,
) -> int:
    """Replace the rows of [start, end) in the bronze table with batch, in one commit.

    An empty batch writes nothing: an empty answer from the source must never
    erase a period that is already loaded. With skip_unchanged, a batch whose
    source columns match the rows already loaded writes nothing either, so that
    a periodic reload does not add a Nessie commit, and a dbt run, for nothing.
    Returns the number of rows written.
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware datetimes")
    if start >= end:
        raise ValueError(f"start ({start}) must be before end ({end})")
    if batch.num_rows == 0:
        return 0

    in_period = pc.and_(
        pc.greater_equal(batch["date_heure"], pa.scalar(start, type=pa.timestamp("us", tz="UTC"))),
        pc.less(batch["date_heure"], pa.scalar(end, type=pa.timestamp("us", tz="UTC"))),
    )
    if batch["date_heure"].null_count or not pc.all(in_period).as_py():
        raise ValueError(f"batch has rows outside [{start}, {end}) or without date_heure")

    table = _bronze_table(catalog)
    period = And(
        GreaterThanOrEqual("date_heure", start.astimezone(UTC).isoformat()),
        LessThan("date_heure", end.astimezone(UTC).isoformat()),
    )
    if skip_unchanged and _same_source_rows(table.scan(row_filter=period).to_arrow(), batch):
        return 0
    table.overwrite(batch, overwrite_filter=period)
    return batch.num_rows


def load_export_file(
    catalog: Catalog,
    export: Path,
    source_dataset: str,
    start: datetime,
    end: datetime,
    ingested_at: datetime,
    *,
    skip_unchanged: bool = False,
) -> int:
    """Load a Parquet export of [start, end) into the bronze table. Returns the rows written."""
    rows = pq.read_table(export)
    if rows.num_rows == 0:
        # Checked before the schema: an empty export may not list every column.
        return 0
    batch = prepare_batch(rows, source_dataset, ingested_at)
    return load_period(catalog, batch, start, end, skip_unchanged=skip_unchanged)


def _same_source_rows(loaded: pa.Table, batch: pa.Table) -> bool:
    """Whether two tables hold the same source rows, ignoring order and technical columns.

    The rows are compared as multisets: the source has duplicate date_heure
    values (spring DST artefacts, see docs/sources.md), so every column is a
    sort key. A month is about 3,000 rows: reading it back costs little.
    """
    if loaded.num_rows != batch.num_rows:
        return False
    keys = [(name, "ascending") for name in SOURCE_SCHEMA.names]
    # The scan may return other Arrow types (large_string): cast before comparing.
    loaded = loaded.select(SOURCE_SCHEMA.names).cast(SOURCE_SCHEMA).sort_by(keys)
    batch = batch.select(SOURCE_SCHEMA.names).cast(SOURCE_SCHEMA).sort_by(keys)
    return loaded.equals(batch)


def _bronze_table(catalog: Catalog) -> Table:
    catalog.create_namespace_if_not_exists(BRONZE_NAMESPACE)
    try:
        return catalog.load_table(BRONZE_TABLE)
    except NoSuchTableError:
        # Unpartitioned on purpose: see the module docstring.
        return catalog.create_table(BRONZE_TABLE, schema=BRONZE_SCHEMA)
