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

BRONZE_NAMESPACE = "bronze"
BRONZE_TABLE = f"{BRONZE_NAMESPACE}.eco2mix_national_history"

_TEXT_FIELDS = ["perimetre", "nature", "date", "heure"]
# Numeric fields that the source declares as text (see docs/sources.md).
_TEXT_TYPED_NUMBERS = {"ech_comm_allemagne_belgique", "gaz_cogen"}
_NUMERIC_FIELDS = [
    "consommation", "prevision_j1", "prevision_j",
    "fioul", "charbon", "gaz", "nucleaire", "eolien", "solaire", "hydraulique",
    "pompage", "bioenergies", "ech_physiques", "taux_co2",
    "ech_comm_angleterre", "ech_comm_espagne", "ech_comm_italie", "ech_comm_suisse",
    "ech_comm_allemagne_belgique",
    "fioul_tac", "fioul_cogen", "fioul_autres",
    "gaz_tac", "gaz_cogen", "gaz_ccg", "gaz_autres",
    "hydraulique_fil_eau_eclusee", "hydraulique_lacs", "hydraulique_step_turbinage",
    "bioenergies_dechets", "bioenergies_biomasse", "bioenergies_biogaz",
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


def load_period(catalog: Catalog, batch: pa.Table, start: datetime, end: datetime) -> int:
    """Replace the rows of [start, end) in the bronze table with batch, in one commit.

    An empty batch writes nothing: an empty answer from the source must never
    erase a period that is already loaded. Returns the number of rows written.
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
    table.overwrite(
        batch,
        overwrite_filter=And(
            GreaterThanOrEqual("date_heure", start.astimezone(UTC).isoformat()),
            LessThan("date_heure", end.astimezone(UTC).isoformat()),
        ),
    )
    return batch.num_rows


def load_export_file(
    catalog: Catalog,
    export: Path,
    source_dataset: str,
    start: datetime,
    end: datetime,
    ingested_at: datetime,
) -> int:
    """Load a Parquet export of [start, end) into the bronze table. Returns the rows written."""
    rows = pq.read_table(export)
    if rows.num_rows == 0:
        # Checked before the schema: an empty export may not list every column.
        return 0
    return load_period(catalog, prepare_batch(rows, source_dataset, ingested_at), start, end)


def _bronze_table(catalog: Catalog) -> Table:
    catalog.create_namespace_if_not_exists(BRONZE_NAMESPACE)
    try:
        return catalog.load_table(BRONZE_TABLE)
    except NoSuchTableError:
        # Unpartitioned on purpose: see the module docstring.
        return catalog.create_table(BRONZE_TABLE, schema=BRONZE_SCHEMA)
