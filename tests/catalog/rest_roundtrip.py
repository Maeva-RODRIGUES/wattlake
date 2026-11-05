"""PyIceberg round trip through the Nessie Iceberg REST catalog (see ADR 004).

Runs inside the cluster, in the throwaway pod started by tests/catalog/run.sh:
creates a table, appends rows, replaces the rows of one source with a filtered
overwrite (the operation the bronze DAG relies on), then reads the table back.
The table is kept after the run so Trino and Nessie can be checked against it.
"""

import os
import sys
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

NAMESPACE = "rest_catalog_test"
TABLE = f"{NAMESPACE}.readings"
TS = datetime(2026, 1, 1, 12, tzinfo=UTC)

SCHEMA = pa.schema(
    [
        ("ts", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
        ("mw", pa.float64()),
    ]
)
INITIAL_ROWS = [
    {"ts": TS, "source": "wind", "mw": 4200.5},
    {"ts": TS, "source": "solar", "mw": 1830.0},
    {"ts": TS, "source": "nuclear", "mw": 38750.25},
]
WIND_REPLACEMENT = [{"ts": TS, "source": "wind", "mw": 5000.0}]
EXPECTED_ROWS = 3
EXPECTED_MW = 5000.0 + 1830.0 + 38750.25


def check(condition: bool, message: str) -> None:
    if not condition:
        sys.exit(f"FAIL: {message}")
    print(f"OK: {message}")


catalog = load_catalog(
    "nessie",
    type="rest",
    uri=os.environ["NESSIE_ICEBERG_URI"],
    # The client's own S3 keys: Nessie neither signs requests nor vends credentials.
    # Nessie's table config makes PyIceberg use FsspecFileIO (s3fs), see ADR 004.
    **{
        "s3.access-key-id": os.environ["S3_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["S3_SECRET_ACCESS_KEY"],
    },
)

catalog.create_namespace_if_not_exists(NAMESPACE)
# Start from a fresh table so that a previous run cannot make this one pass.
if catalog.table_exists(TABLE):
    catalog.drop_table(TABLE)
table = catalog.create_table(TABLE, schema=SCHEMA)
print(f"OK: table {TABLE} created at {table.location()}")

table.append(pa.Table.from_pylist(INITIAL_ROWS, schema=SCHEMA))
table.overwrite(pa.Table.from_pylist(WIND_REPLACEMENT, schema=SCHEMA), overwrite_filter=EqualTo("source", "wind"))

rows = table.scan().to_arrow()
mw = pc.sum(rows["mw"]).as_py()
check(rows.num_rows == EXPECTED_ROWS and mw == EXPECTED_MW, f"{rows.num_rows} rows, {mw} MW read back")
check(table.location().startswith("s3://warehouse/"), "table stored in s3://warehouse")
# Nessie only returns the snapshot of its current commit: the table history is
# checked as Nessie commits by run.sh, not as Iceberg snapshots.
