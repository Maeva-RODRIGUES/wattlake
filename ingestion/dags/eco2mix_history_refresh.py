"""Reload the last 24 months of the eCO2mix history into bronze, every week.

RTE publishes each month several times (docs/sources.md): consolidated data
about two to three months later, then final data in the second half of the
following year. eco2mix_history_to_bronze loads each month once, at the start
of the next month: too early for the consolidated data, so the month ends as
skipped and is never retried, and a later revision is never picked up. This
DAG catches up both (docs/adr/015-rechargement-historique.md).

- One mapped task per month, from the oldest. Each one downloads its month and
  replaces it in bronze.eco2mix_national_history only if the source rows have
  changed: an unchanged or empty month ends as skipped, without a Nessie commit.
- Only a month that changed updates the Asset of the bronze table, and so
  triggers dbt_transformations.
- One month at a time, like eco2mix_history_to_bronze. The two DAGs can still
  commit to the table at the same time (the 1st of a month falling on a Sunday,
  a manual run): Nessie then rejects one of the commits, and its task is retried.
- 24 months cover the final data, published at most about 23 months after the
  month it describes.
"""

from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, CronTriggerTimetable, dag, get_current_context, task

from wattlake_ingestion.lakehouse import BRONZE_TABLE, BRONZE_TABLE_URI

PARIS = pendulum.timezone("Europe/Paris")
BRONZE = Asset(name=BRONZE_TABLE, uri=BRONZE_TABLE_URI)
WINDOW_MONTHS = 24


@dag(
    dag_id="eco2mix_history_refresh",
    # Sunday 04:00, Paris time: away from the monthly load (the 1st at 00:00) and
    # from iceberg_maintenance (11:30 UTC).
    schedule=CronTriggerTimetable("0 4 * * 0", timezone=PARIS),
    start_date=pendulum.datetime(2026, 11, 1, tz=PARIS),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion", "bronze"],
    doc_md=__doc__,
)
def eco2mix_history_refresh() -> None:
    @task
    def months_to_refresh() -> list[dict[str, str]]:
        from zoneinfo import ZoneInfo

        from wattlake_ingestion.bronze import recent_months

        # run_after is set for scheduled and manual runs alike, unlike logical_date.
        run_after = get_current_context()["dag_run"].run_after
        months = recent_months(run_after.astimezone(ZoneInfo("Europe/Paris")), WINDOW_MONTHS)
        # ISO strings rather than datetimes: they go through XCom.
        return [{"start": start.isoformat(), "end": end.isoformat()} for start, end in months]

    # Mapped tasks of a run one at a time: they commit to the same table.
    @task(outlets=[BRONZE], max_active_tis_per_dagrun=1)
    def refresh_month(month: dict[str, str]) -> int:
        # Imported here rather than at the top of the file: the dag-processor
        # parses this file often, and only the task needs these libraries.
        import logging
        import tempfile
        from datetime import UTC, datetime
        from pathlib import Path

        from pyiceberg.catalog import load_catalog

        from wattlake_ingestion.bronze import load_export_file
        from wattlake_ingestion.odre import HISTORY_DATASET, OdreClient

        start, end = datetime.fromisoformat(month["start"]), datetime.fromisoformat(month["end"])

        with tempfile.TemporaryDirectory() as tmp:
            export = Path(tmp) / "export.parquet"
            with OdreClient() as client:
                client.export_to_file(HISTORY_DATASET, start, end, export)
            written = load_export_file(
                load_catalog("wattlake"),
                export,
                HISTORY_DATASET,
                start,
                end,
                datetime.now(UTC),
                skip_unchanged=True,
            )

        if not written:
            # Skipped rather than successful: no Asset event, so dbt is not triggered.
            raise AirflowSkipException(f"{BRONZE_TABLE}: [{start}, {end}) not published yet or unchanged")
        logging.getLogger(__name__).info("%s: %d rows replaced for [%s, %s)", BRONZE_TABLE, written, start, end)
        return written

    refresh_month.expand(month=months_to_refresh())


eco2mix_history_refresh()
