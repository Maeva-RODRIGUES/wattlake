"""Load the eCO2mix national history into the bronze layer, one month per run.

Each run downloads its month from the ODRE export API and replaces that month
in the Iceberg table bronze.eco2mix_national_history, in a single Nessie
commit: running a month again gives the same result. Catchup loads the whole
history from January 2012.

- Months are calendar months in Paris time, as RTE publishes them: January 2012
  starts at 2011-12-31 23:00 UTC.
- One run at a time: parallel runs would commit to the same table at the same
  time and conflict in Nessie.
- A month the history does not contain yet writes nothing: the task ends as
  skipped.
- Each load that writes rows updates the Asset of the bronze table, which
  triggers the dbt DAG (dbt_transformations). A skipped task emits no Asset
  event, so an empty month does not rebuild silver and gold for nothing.

The logic lives in the wattlake_ingestion package, tested without Airflow.
The Iceberg catalog "wattlake" is configured by PYICEBERG_CATALOG__WATTLAKE__*
environment variables (infra/airflow/values.yaml).
"""

from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from wattlake_ingestion.lakehouse import BRONZE_TABLE, BRONZE_TABLE_URI

PARIS = pendulum.timezone("Europe/Paris")
BRONZE = Asset(name=BRONZE_TABLE, uri=BRONZE_TABLE_URI)


@dag(
    dag_id="eco2mix_history_to_bronze",
    # Airflow 3 cron schedules have no data interval by default: this timetable
    # gives each run the month it covers, [data_interval_start, data_interval_end).
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone=PARIS),
    start_date=pendulum.datetime(2012, 1, 1, tz=PARIS),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion", "bronze"],
    doc_md=__doc__,
)
def eco2mix_history_to_bronze() -> None:
    @task(outlets=[BRONZE])
    def load_month() -> int:
        # Imported here rather than at the top of the file: the dag-processor
        # parses this file often, and only the task needs these libraries.
        import logging
        import tempfile
        from datetime import UTC, datetime
        from pathlib import Path

        from pyiceberg.catalog import load_catalog

        from wattlake_ingestion.bronze import load_export_file
        from wattlake_ingestion.odre import HISTORY_DATASET, OdreClient

        context = get_current_context()
        start, end = context["data_interval_start"], context["data_interval_end"]

        with tempfile.TemporaryDirectory() as tmp:
            export = Path(tmp) / "export.parquet"
            with OdreClient() as client:
                client.export_to_file(HISTORY_DATASET, start, end, export)
            written = load_export_file(load_catalog("wattlake"), export, HISTORY_DATASET, start, end, datetime.now(UTC))

        if not written:
            # Skipped rather than successful: no Asset event, so dbt is not triggered.
            raise AirflowSkipException(f"{BRONZE_TABLE}: nothing to load for [{start}, {end}), table left unchanged")
        logging.getLogger(__name__).info("%s: %d rows written for [%s, %s)", BRONZE_TABLE, written, start, end)
        return written

    load_month()


eco2mix_history_to_bronze()
