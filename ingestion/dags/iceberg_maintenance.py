"""Nightly maintenance of the Iceberg tables (docs/adr/013-maintenance-iceberg.md).

compact_bronze runs Trino's optimize on the two bronze tables, through the dbt
macro optimize_bronze_tables (transform/macros/), so that it reuses dbt's
connection to Trino and the project's source definitions. The real-time table
receives about 100 small files a day from the streaming writer: optimize
rewrites them into a few larger ones.

It runs on main directly (--target dev), not through Write-Audit-Publish: the
content of the tables does not change, only the way it is split into files.
If optimize and the writer commit to the same table at the same moment, Nessie
accepts one commit and rejects the other: optimize fails and is retried, or the
writer restarts and resumes from the offsets in the table (ADR 010).

The files replaced by the compaction stay in the object store until Nessie GC
removes them, once they are older than the retention (step 8.2).
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import CronTriggerTimetable, dag

DBT_PROJECT_DIR = "/opt/wattlake/transform"


@dag(
    dag_id="iceberg_maintenance",
    # 02:30 UTC: away from the hourly dbt run (on the hour).
    schedule=CronTriggerTimetable("30 2 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["maintenance", "iceberg"],
    doc_md=__doc__,
)
def iceberg_maintenance() -> None:
    BashOperator(
        task_id="compact_bronze",
        bash_command=(
            "/opt/dbt/venv/bin/dbt run-operation optimize_bronze_tables --target dev"
            f" --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
            " --target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
        ),
        env={"TRINO_HOST": "trino.wattlake.svc.cluster.local", "TRINO_PORT": "8080"},
        append_env=True,
    )


iceberg_maintenance()
