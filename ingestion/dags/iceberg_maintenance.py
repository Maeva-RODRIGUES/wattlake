"""Daily maintenance of the Iceberg tables (docs/adr/013-maintenance-iceberg.md).

compact_bronze runs Trino's optimize on the two bronze tables, through the dbt
macro optimize_bronze_tables (transform/macros/), so that it reuses dbt's
connection to Trino and the project's source definitions. The real-time table
receives about 100 small files a day from the streaming writer: optimize
rewrites them into a few larger ones.

It runs on main directly (--target dev), not through Write-Audit-Publish: the
content of the tables does not change, only the way it is split into files.
If optimize commits while the writer prepares a batch, Nessie rejects the
writer's append: the writer reloads the table and appends the batch again
(streaming/src/wattlake_streaming/writer.py). The other way round, optimize
fails and is retried.

The files replaced by the compaction stay in the object store until Nessie GC
removes them, once they are older than the retention (step 8.2).

run_nessie_gc then starts Nessie GC as a Kubernetes Job, from the template of
the suspended CronJob nessie-gc (infra/nessie-gc/), and waits for its end. The
CronJob no longer schedules itself: its controller times the next run with a
timer that stops while the laptop sleeps, and started runs hours late. Airflow's
scheduler compares the schedule with the wall clock on every loop.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task

DBT_PROJECT_DIR = "/opt/wattlake/transform"
NAMESPACE = "wattlake"


@dag(
    dag_id="iceberg_maintenance",
    # 11:30 UTC: midday, when the machine is usually on, and away from the
    # hourly dbt run (on the hour).
    schedule=CronTriggerTimetable("30 11 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["maintenance", "iceberg"],
    doc_md=__doc__,
)
def iceberg_maintenance() -> None:
    compact_bronze = BashOperator(
        task_id="compact_bronze",
        bash_command=(
            "/opt/dbt/venv/bin/dbt run-operation optimize_bronze_tables --target dev"
            f" --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
            " --target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
        ),
        env={"TRINO_HOST": "trino.wattlake.svc.cluster.local", "TRINO_PORT": "8080"},
        append_env=True,
    )

    # No Airflow retries: the Job retries its pod itself (backoffLimit), and a
    # new attempt of the task would only wait for the same Job (same name).
    # Its own deadline (activeDeadlineSeconds, 1 hour) ends it before this one.
    @task(retries=0, execution_timeout=timedelta(minutes=70))
    def run_nessie_gc() -> None:
        from wattlake_ingestion.kubernetes_job import run_job_from_cronjob

        run_after = get_current_context()["dag_run"].run_after
        run_job_from_cronjob(NAMESPACE, "nessie-gc", f"nessie-gc-{run_after:%Y%m%d-%H%M}")

    # Nessie GC runs even if the compaction fails: the cleanup does not need it.
    compact_bronze >> run_nessie_gc.override(trigger_rule="all_done")()


iceberg_maintenance()
