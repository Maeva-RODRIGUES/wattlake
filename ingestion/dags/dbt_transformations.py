"""Build the silver and gold layers with dbt every hour, and whenever the history changes.

Two triggers (AssetOrTimeSchedule), whichever comes first:

- every hour, on the hour: silver and gold take in the real-time data that the
  streaming writer appends to bronze every 5 minutes, outside Airflow
  (docs/adr/012-temps-reel-silver-gold.md). Gold is at most about an hour
  behind the stream;
- after each load of the history bronze table (its Asset). Asset events that
  pile up while a run is in progress (during a catchup for example) are
  consumed by the next run together, so dbt does not rebuild once per loaded
  month.

Write-Audit-Publish (docs/adr/008-write-audit-publish.md), in three tasks:

1. create_audit_branch: the Nessie branch "audit" is recreated from the head
   of main.
2. dbt_build (write and audit): "dbt build --target audit" builds silver then
   gold on that branch, through the Trino catalog iceberg_audit, and runs every
   test. A failing test stops the models that depend on it, and the task.
3. publish: only when dbt_build succeeds, the branch is merged into main, in
   one Nessie commit. Readers of main see the old tables or the new tested
   ones, never a partial or failed build.

When a test fails, main is left as it was and the audit branch keeps the
failed build, to be queried in Trino (catalog iceberg_audit) until the next
run recreates it.

dbt runs in its own virtual environment of the Airflow image (/opt/dbt/venv),
installed from transform/requirements.txt: its pinned dependencies differ
from the Airflow constraints for 26 shared packages, so the two cannot share
an environment. The dbt project and its packages (dbt deps) are part of the
image, built with it.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, AssetOrTimeSchedule, CronTriggerTimetable, dag, task

from wattlake_ingestion.lakehouse import AUDIT_BRANCH, BRONZE_TABLE, BRONZE_TABLE_URI, MAIN_BRANCH

BRONZE = Asset(name=BRONZE_TABLE, uri=BRONZE_TABLE_URI)
DBT_PROJECT_DIR = "/opt/wattlake/transform"


@dag(
    dag_id="dbt_transformations",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 * * * *", timezone="UTC"),
        assets=[BRONZE],
    ),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    # Also what makes a single audit branch enough: one run uses it at a time.
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["transform", "dbt"],
    doc_md=__doc__,
)
def dbt_transformations() -> None:
    @task
    def create_audit_branch() -> str:
        """Recreate the audit branch from main. Returns the main hash it starts from."""
        import logging

        from wattlake_ingestion.nessie import NessieClient

        with NessieClient() as nessie:
            base_hash = nessie.recreate_branch(AUDIT_BRANCH, MAIN_BRANCH)
        logging.getLogger(__name__).info("branch %s recreated from %s@%s", AUDIT_BRANCH, MAIN_BRANCH, base_hash)
        return base_hash

    dbt_build = BashOperator(
        task_id="dbt_build",
        # target/ and logs/ go to a temporary directory, not into the image.
        bash_command=(
            "/opt/dbt/venv/bin/dbt build --target audit"
            f" --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
            " --target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
        ),
        # Same profiles.yml as on a laptop, pointed at the in-cluster Trino.
        env={"TRINO_HOST": "trino.wattlake.svc.cluster.local", "TRINO_PORT": "8080"},
        append_env=True,
    )

    @task
    def publish(base_hash: str) -> str:
        """Merge the audited branch into main. Returns the new hash of main."""
        import logging

        from airflow.sdk import get_current_context

        from wattlake_ingestion.nessie import NessieClient

        log = logging.getLogger(__name__)
        with NessieClient() as nessie:
            audited_hash = nessie.head(AUDIT_BRANCH)
            if audited_hash is None:
                raise RuntimeError(f"branch {AUDIT_BRANCH} disappeared before the merge")
            if audited_hash == base_hash:
                log.info("branch %s has no commit since %s: nothing to merge", AUDIT_BRANCH, base_hash)
                return base_hash
            # The hash read now is the one merged: a commit added to the branch
            # after this point would not reach main.
            merged_hash = nessie.merge(
                AUDIT_BRANCH,
                audited_hash,
                MAIN_BRANCH,
                base_hash,
                f"Publish dbt build (Airflow run {get_current_context()['run_id']})",
            )
        log.info("%s@%s merged into %s: %s", AUDIT_BRANCH, audited_hash, MAIN_BRANCH, merged_hash)
        return merged_hash

    base_hash = create_audit_branch()
    base_hash >> dbt_build >> publish(base_hash)


dbt_transformations()
