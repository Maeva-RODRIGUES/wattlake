"""Build the silver and gold layers with dbt whenever the bronze table changes.

The DAG is scheduled on the Asset of the bronze table: it runs after each
bronze load that wrote rows. Asset events that pile up while a run is in
progress (during a catchup for example) are consumed by the next run together,
so dbt does not rebuild once per loaded month.

dbt runs in its own virtual environment of the Airflow image (/opt/dbt/venv),
installed from transform/requirements.txt: its pinned dependencies differ
from the Airflow constraints for 26 shared packages, so the two cannot share
an environment. The dbt project and its packages (dbt deps) are part of the
image, built with it. One "dbt build" builds silver then gold and runs every
test: a failing test stops the models that depend on it.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag

from wattlake_ingestion.lakehouse import BRONZE_TABLE, BRONZE_TABLE_URI

BRONZE = Asset(name=BRONZE_TABLE, uri=BRONZE_TABLE_URI)
DBT_PROJECT_DIR = "/opt/wattlake/transform"


@dag(
    dag_id="dbt_transformations",
    schedule=[BRONZE],
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["transform", "dbt"],
    doc_md=__doc__,
)
def dbt_transformations() -> None:
    BashOperator(
        task_id="dbt_build",
        # target/ and logs/ go to a temporary directory, not into the image.
        bash_command=(
            "/opt/dbt/venv/bin/dbt build"
            f" --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
            " --target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
        ),
        # Same profiles.yml as on a laptop, pointed at the in-cluster Trino.
        env={"TRINO_HOST": "trino.wattlake.svc.cluster.local", "TRINO_PORT": "8080"},
        append_env=True,
    )


dbt_transformations()
