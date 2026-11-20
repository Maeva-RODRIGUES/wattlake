"""Check that every DAG of the Airflow image imports without error.

Runs inside the image built by `make airflow-image` (`make dag-check`), so the
DAGs are parsed with the exact Airflow version, providers and packages they
run with in the cluster. The CI runs it before publishing the image: an image
whose DAGs do not import is never deployed.

DagBag only parses the files: no database, scheduler or cluster is needed. The
unit tests of ingestion/ cover the logic without Airflow; this check covers what
they cannot, the DAG files themselves (imports, decorators, schedules).
"""

import sys

from airflow.models.dagbag import DagBag

DAG_FOLDER = "/opt/airflow/dags"
# A DAG missing from this list fails the check as much as an import error: a
# renamed or deleted dag_id would silently stop its schedule in the cluster.
EXPECTED_DAGS = {
    "dbt_transformations",
    "eco2mix_history_refresh",
    "eco2mix_history_to_bronze",
    "iceberg_maintenance",
}


def main() -> int:
    bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
    failed = False

    for path, error in sorted(bag.import_errors.items()):
        print(f"IMPORT ERROR in {path}:\n{error}")
        failed = True

    found = set(bag.dags)
    if missing := EXPECTED_DAGS - found:
        print(f"MISSING DAGS: {sorted(missing)}")
        failed = True
    if unexpected := found - EXPECTED_DAGS:
        print(f"UNEXPECTED DAGS (add them to EXPECTED_DAGS): {sorted(unexpected)}")
        failed = True

    for dag_id in sorted(found):
        print(f"{dag_id}: {len(bag.dags[dag_id].tasks)} tasks")
    print("==> DAG check failed" if failed else f"==> DAG check passed: {len(found)} DAGs")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
