"""Names of the lakehouse tables, shared by the loaders and the Airflow DAGs.

No heavy import here: DAG files import this module every time the
dag-processor parses them.
"""

BRONZE_NAMESPACE = "bronze"
BRONZE_TABLE = f"{BRONZE_NAMESPACE}.eco2mix_national_history"
# Identifies the bronze table as an Airflow Asset: the bronze DAG updates it,
# the dbt DAG is scheduled on it.
BRONZE_TABLE_URI = "iceberg://wattlake/bronze/eco2mix_national_history"

# Write-Audit-Publish (docs/adr/008-write-audit-publish.md): dbt writes on the
# audit branch, read and written by Trino through its "iceberg_audit" catalog,
# and the branch is merged into main once every test passes.
MAIN_BRANCH = "main"
AUDIT_BRANCH = "audit"
